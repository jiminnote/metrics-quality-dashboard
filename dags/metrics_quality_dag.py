"""
지표 정합성 모니터링 DAG
============================================================
파이프라인 흐름:
  ┌─ refresh_kpi_tables (SQL)
  │     ↓
  ├─ run_integrity_checks (Python 10종 검증)
  │     ↓
  ├─ evaluate_results (Branch)
  │     ├─ [CRITICAL] → alert_critical → escalate_to_oncall
  │     ├─ [WARNING]  → alert_warning
  │     └─ [PASS]     → log_success
  │     ↓
  └─ generate_report (CSV/JSON/HTML)
       ↓
     cleanup_old_reports

스케줄  : 매일 04:00 KST (UTC 19:00)
SLA     : 30분 이내 완료
알림    : Slack (#data-quality-alerts)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.utils.task_group import TaskGroup
except ImportError:
    TaskGroup = None  # Airflow < 2.3 fallback

# ── 프로젝트 경로 설정 ──
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

# ── YAML 설정 로드 (Airflow Variable 우선) ──
try:
    import yaml

    _config_path = os.path.join(PROJECT_DIR, "config", "thresholds.yaml")
    with open(_config_path, "r", encoding="utf-8") as _f:
        DAG_CONFIG = yaml.safe_load(_f)
except Exception:
    DAG_CONFIG = {}

_airflow_cfg = DAG_CONFIG.get("airflow", {})


# ══════════════════════════════════════════════════════════
# DAG 기본 설정
# ══════════════════════════════════════════════════════════

default_args = {
    "owner": "data-quality",
    "depends_on_past": False,
    "retries": _airflow_cfg.get("retries", 2),
    "retry_delay": timedelta(minutes=_airflow_cfg.get("retry_delay_minutes", 5)),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=_airflow_cfg.get("sla_minutes", 30)),
    "on_failure_callback": lambda ctx: _on_task_failure(ctx),
    "sla": timedelta(minutes=_airflow_cfg.get("sla_minutes", 30)),
}

dag = DAG(
    dag_id=_airflow_cfg.get("dag_id", "metrics_quality_monitoring"),
    default_args=default_args,
    description="KPI 지표 정합성 자동 검증 및 모니터링 파이프라인",
    schedule_interval=_airflow_cfg.get("schedule", "0 19 * * *"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=_airflow_cfg.get("tags", ["metrics", "quality", "monitoring", "dataops"]),
    max_active_runs=_airflow_cfg.get("max_active_runs", 1),
    doc_md=__doc__,
)

POSTGRES_CONN_ID = _airflow_cfg.get("postgres_conn_id", "metrics_db")


# ══════════════════════════════════════════════════════════
# 콜백 함수
# ══════════════════════════════════════════════════════════

def _on_task_failure(context: Dict[str, Any]) -> None:
    """태스크 실패 시 Slack 알림 전송"""
    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")

    message = (
        f"*Airflow Task 실패*\n"
        f"  DAG: `{dag_id}`\n"
        f"  Task: `{task.task_id if task else 'unknown'}`\n"
        f"  실행일: {execution_date}\n"
        f"  로그: {task.log_url if task else ''}"
    )

    _send_slack_notification(message, severity="CRITICAL")


def _send_slack_notification(message: str, severity: str = "INFO") -> None:
    """Slack Webhook 알림"""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print(f"[Slack 알림 - {severity}] (webhook 미설정)\n{message}")
        return

    try:
        import requests

        color_map = {"CRITICAL": "#dc2626", "WARNING": "#d97706", "INFO": "#2563eb", "PASS": "#10b981"}
        payload = {
            "channel": DAG_CONFIG.get("alerting", {}).get("slack", {}).get("channel", "#data-quality-alerts"),
            "attachments": [{
                "color": color_map.get(severity, "#6b7280"),
                "text": message,
                "footer": f"metrics-quality-dashboard | {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            }],
        }
        requests.post(webhook_url, json=payload, timeout=10)
    except Exception as e:
        print(f"Slack 알림 전송 실패: {e}")


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA 미준수 시 알림"""
    task_names = ", ".join(t.task_id for t in task_list)
    _send_slack_notification(
        f"*SLA 미준수 경고*\n"
        f"  DAG: `{dag.dag_id}`\n"
        f"  미준수 태스크: {task_names}\n"
        f"  SLA: {_airflow_cfg.get('sla_minutes', 30)}분",
        severity="WARNING",
    )


dag.sla_miss_callback = _sla_miss_callback


# ══════════════════════════════════════════════════════════
# Task 함수
# ══════════════════════════════════════════════════════════

def run_integrity_checks_task(**context) -> str:
    """10종 정합성 검증 실행"""
    from scripts.run_integrity_checks import MetricsIntegrityChecker, load_config

    config_path = os.path.join(PROJECT_DIR, "config", "thresholds.yaml")
    config = load_config(config_path)
    checker = MetricsIntegrityChecker(config=config)

    # ── DB에서 데이터 로드 ──
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    def _query(sql: str):
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    usage_data = _query("SELECT year_month, card_company, usage_amount FROM credit_card_usage")
    share_data = _query("SELECT year_month, card_company, market_share_pct, share_change_pp FROM kpi_market_share")
    growth_data = _query(
        "SELECT year_month, card_company, current_amount, prev_month_amount, "
        "prev_year_amount, mom_growth_rate, yoy_growth_rate FROM kpi_growth_rate"
    )
    activation_data = _query("SELECT year_month, card_company, activation_rate FROM kpi_activation_rate")
    monthly_data = _query("SELECT year_month, card_company, total_usage_amount FROM kpi_monthly_usage")
    category_data = _query(
        "SELECT year_month, card_company, business_category, category_share_pct FROM kpi_category_usage"
    )

    # ── 10종 검증 실행 ──
    checker.check_sum_integrity(usage_data)
    checker.check_market_share_integrity(share_data)
    checker.check_category_ratio_integrity(category_data)
    checker.check_formula_mom(growth_data)
    checker.check_formula_yoy(growth_data)
    checker.check_range_activation(activation_data)
    checker.check_continuity(monthly_data)
    checker.check_statistical_anomaly(monthly_data)
    checker.check_trend_breaks(monthly_data)
    checker.check_cross_kpi_consistency(share_data, growth_data)

    # ── 결과 저장 ──
    summary = checker.get_summary()
    checker.print_report()

    context["ti"].xcom_push(key="check_summary", value=json.dumps(summary, ensure_ascii=False, default=str))
    context["ti"].xcom_push(key="overall_status", value=summary["overall_status"])
    context["ti"].xcom_push(key="critical_failures", value=summary["critical_failures"])
    context["ti"].xcom_push(key="total_checks", value=summary["total_checks"])
    context["ti"].xcom_push(key="pass_rate", value=summary["overall_pass_rate"])

    return summary["overall_status"]


def evaluate_results_task(**context) -> str:
    """검증 결과에 따른 3-way 분기"""
    status = context["ti"].xcom_pull(task_ids="integrity_checks.run_checks", key="overall_status")

    if status == "CRITICAL":
        return "alerting.alert_critical"
    elif status == "WARNING":
        return "alerting.alert_warning"
    else:
        return "alerting.log_success"


def alert_critical_task(**context) -> None:
    """CRITICAL 실패 알림 — 즉시 대응 필요"""
    summary = json.loads(
        context["ti"].xcom_pull(task_ids="integrity_checks.run_checks", key="check_summary")
    )

    failed_items = "\n".join(
        f"  [{c['severity']}] {c['check_name']}: {c['detail']}"
        for c in summary.get("failed_checks", [])
        if c["severity"] == "CRITICAL"
    )

    message = (
        f"*[CRITICAL] 지표 정합성 검증 실패*\n"
        f"  실패: {summary['failed']}건 (CRITICAL: {summary['critical_failures']}건)\n"
        f"  통과율: {summary['overall_pass_rate']}%\n\n"
        f"{failed_items}"
    )

    _send_slack_notification(message, severity="CRITICAL")

    # 메트릭 기록 (추후 모니터링 시스템 연동)
    print(f"metric.integrity.critical_failures={summary['critical_failures']}")
    print(f"metric.integrity.pass_rate={summary['overall_pass_rate']}")


def escalate_to_oncall_task(**context) -> None:
    """CRITICAL 2회 연속 시 온콜 에스컬레이션"""
    # 이전 실행 결과 확인 (실제 환경에서는 DB/Variable 조회)
    prev_status = Variable.get("prev_integrity_status", default_var="PASS")
    current_status = context["ti"].xcom_pull(
        task_ids="integrity_checks.run_checks", key="overall_status"
    )

    if prev_status == "CRITICAL" and current_status == "CRITICAL":
        _send_slack_notification(
            "*[ESCALATION] 정합성 CRITICAL 2회 연속 — 온콜 확인 필요*\n"
            f"  @data-team 즉시 확인 바랍니다.",
            severity="CRITICAL",
        )

    Variable.set("prev_integrity_status", current_status)


def alert_warning_task(**context) -> None:
    """WARNING 수준 알림"""
    summary = json.loads(
        context["ti"].xcom_pull(task_ids="integrity_checks.run_checks", key="check_summary")
    )

    _send_slack_notification(
        f"*[WARNING] 지표 정합성 검증 경고*\n"
        f"  실패: {summary['failed']}건 | 통과율: {summary['overall_pass_rate']}%",
        severity="WARNING",
    )


def log_success_task(**context) -> None:
    """전체 통과 로그"""
    pass_rate = context["ti"].xcom_pull(task_ids="integrity_checks.run_checks", key="pass_rate")
    total = context["ti"].xcom_pull(task_ids="integrity_checks.run_checks", key="total_checks")
    print(f"전체 정합성 검증 통과: {total}건, 통과율 {pass_rate}%")
    print(f"metric.integrity.pass_rate={pass_rate}")


def generate_report_task(**context) -> None:
    """일별 종합 리포트 생성 (CSV + JSON + HTML)"""
    from scripts.run_integrity_checks import MetricsIntegrityChecker, load_config

    summary_json = context["ti"].xcom_pull(
        task_ids="integrity_checks.run_checks", key="check_summary"
    )
    summary = json.loads(summary_json)
    execution_date = context["ds"]

    report_dir = os.path.join(PROJECT_DIR, "reports")
    os.makedirs(report_dir, exist_ok=True)

    # JSON 요약 저장
    report_path = os.path.join(report_dir, f"daily_summary_{execution_date}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({
            "execution_date": execution_date,
            "pipeline": "metrics_quality_monitoring",
            **summary,
        }, f, indent=2, ensure_ascii=False, default=str)

    print(f"리포트 저장: {report_path}")
    print(f"\n{'═' * 60}")
    print(f"  일별 지표 정합성 리포트: {execution_date}")
    print(f"{'═' * 60}")
    print(f"  상태: {'PASS' if summary['overall_status'] == 'PASS' else '' + summary['overall_status']}")
    print(f"  검증: {summary['passed']}/{summary['total_checks']} 통과 ({summary['overall_pass_rate']}%)")
    print(f"  CRITICAL: {summary['critical_failures']}건")
    print(f"{'═' * 60}")


def cleanup_old_reports_task(**context) -> None:
    """보관 기간 초과 리포트 삭제"""
    report_dir = os.path.join(PROJECT_DIR, "reports")
    retention_days = DAG_CONFIG.get("reporting", {}).get("retention_days", 90)

    if not os.path.exists(report_dir):
        return

    import glob
    from datetime import date, timedelta

    cutoff = date.today() - timedelta(days=retention_days)
    removed = 0

    for filepath in glob.glob(os.path.join(report_dir, "*")):
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath)).date()
            if mtime < cutoff:
                os.remove(filepath)
                removed += 1
        except Exception:
            pass

    if removed:
        print(f"{removed}개 오래된 리포트 삭제 (보관: {retention_days}일)")


# ══════════════════════════════════════════════════════════
# Task 정의 (TaskGroup 활용)
# ══════════════════════════════════════════════════════════

# ── KPI 갱신 ──
refresh_kpis = PostgresOperator(
    task_id="refresh_kpi_tables",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql="sql/kpi_definitions.sql",
    dag=dag,
)

# ── 검증 TaskGroup ──
if TaskGroup:
    with TaskGroup("integrity_checks", dag=dag) as checks_group:
        run_checks = PythonOperator(
            task_id="run_checks",
            python_callable=run_integrity_checks_task,
        )

        evaluate = BranchPythonOperator(
            task_id="evaluate",
            python_callable=evaluate_results_task,
        )

        run_checks >> evaluate
else:
    run_checks = PythonOperator(
        task_id="run_checks",
        python_callable=run_integrity_checks_task,
        dag=dag,
    )
    evaluate = BranchPythonOperator(
        task_id="evaluate",
        python_callable=evaluate_results_task,
        dag=dag,
    )
    run_checks >> evaluate

# ── 알림 TaskGroup ──
if TaskGroup:
    with TaskGroup("alerting", dag=dag) as alert_group:
        alert_critical = PythonOperator(
            task_id="alert_critical",
            python_callable=alert_critical_task,
        )
        escalate = PythonOperator(
            task_id="escalate_to_oncall",
            python_callable=escalate_to_oncall_task,
        )
        alert_warning = PythonOperator(
            task_id="alert_warning",
            python_callable=alert_warning_task,
        )
        log_success = PythonOperator(
            task_id="log_success",
            python_callable=log_success_task,
        )

        alert_critical >> escalate
else:
    alert_critical = PythonOperator(
        task_id="alert_critical",
        python_callable=alert_critical_task,
        dag=dag,
    )
    escalate = PythonOperator(
        task_id="escalate_to_oncall",
        python_callable=escalate_to_oncall_task,
        dag=dag,
    )
    alert_warning = PythonOperator(
        task_id="alert_warning",
        python_callable=alert_warning_task,
        dag=dag,
    )
    log_success = PythonOperator(
        task_id="log_success",
        python_callable=log_success_task,
        dag=dag,
    )
    alert_critical >> escalate

# ── 리포트 ──
generate_report = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report_task,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

cleanup_reports = PythonOperator(
    task_id="cleanup_old_reports",
    python_callable=cleanup_old_reports_task,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)


# ══════════════════════════════════════════════════════════
# 의존성 그래프
# ══════════════════════════════════════════════════════════

refresh_kpis >> run_checks
evaluate >> [alert_critical, alert_warning, log_success]
[alert_critical, escalate, alert_warning, log_success] >> generate_report
generate_report >> cleanup_reports
