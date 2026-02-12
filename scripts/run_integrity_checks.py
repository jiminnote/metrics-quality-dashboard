"""
지표 정합성 자동 검증 스크립트
============================================================
KPI 지표 간 교차 검증을 실행하고 CSV / JSON / HTML 리포트를 생성합니다.

검증 카테고리 (10종):
  1. sum_integrity       : 합계 정합성 (전체 = 부분 합계)
  2. ratio_integrity     : 비율 정합성 (점유율·업종비율 합계 = 100%)
  3. formula_integrity   : 산출식 정합성 (MoM·YoY 역산)
  4. range_integrity     : 범위 정합성 (활성화율 0~100, HHI 0~10000)
  5. continuity_integrity: 연속성 정합성 (월 누락)
  6. statistical_integrity: 통계 정합성 (Z-Score 이상치 비율)
  7. cross_kpi_integrity : 교차 정합성 (점유율↔성장률 방향성)

사용법:
  python run_integrity_checks.py
  python run_integrity_checks.py --config config/thresholds.yaml
  python run_integrity_checks.py --export html --output reports/
  python run_integrity_checks.py --db-url postgresql://user:pass@localhost:5432/db
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import statistics
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml

# ──────────────────────────────────────────────────────────
# 로깅 설정
# ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════
# Enums & Data Classes
# ══════════════════════════════════════════════════════════

class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


class CheckStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass
class IntegrityCheckResult:
    """정합성 검증 결과 단위"""
    check_name: str
    check_category: str
    severity: str
    expected_value: float
    actual_value: float
    difference: float
    tolerance: float
    status: str
    detail: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def is_passed(self) -> bool:
        return self.status == CheckStatus.PASS

    @property
    def is_critical_failure(self) -> bool:
        return not self.is_passed and self.severity == Severity.CRITICAL


@dataclass
class ThresholdConfig:
    """검증 임계값 설정"""
    tolerance: float = 0.01
    severity: str = "WARNING"
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    description: str = ""


# ══════════════════════════════════════════════════════════
# 설정 로더
# ══════════════════════════════════════════════════════════

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """YAML 설정 파일 로드 — 없으면 기본값 사용"""
    defaults = {
        "thresholds": {
            "sum_integrity": {"tolerance": 1, "severity": "CRITICAL"},
            "ratio_market_share": {"tolerance": 0.1, "severity": "CRITICAL"},
            "ratio_category": {"tolerance": 0.5, "severity": "WARNING"},
            "formula_mom": {"tolerance": 10, "severity": "WARNING"},
            "formula_yoy": {"tolerance": 10, "severity": "WARNING"},
            "range_activation": {"min": 0, "max": 100, "severity": "CRITICAL"},
            "range_hhi": {"min": 0, "max": 10000, "severity": "WARNING"},
            "continuity": {"max_missing_months": 0, "severity": "CRITICAL"},
            "statistical_anomaly": {
                "z_score_warning": 2.0,
                "z_score_critical": 3.0,
                "max_critical_ratio": 5.0,
                "severity": "WARNING",
            },
            "cross_kpi": {
                "share_change_threshold": 0.5,
                "growth_rate_threshold": -1.0,
                "severity": "INFO",
            },
        },
        "reporting": {
            "output_dir": "reports",
            "formats": ["csv", "json", "html"],
            "retention_days": 90,
        },
    }

    if config_path and Path(config_path).exists():
        with open(config_path, "r", encoding="utf-8") as f:
            user_cfg = yaml.safe_load(f)
        # 사용자 설정으로 기본값 오버라이드
        if user_cfg:
            for section in ("thresholds", "reporting", "alerting"):
                if section in user_cfg:
                    defaults.setdefault(section, {}).update(user_cfg[section])
        logger.info("설정 로드 완료: %s", config_path)
    else:
        logger.info("기본 설정 사용 (YAML 파일 없음)")

    return defaults


# ══════════════════════════════════════════════════════════
# 통계 유틸리티
# ══════════════════════════════════════════════════════════

def calc_z_score(value: float, values: Sequence[float]) -> Optional[float]:
    """Z-Score 계산 (모집단 기준)"""
    if len(values) < 2:
        return None
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)
    if stdev == 0:
        return 0.0
    return round((value - mean) / stdev, 3)


def calc_iqr_bounds(values: Sequence[float]) -> tuple[float, float]:
    """IQR 기반 이상치 경계 산출"""
    sorted_v = sorted(values)
    n = len(sorted_v)
    q1 = sorted_v[n // 4]
    q3 = sorted_v[(3 * n) // 4]
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr


def detect_trend_break(
    values: Sequence[float], window: int = 3, threshold_sigma: float = 2.0
) -> List[int]:
    """이동평균 대비 급변 구간 탐지 — 인덱스 반환"""
    if len(values) < window + 1:
        return []

    breaks = []
    for i in range(window, len(values)):
        moving_avg = statistics.mean(values[i - window : i])
        moving_std = statistics.pstdev(values[i - window : i]) or 1.0
        if abs(values[i] - moving_avg) > threshold_sigma * moving_std:
            breaks.append(i)
    return breaks


# ══════════════════════════════════════════════════════════
# 핵심 검증 엔진
# ══════════════════════════════════════════════════════════

class MetricsIntegrityChecker:
    """KPI 지표 정합성 검증기

    - 10종 교차 검증
    - YAML 기반 임계값 설정
    - CSV / JSON / HTML 리포트 생성
    - 심각도(Severity) 3단계 분류
    """

    def __init__(
        self,
        connection=None,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.connection = connection
        self.config = config or load_config()
        self.results: List[IntegrityCheckResult] = []
        self.check_date = date.today()
        self._thresholds = self.config.get("thresholds", {})

    def _get_threshold(self, key: str) -> Dict[str, Any]:
        return self._thresholds.get(key, {"tolerance": 0, "severity": "WARNING"})

    # ──────────────────────────────────────────────────────
    # CHECK 1: 합계 정합성
    # ──────────────────────────────────────────────────────
    def check_sum_integrity(self, data: List[Dict]) -> List[IntegrityCheckResult]:
        """전체 이용금액 = 카드사별 합계"""
        cfg = self._get_threshold("sum_integrity")
        results = []

        monthly: Dict[str, Dict] = defaultdict(lambda: {"total": 0, "by_company": defaultdict(float)})
        for row in data:
            ym = str(row.get("year_month", ""))
            amount = float(row.get("usage_amount", 0))
            company = row.get("card_company", "")
            monthly[ym]["total"] += amount
            monthly[ym]["by_company"][company] += amount

        for ym, totals in sorted(monthly.items()):
            company_sum = sum(totals["by_company"].values())
            diff = abs(totals["total"] - company_sum)
            tol = cfg.get("tolerance", 1)

            results.append(IntegrityCheckResult(
                check_name="전체 이용금액 = 카드사별 합계",
                check_category="sum_integrity",
                severity=cfg.get("severity", "CRITICAL"),
                expected_value=round(totals["total"], 2),
                actual_value=round(company_sum, 2),
                difference=round(diff, 2),
                tolerance=tol,
                status=CheckStatus.PASS if diff < tol else CheckStatus.FAIL,
                detail=f"year_month={ym}",
            ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 2: 시장 점유율 합계 = 100%
    # ──────────────────────────────────────────────────────
    def check_market_share_integrity(
        self, share_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """시장 점유율 합계 = 100% 검증"""
        cfg = self._get_threshold("ratio_market_share")
        results = []

        monthly_shares: Dict[str, float] = defaultdict(float)
        for row in share_data:
            ym = str(row.get("year_month", ""))
            monthly_shares[ym] += float(row.get("market_share_pct", 0))

        for ym, total_share in sorted(monthly_shares.items()):
            diff = abs(100.0 - total_share)
            tol = cfg.get("tolerance", 0.1)

            results.append(IntegrityCheckResult(
                check_name="시장 점유율 합계 = 100%",
                check_category="ratio_integrity",
                severity=cfg.get("severity", "CRITICAL"),
                expected_value=100.0,
                actual_value=round(total_share, 2),
                difference=round(diff, 2),
                tolerance=tol,
                status=CheckStatus.PASS if diff < tol else CheckStatus.FAIL,
                detail=f"year_month={ym}",
            ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 3: 업종별 비율 합계 = 100% (카드사별)
    # ──────────────────────────────────────────────────────
    def check_category_ratio_integrity(
        self, category_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """카드사별 업종 비율 합계 = 100% 검증"""
        cfg = self._get_threshold("ratio_category")
        results = []

        groups: Dict[str, float] = defaultdict(float)
        for row in category_data:
            key = f"{row.get('year_month', '')}|{row.get('card_company', '')}"
            groups[key] += float(row.get("category_share_pct", 0))

        for key, total in sorted(groups.items()):
            ym, company = key.split("|", 1)
            diff = abs(100.0 - total)
            tol = cfg.get("tolerance", 0.5)

            results.append(IntegrityCheckResult(
                check_name="업종별 비율 합계 = 100% (카드사별)",
                check_category="ratio_integrity",
                severity=cfg.get("severity", "WARNING"),
                expected_value=100.0,
                actual_value=round(total, 2),
                difference=round(diff, 2),
                tolerance=tol,
                status=CheckStatus.PASS if diff < tol else CheckStatus.FAIL,
                detail=f"year_month={ym}, company={company}",
            ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 4: MoM 성장률 역산 검증
    # ──────────────────────────────────────────────────────
    def check_formula_mom(
        self, growth_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """MoM 성장률로 전월 금액 역산 → 실제 전월 금액과 비교"""
        cfg = self._get_threshold("formula_mom")
        results = []

        for row in growth_data:
            mom = row.get("mom_growth_rate")
            prev = row.get("prev_month_amount") or row.get("previous_amount")
            curr = row.get("current_amount")

            if mom is None or prev is None or curr is None:
                continue
            mom, prev, curr = float(mom), float(prev), float(curr)
            if mom == 0:
                continue

            reverse_calc = round(curr / (1 + mom / 100.0), 0)
            diff = abs(prev - reverse_calc)
            tol = cfg.get("tolerance", 10)

            results.append(IntegrityCheckResult(
                check_name="MoM 성장률 역산 검증",
                check_category="formula_integrity",
                severity=cfg.get("severity", "WARNING"),
                expected_value=prev,
                actual_value=reverse_calc,
                difference=round(diff, 2),
                tolerance=tol,
                status=CheckStatus.PASS if diff < tol else CheckStatus.FAIL,
                detail=f"year_month={row.get('year_month', '')}, company={row.get('card_company', '')}",
            ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 5: YoY 성장률 역산 검증
    # ──────────────────────────────────────────────────────
    def check_formula_yoy(
        self, growth_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """YoY 성장률로 전년 동월 금액 역산"""
        cfg = self._get_threshold("formula_yoy")
        results = []

        for row in growth_data:
            yoy = row.get("yoy_growth_rate")
            prev_y = row.get("prev_year_amount")
            curr = row.get("current_amount")

            if yoy is None or prev_y is None or curr is None:
                continue
            yoy, prev_y, curr = float(yoy), float(prev_y), float(curr)
            if yoy == 0:
                continue

            reverse_calc = round(curr / (1 + yoy / 100.0), 0)
            diff = abs(prev_y - reverse_calc)
            tol = cfg.get("tolerance", 10)

            results.append(IntegrityCheckResult(
                check_name="YoY 성장률 역산 검증",
                check_category="formula_integrity",
                severity=cfg.get("severity", "WARNING"),
                expected_value=prev_y,
                actual_value=reverse_calc,
                difference=round(diff, 2),
                tolerance=tol,
                status=CheckStatus.PASS if diff < tol else CheckStatus.FAIL,
                detail=f"year_month={row.get('year_month', '')}, company={row.get('card_company', '')}",
            ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 6: 활성화율 범위 검증
    # ──────────────────────────────────────────────────────
    def check_range_activation(
        self, activation_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """활성화율 0~100% 범위 검증"""
        cfg = self._get_threshold("range_activation")
        results = []
        lo, hi = cfg.get("min", 0), cfg.get("max", 100)

        for row in activation_data:
            rate = float(row.get("activation_rate", 0))
            in_range = lo <= rate <= hi

            results.append(IntegrityCheckResult(
                check_name="활성화율 범위 검증 (0~100%)",
                check_category="range_integrity",
                severity=cfg.get("severity", "CRITICAL"),
                expected_value=hi,
                actual_value=rate,
                difference=0 if in_range else abs(rate - (hi if rate > hi else lo)),
                tolerance=0,
                status=CheckStatus.PASS if in_range else CheckStatus.FAIL,
                detail=f"year_month={row.get('year_month', '')}, company={row.get('card_company', '')}",
            ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 7: 데이터 연속성 (월 누락)
    # ──────────────────────────────────────────────────────
    def check_continuity(
        self, monthly_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """카드사별 연속 월 데이터 누락 검사"""
        cfg = self._get_threshold("continuity")
        results = []

        company_months: Dict[str, set] = defaultdict(set)
        for row in monthly_data:
            company_months[row.get("card_company", "")].add(
                str(row.get("year_month", ""))
            )

        for company, months in sorted(company_months.items()):
            sorted_m = sorted(months)
            if len(sorted_m) < 2:
                continue

            # YYYY-MM 형식 파싱해서 기대 월 수 산출
            try:
                first = datetime.strptime(sorted_m[0][:7], "%Y-%m")
                last = datetime.strptime(sorted_m[-1][:7], "%Y-%m")
                expected = (last.year - first.year) * 12 + (last.month - first.month) + 1
            except (ValueError, IndexError):
                expected = len(sorted_m)

            actual = len(sorted_m)
            missing = expected - actual

            results.append(IntegrityCheckResult(
                check_name="데이터 연속성 검증 (월 누락)",
                check_category="continuity_integrity",
                severity=cfg.get("severity", "CRITICAL"),
                expected_value=expected,
                actual_value=actual,
                difference=missing,
                tolerance=cfg.get("max_missing_months", 0),
                status=CheckStatus.PASS if missing <= cfg.get("max_missing_months", 0) else CheckStatus.FAIL,
                detail=f"company={company}, months={actual}/{expected}",
            ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 8: Z-Score 이상치 탐지
    # ──────────────────────────────────────────────────────
    def check_statistical_anomaly(
        self, usage_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """카드사별 이용금액 Z-Score 산출 및 이상치 비율 검증"""
        cfg = self._get_threshold("statistical_anomaly")
        results = []

        # 카드사별 금액 리스트 구성
        company_amounts: Dict[str, List[tuple]] = defaultdict(list)
        for row in usage_data:
            company_amounts[row.get("card_company", "")].append(
                (str(row.get("year_month", "")), float(row.get("total_usage_amount", row.get("usage_amount", 0))))
            )

        z_critical = cfg.get("z_score_critical", 3.0)
        z_warning = cfg.get("z_score_warning", 2.0)
        all_z_scores = []

        for company, records in sorted(company_amounts.items()):
            values = [r[1] for r in records]
            if len(values) < 3:
                continue

            for ym, val in records:
                z = calc_z_score(val, values)
                if z is None:
                    continue

                abs_z = abs(z)
                if abs_z > z_critical:
                    level = Severity.CRITICAL
                elif abs_z > z_warning:
                    level = Severity.WARNING
                else:
                    level = Severity.INFO

                all_z_scores.append((company, ym, z, level))

        # 개별 이상치 리포팅
        critical_count = sum(1 for _, _, _, lvl in all_z_scores if lvl == Severity.CRITICAL)
        total_count = len(all_z_scores) or 1
        critical_ratio = round(critical_count / total_count * 100, 2)
        max_ratio = cfg.get("max_critical_ratio", 5.0)

        results.append(IntegrityCheckResult(
            check_name="Z-Score 이상치 비율 검증 (<5%)",
            check_category="statistical_integrity",
            severity=cfg.get("severity", "WARNING"),
            expected_value=max_ratio,
            actual_value=critical_ratio,
            difference=max(0, critical_ratio - max_ratio),
            tolerance=max_ratio,
            status=CheckStatus.PASS if critical_ratio <= max_ratio else CheckStatus.FAIL,
            detail=f"critical_count={critical_count}, total={total_count}",
        ))

        # IQR 기반 보조 검증
        for company, records in sorted(company_amounts.items()):
            values = [r[1] for r in records]
            if len(values) < 4:
                continue
            lower, upper = calc_iqr_bounds(values)
            outliers = [(ym, v) for ym, v in records if v < lower or v > upper]
            for ym, v in outliers:
                results.append(IntegrityCheckResult(
                    check_name="IQR 이상치 탐지",
                    check_category="statistical_integrity",
                    severity="INFO",
                    expected_value=round(upper, 2),
                    actual_value=round(v, 2),
                    difference=round(abs(v - upper) if v > upper else abs(lower - v), 2),
                    tolerance=0,
                    status=CheckStatus.FAIL,
                    detail=f"company={company}, year_month={ym}, bounds=[{lower:.0f}, {upper:.0f}]",
                ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 9: 추세 급변 탐지 (Trend Break Detection)
    # ──────────────────────────────────────────────────────
    def check_trend_breaks(
        self, usage_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """카드사별 이용금액 추세 급변 구간 탐지"""
        results = []

        company_series: Dict[str, List[tuple]] = defaultdict(list)
        for row in usage_data:
            company_series[row.get("card_company", "")].append(
                (str(row.get("year_month", "")), float(row.get("total_usage_amount", row.get("usage_amount", 0))))
            )

        for company, records in sorted(company_series.items()):
            records.sort(key=lambda x: x[0])
            values = [r[1] for r in records]
            breaks = detect_trend_break(values, window=3, threshold_sigma=2.0)

            for idx in breaks:
                ym = records[idx][0]
                results.append(IntegrityCheckResult(
                    check_name="추세 급변 탐지 (Trend Break)",
                    check_category="trend_integrity",
                    severity="WARNING",
                    expected_value=round(statistics.mean(values[max(0, idx-3):idx]), 2),
                    actual_value=round(values[idx], 2),
                    difference=round(abs(values[idx] - statistics.mean(values[max(0, idx-3):idx])), 2),
                    tolerance=0,
                    status=CheckStatus.FAIL,
                    detail=f"company={company}, year_month={ym}",
                ))

        self.results.extend(results)
        return results

    # ──────────────────────────────────────────────────────
    # CHECK 10: 점유율 ↔ 성장률 교차 검증
    # ──────────────────────────────────────────────────────
    def check_cross_kpi_consistency(
        self, share_data: List[Dict], growth_data: List[Dict]
    ) -> List[IntegrityCheckResult]:
        """점유율 변동 방향과 성장률 부호 교차 검증"""
        cfg = self._get_threshold("cross_kpi")
        results = []

        share_map = {}
        for row in share_data:
            key = f"{row.get('year_month', '')}|{row.get('card_company', '')}"
            share_map[key] = float(row.get("share_change_pp", 0))

        for row in growth_data:
            key = f"{row.get('year_month', '')}|{row.get('card_company', '')}"
            share_chg = share_map.get(key)
            mom = row.get("mom_growth_rate")
            if share_chg is None or mom is None:
                continue
            mom = float(mom)

            # 점유율 증가(+0.5pp 초과)인데 성장률이 마이너스(-1% 미만)이면 불일치
            share_thr = cfg.get("share_change_threshold", 0.5)
            growth_thr = cfg.get("growth_rate_threshold", -1.0)
            inconsistent = share_chg > share_thr and mom < growth_thr

            results.append(IntegrityCheckResult(
                check_name="점유율 변동 ↔ 성장률 교차 검증",
                check_category="cross_kpi_integrity",
                severity=cfg.get("severity", "INFO"),
                expected_value=0,
                actual_value=1 if inconsistent else 0,
                difference=1 if inconsistent else 0,
                tolerance=0,
                status=CheckStatus.FAIL if inconsistent else CheckStatus.PASS,
                detail=f"year_month={row.get('year_month', '')}, company={row.get('card_company', '')}, "
                       f"share_chg={share_chg:+.2f}pp, mom={mom:+.2f}%",
            ))

        self.results.extend(results)
        return results

    # ══════════════════════════════════════════════════════
    # 결과 집계 & 리포트
    # ══════════════════════════════════════════════════════

    def get_summary(self) -> Dict[str, Any]:
        """검증 결과 요약"""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.is_passed)
        failed = total - passed
        critical_fails = sum(1 for r in self.results if r.is_critical_failure)

        by_category: Dict[str, Dict] = {}
        for r in self.results:
            cat = r.check_category
            if cat not in by_category:
                by_category[cat] = {"total": 0, "passed": 0, "failed": 0}
            by_category[cat]["total"] += 1
            by_category[cat]["passed" if r.is_passed else "failed"] += 1

        for cat in by_category:
            by_category[cat]["pass_rate"] = round(
                by_category[cat]["passed"] / max(by_category[cat]["total"], 1) * 100, 1
            )

        return {
            "check_date": self.check_date.isoformat(),
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "critical_failures": critical_fails,
            "overall_pass_rate": round(passed / max(total, 1) * 100, 1),
            "overall_status": "PASS" if failed == 0 else ("CRITICAL" if critical_fails > 0 else "WARNING"),
            "by_category": by_category,
            "failed_checks": [asdict(r) for r in self.results if not r.is_passed],
        }

    def export_to_csv(self, output_dir: str) -> str:
        """CSV 리포트 내보내기"""
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"integrity_report_{self.check_date}.csv")

        fieldnames = [
            "check_name", "check_category", "severity",
            "expected_value", "actual_value", "difference",
            "tolerance", "status", "detail", "timestamp",
        ]
        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in self.results:
                writer.writerow(asdict(r))

        logger.info("CSV 저장: %s (%d건)", filepath, len(self.results))
        return filepath

    def export_to_json(self, output_dir: str) -> str:
        """JSON 리포트 내보내기"""
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"integrity_report_{self.check_date}.json")

        report = self.get_summary()
        report["all_checks"] = [asdict(r) for r in self.results]

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)

        logger.info("JSON 저장: %s", filepath)
        return filepath

    def export_to_html(self, output_dir: str) -> str:
        """HTML 시각화 리포트 생성"""
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"integrity_report_{self.check_date}.html")
        summary = self.get_summary()

        status_color = {
            "PASS": "#10b981", "WARNING": "#f59e0b", "CRITICAL": "#ef4444",
            "FAIL": "#ef4444",
        }
        overall_color = status_color.get(summary["overall_status"], "#6b7280")

        # 카테고리별 테이블 행
        cat_rows = ""
        for cat, stats in summary["by_category"].items():
            icon = "" if stats["failed"] == 0 else ""
            cat_rows += f"""
            <tr>
                <td>{icon} {cat}</td>
                <td class="num">{stats['total']}</td>
                <td class="num pass">{stats['passed']}</td>
                <td class="num fail">{stats['failed']}</td>
                <td class="num">{stats['pass_rate']}%</td>
            </tr>"""

        # 실패 항목 행
        fail_rows = ""
        for r in self.results:
            if r.is_passed:
                continue
            sev_cls = r.severity.lower()
            fail_rows += f"""
            <tr class="{sev_cls}">
                <td><span class="badge {sev_cls}">{r.severity}</span></td>
                <td>{r.check_name}</td>
                <td class="num">{r.expected_value}</td>
                <td class="num">{r.actual_value}</td>
                <td class="num">{r.difference}</td>
                <td>{r.detail}</td>
            </tr>"""

        # 전체 결과 행
        all_rows = ""
        for r in self.results:
            cls = "pass-row" if r.is_passed else "fail-row"
            all_rows += f"""
            <tr class="{cls}">
                <td>{r.status}</td>
                <td>{r.check_name}</td>
                <td>{r.check_category}</td>
                <td><span class="badge {r.severity.lower()}">{r.severity}</span></td>
                <td class="num">{r.expected_value}</td>
                <td class="num">{r.actual_value}</td>
                <td class="num">{r.difference}</td>
                <td>{r.detail}</td>
            </tr>"""

        html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>지표 정합성 검증 리포트 — {self.check_date}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, 'Pretendard', sans-serif; background: #f8fafc; color: #1e293b; padding: 2rem; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ font-size: 1.5rem; margin-bottom: 0.25rem; }}
        .subtitle {{ color: #64748b; margin-bottom: 2rem; }}

        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
        .kpi-card {{ background: white; border-radius: 12px; padding: 1.25rem; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
        .kpi-label {{ font-size: 0.8rem; color: #64748b; margin-bottom: 4px; }}
        .kpi-value {{ font-size: 1.75rem; font-weight: 700; }}
        .kpi-value.pass {{ color: #10b981; }}
        .kpi-value.fail {{ color: #ef4444; }}

        table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 2rem; }}
        th {{ background: #f1f5f9; text-align: left; padding: 0.75rem 1rem; font-size: 0.8rem; color: #475569; text-transform: uppercase; letter-spacing: 0.05em; }}
        td {{ padding: 0.65rem 1rem; border-top: 1px solid #f1f5f9; font-size: 0.875rem; }}
        .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .pass {{ color: #10b981; }}
        .fail {{ color: #ef4444; }}
        .pass-row {{ background: #f0fdf4; }}
        .fail-row {{ background: #fef2f2; }}

        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 6px; font-size: 0.7rem; font-weight: 600; }}
        .badge.critical {{ background: #fee2e2; color: #dc2626; }}
        .badge.warning {{ background: #fef3c7; color: #d97706; }}
        .badge.info {{ background: #dbeafe; color: #2563eb; }}

        .section-title {{ font-size: 1.1rem; font-weight: 600; margin: 1.5rem 0 0.75rem; }}
        details {{ margin-bottom: 2rem; }}
        summary {{ cursor: pointer; font-weight: 600; padding: 0.5rem 0; }}
    </style>
</head>
<body>
<div class="container">
    <h1>지표 정합성 검증 리포트</h1>
    <p class="subtitle">{self.check_date} · {summary['total_checks']}건 검증 수행</p>

    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="kpi-label">전체 상태</div>
            <div class="kpi-value" style="color:{overall_color}">{summary['overall_status']}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">전체 통과율</div>
            <div class="kpi-value {'pass' if summary['overall_pass_rate'] == 100 else 'fail'}">{summary['overall_pass_rate']}%</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">통과</div>
            <div class="kpi-value pass">{summary['passed']}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">실패</div>
            <div class="kpi-value fail">{summary['failed']}</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-label">CRITICAL 실패</div>
            <div class="kpi-value fail">{summary['critical_failures']}</div>
        </div>
    </div>

    <div class="section-title">카테고리별 요약</div>
    <table>
        <thead><tr><th>카테고리</th><th>전체</th><th>통과</th><th>실패</th><th>통과율</th></tr></thead>
        <tbody>{cat_rows}</tbody>
    </table>

    {"<div class='section-title'>실패 항목 상세</div><table><thead><tr><th>심각도</th><th>검증 항목</th><th>기대값</th><th>실제값</th><th>차이</th><th>상세</th></tr></thead><tbody>" + fail_rows + "</tbody></table>" if fail_rows else ""}

    <details>
        <summary>전체 검증 결과 ({summary['total_checks']}건)</summary>
        <table>
            <thead><tr><th>상태</th><th>검증 항목</th><th>카테고리</th><th>심각도</th><th>기대값</th><th>실제값</th><th>차이</th><th>상세</th></tr></thead>
            <tbody>{all_rows}</tbody>
        </table>
    </details>

    <p style="color:#94a3b8; font-size:0.75rem; margin-top:2rem;">
        Generated by metrics-quality-dashboard · {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </p>
</div>
</body>
</html>"""

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info("HTML 저장: %s", filepath)
        return filepath

    def print_report(self) -> None:
        """콘솔 리포트 출력"""
        summary = self.get_summary()
        overall = summary["overall_status"]
        icon = "" if overall == "PASS" else ("" if overall == "CRITICAL" else "")

        print(f"\n{'═' * 72}")
        print(f"  지표 정합성 검증 리포트 ({self.check_date})")
        print(f"{'═' * 72}")
        print(f"  상태: {icon} {overall}")
        print(f"  전체: {summary['total_checks']}건 │ "
              f"통과: {summary['passed']}건 │ "
              f"실패: {summary['failed']}건 │ "
              f"CRITICAL: {summary['critical_failures']}건 │ "
              f"통과율: {summary['overall_pass_rate']}%")
        print(f"{'─' * 72}")
        print(f"  {'카테고리':<28} {'전체':>5} {'통과':>5} {'실패':>5} {'통과율':>8}")
        print(f"  {'─' * 58}")

        for cat, stats in summary["by_category"].items():
            ci = "" if stats["failed"] == 0 else ""
            print(f"  {ci} {cat:<25} {stats['total']:>5} {stats['passed']:>5} "
                  f"{stats['failed']:>5} {stats['pass_rate']:>7}%")

        if summary["failed_checks"]:
            print(f"\n  {'실패 항목 상세':}")
            print(f"  {'─' * 58}")
            for chk in summary["failed_checks"]:
                sev = chk["severity"]
                badge = "" if sev == "CRITICAL" else ("" if sev == "WARNING" else "")
                print(f"  {badge} [{sev}] {chk['check_name']}")
                print(f"       기대={chk['expected_value']} → 실제={chk['actual_value']} "
                      f"(차이={chk['difference']}) │ {chk['detail']}")

        print(f"{'═' * 72}\n")


# ══════════════════════════════════════════════════════════
# 데모 데이터 생성
# ══════════════════════════════════════════════════════════

def generate_demo_data() -> Dict[str, List[Dict]]:
    """데모용 24개월 × 8개 카드사 데이터 생성"""
    import random
    random.seed(42)

    companies = ["신한카드", "삼성카드", "KB국민카드", "현대카드", "우리카드", "하나카드", "롯데카드", "BC카드"]
    categories = ["음식점", "주유소", "대형마트", "온라인쇼핑", "교통", "의료", "교육", "여행/숙박"]

    # 카드사별 기본 규모 (억원)
    base_amounts = {
        "신한카드": 15000, "삼성카드": 13500, "KB국민카드": 14000, "현대카드": 10000,
        "우리카드": 8000, "하나카드": 7500, "롯데카드": 6500, "BC카드": 5500,
    }

    usage_data = []
    monthly_usage = []
    category_data = []

    for year in (2024, 2025):
        for month in range(1, 13):
            ym = f"{year}-{month:02d}-01"
            trend = 1 + (year - 2024) * 0.03 + month * 0.002  # 완만 성장 트렌드

            for company in companies:
                base = base_amounts[company]
                noise = random.uniform(-0.05, 0.05)
                seasonal = 0.03 * math.sin(2 * math.pi * month / 12)  # 계절성
                amount = round(base * trend * (1 + noise + seasonal))

                usage_data.append({
                    "year_month": ym,
                    "card_company": company,
                    "usage_amount": amount,
                })

            # 월별 집계
            month_total = sum(r["usage_amount"] for r in usage_data if r["year_month"] == ym)
            for company in companies:
                comp_amount = sum(
                    r["usage_amount"]
                    for r in usage_data
                    if r["year_month"] == ym and r["card_company"] == company
                )
                monthly_usage.append({
                    "year_month": ym,
                    "card_company": company,
                    "total_usage_amount": comp_amount,
                })

    # 점유율 데이터
    share_data = []
    months_map: Dict[str, float] = defaultdict(float)
    for row in monthly_usage:
        months_map[row["year_month"]] += row["total_usage_amount"]

    prev_shares: Dict[str, float] = {}
    for row in monthly_usage:
        ym = row["year_month"]
        company = row["card_company"]
        share = round(row["total_usage_amount"] / months_map[ym] * 100, 2)
        prev = prev_shares.get(company, share)
        share_data.append({
            "year_month": ym,
            "card_company": company,
            "market_share_pct": share,
            "share_change_pp": round(share - prev, 2),
        })
        prev_shares[company] = share

    # 성장률 데이터
    growth_data = []
    prev_amounts: Dict[str, float] = {}
    prev_year_amounts: Dict[str, Dict[str, float]] = defaultdict(dict)

    for row in sorted(monthly_usage, key=lambda x: x["year_month"]):
        company = row["card_company"]
        ym = row["year_month"]
        curr = row["total_usage_amount"]
        prev = prev_amounts.get(company)
        # YoY: 12개월 전
        ym_parts = ym.split("-")
        prev_y_key = f"{int(ym_parts[0]) - 1}-{ym_parts[1]}-{ym_parts[2]}"
        prev_y = prev_year_amounts.get(company, {}).get(prev_y_key)

        mom = round((curr - prev) / prev * 100, 2) if prev and prev != 0 else None
        yoy = round((curr - prev_y) / prev_y * 100, 2) if prev_y and prev_y != 0 else None

        growth_data.append({
            "year_month": ym,
            "card_company": company,
            "current_amount": curr,
            "prev_month_amount": prev,
            "prev_year_amount": prev_y,
            "mom_growth_rate": mom,
            "yoy_growth_rate": yoy,
        })

        prev_amounts[company] = curr
        prev_year_amounts[company][ym] = curr

    # 활성화율 데이터
    activation_data = []
    for company in companies:
        base_rate = random.uniform(62, 78)
        for year in (2024, 2025):
            for month in range(1, 13):
                ym = f"{year}-{month:02d}-01"
                rate = round(base_rate + random.uniform(-2, 2) + (year - 2024) * 1.5, 2)
                rate = max(0, min(100, rate))  # 범위 보장
                activation_data.append({
                    "year_month": ym,
                    "card_company": company,
                    "activation_rate": rate,
                })

    # 업종별 비율 데이터
    cat_weights = [0.22, 0.12, 0.15, 0.18, 0.10, 0.08, 0.07, 0.08]
    for row in monthly_usage:
        remainder = 100.0
        for i, cat in enumerate(categories):
            if i == len(categories) - 1:
                pct = round(remainder, 2)
            else:
                pct = round(cat_weights[i] * 100 + random.uniform(-2, 2), 2)
                remainder -= pct
            category_data.append({
                "year_month": row["year_month"],
                "card_company": row["card_company"],
                "business_category": cat,
                "category_share_pct": pct,
            })

    return {
        "usage": usage_data,
        "monthly_usage": monthly_usage,
        "share": share_data,
        "growth": growth_data,
        "activation": activation_data,
        "category": category_data,
    }


# ══════════════════════════════════════════════════════════
# CLI 엔트리포인트
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="지표 정합성 자동 검증 시스템",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python run_integrity_checks.py
  python run_integrity_checks.py --config config/thresholds.yaml --export html
  python run_integrity_checks.py --db-url postgresql://user:pass@localhost:5432/db
        """,
    )
    parser.add_argument("--config", type=str, default="config/thresholds.yaml",
                        help="YAML 설정 파일 경로")
    parser.add_argument("--db-url", type=str, help="PostgreSQL 연결 URL")
    parser.add_argument("--export", type=str, choices=["csv", "json", "html", "all"],
                        default="all", help="내보내기 형식")
    parser.add_argument("--output", type=str, default="reports",
                        help="리포트 출력 디렉토리")
    args = parser.parse_args()

    # 설정 로드
    config = load_config(args.config)
    checker = MetricsIntegrityChecker(config=config)

    logger.info("지표 정합성 검증 시작")

    # 데모 데이터 생성 (DB 연결 없는 경우)
    demo = generate_demo_data()
    logger.info("데모 데이터 생성 완료: %d건 이용내역, %d개 카드사",
                len(demo["usage"]), 8)

    # ── 10종 검증 실행 ──
    checker.check_sum_integrity(demo["usage"])
    checker.check_market_share_integrity(demo["share"])
    checker.check_category_ratio_integrity(demo["category"])
    checker.check_formula_mom(demo["growth"])
    checker.check_formula_yoy(demo["growth"])
    checker.check_range_activation(demo["activation"])
    checker.check_continuity(demo["monthly_usage"])
    checker.check_statistical_anomaly(demo["monthly_usage"])
    checker.check_trend_breaks(demo["monthly_usage"])
    checker.check_cross_kpi_consistency(demo["share"], demo["growth"])

    # ── 리포트 출력 ──
    checker.print_report()

    if args.export in ("csv", "all"):
        checker.export_to_csv(args.output)
    if args.export in ("json", "all"):
        checker.export_to_json(args.output)
    if args.export in ("html", "all"):
        checker.export_to_html(args.output)

    # 종료 코드: CRITICAL 실패 시 1 반환 (CI/CD 연동)
    summary = checker.get_summary()
    if summary["critical_failures"] > 0:
        logger.error("CRITICAL 실패 %d건 — 즉시 확인 필요", summary["critical_failures"])
        sys.exit(1)
    elif summary["failed"] > 0:
        logger.warning("WARNING 실패 %d건", summary["failed"])
        sys.exit(0)
    else:
        logger.info("전체 검증 통과")
        sys.exit(0)


if __name__ == "__main__":
    main()
