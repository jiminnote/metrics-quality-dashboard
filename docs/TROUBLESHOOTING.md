# 🔧 정합성 검증 트러블슈팅 가이드

> **metrics-quality-dashboard** 운영 중 자주 발생하는 문제와 해결 방법을 정리한 문서입니다.
> 문제 발생 시 이 문서를 먼저 확인하고, 해결되지 않으면 `#data-quality-alerts` 채널로 에스컬레이션하세요.

---

## 목차

1. [정합성 검증 순서 의존성](#1-정합성-검증-순서-의존성)
2. [임계값 튜닝 과정](#2-임계값-튜닝-과정)
3. [Tableau 뷰 성능 최적화](#3-tableau-뷰-성능-최적화)
4. [DAG 실행 실패 및 재처리](#4-dag-실행-실패-및-재처리)
5. [YAML 설정 검증 오류](#5-yaml-설정-검증-오류)
6. [데이터 소스 관련 이슈](#6-데이터-소스-관련-이슈)
7. [알림(Slack) 장애](#7-알림slack-장애)
8. [Docker 환경 이슈](#8-docker-환경-이슈)

---

## 1. 정합성 검증 순서 의존성

### 문제 설명

10종 정합성 검증은 **실행 순서에 논리적 의존성**이 존재합니다.
순서를 무시하고 실행하면 잘못된 검증 결과(False Positive/Negative)가 발생할 수 있습니다.

### 검증 순서 및 의존 관계

```
┌─────────────────────────────────────────────────────────────┐
│  Phase 1: 기본 정합성 (선행 필수)                              │
│  ┌─ ① sum_integrity      (합계 = 부분합)                     │
│  ├─ ② ratio_market_share (점유율 합 = 100%)                  │
│  └─ ③ ratio_category     (업종비율 합 = 100%)                │
├─────────────────────────────────────────────────────────────┤
│  Phase 2: 산출식 검증 (Phase 1 통과 전제)                     │
│  ├─ ④ formula_mom        (MoM 역산)                          │
│  └─ ⑤ formula_yoy        (YoY 역산)                          │
├─────────────────────────────────────────────────────────────┤
│  Phase 3: 범위·연속성 (독립 실행 가능)                         │
│  ├─ ⑥ range_activation   (활성화율 0~100%)                   │
│  └─ ⑦ continuity         (월 데이터 누락)                     │
├─────────────────────────────────────────────────────────────┤
│  Phase 4: 통계·교차 (Phase 1~3 결과 참조)                     │
│  ├─ ⑧ statistical_anomaly (Z-Score 이상치)                   │
│  ├─ ⑨ trend_breaks        (이동평균 급변)                     │
│  └─ ⑩ cross_kpi           (점유율 ↔ 성장률 교차)              │
└─────────────────────────────────────────────────────────────┘
```

### 자주 발생하는 문제

| 증상 | 원인 | 해결 |
|------|------|------|
| `cross_kpi` 검증에서 대량 FAIL | `sum_integrity`가 먼저 실패 → 기초 금액 자체가 틀림 | Phase 1 검증 먼저 확인. `sum_integrity` FAIL이면 하위 검증 결과 무시 |
| `formula_mom` FAIL인데 실제 데이터는 정상 | KPI 테이블 갱신(`refresh_kpi_tables`) 전에 검증 실행 | `refresh_kpis >> run_checks` 의존성 확인. Airflow UI에서 태스크 순서 확인 |
| `statistical_anomaly` False Positive 급증 | 신규 카드사 데이터 추가로 분포 변동 | `z_score_warning` 임계값 상향 조정 (2.0 → 2.5) 또는 데이터 기간 확인 |
| `continuity` FAIL 반복 | 원천 데이터 공개 주기(익월 말) 대비 파이프라인 실행이 빠름 | `data_source.refresh_cycle` 확인. 데이터 미공개 월은 `max_missing_months: 1` 허용 |

### 디버깅 순서 권장

```bash
# 1. Phase 1 결과부터 확인
cat reports/daily_summary_*.json | python -c "
import json, sys
data = json.load(sys.stdin)
for c in data.get('failed_checks', []):
    if c['check_category'] in ('sum_integrity', 'ratio_integrity'):
        print(f\"[{c['severity']}] {c['check_name']}: {c['detail']}\")
"

# 2. 특정 월 데이터 직접 검증
psql -h localhost -U metrics -d metrics_db -c "
  SELECT year_month, SUM(usage_amount)
  FROM credit_card_usage
  WHERE year_month = '2025-12-01'
  GROUP BY year_month;
"
```

---

## 2. 임계값 튜닝 과정

### 문제 설명

`config/thresholds.yaml`의 임계값이 너무 엄격하면 **False Positive**(정상인데 FAIL), 너무 느슨하면 **False Negative**(이상인데 PASS)가 발생합니다. 데이터 특성에 맞는 적절한 튜닝이 필요합니다.

### 튜닝 프로세스

```
[1단계] 현황 분석          [2단계] 임계값 조정          [3단계] 검증·배포
    │                         │                           │
    ▼                         ▼                           ▼
 리포트에서               thresholds.yaml              테스트 실행
 FAIL 빈도 집계     →     tolerance 값 수정     →     pytest 통과 확인
    │                         │                           │
 severity별              스키마 검증 자동 수행           DAG 수동 트리거로
 분포 확인                                              실 데이터 검증
```

### 검증 항목별 튜닝 가이드

#### `sum_integrity` — 합계 정합성

```yaml
# 기본값
sum_integrity:
  tolerance: 1          # 백만원
  severity: CRITICAL
```

- **너무 엄격한 경우**: 반올림 오차로 매일 FAIL 발생
  - 조치: `tolerance: 5` (5백만원)로 상향
  - 근거: 8개 카드사 × 10개 업종 반올림 누적 오차 ≈ 2~3백만원

- **너무 느슨한 경우**: 실제 누락 데이터를 놓침
  - 조치: `tolerance: 0.5`로 하향 + 연속 FAIL 시 에스컬레이션

#### `statistical_anomaly` — Z-Score 이상치

```yaml
# 기본값
statistical_anomaly:
  z_score_warning: 2.0
  z_score_critical: 3.0
  max_critical_ratio: 5.0  # %
```

- **계절성이 강한 데이터** (설 연휴, 블랙프라이데이 등):
  - 조치: `z_score_warning: 2.5`, `z_score_critical: 3.5`로 상향
  - 또는 계절 보정 로직 추가 검토

- **급변 감지가 안 되는 경우**:
  - 조치: `max_critical_ratio: 3.0`으로 하향 (전체 대비 3% 초과 시 경고)

#### `cross_kpi` — 교차 검증

```yaml
# 기본값
cross_kpi:
  share_change_threshold: 0.5   # pp
  growth_rate_threshold: -1.0   # %
```

- **소규모 카드사에서 False Positive 빈번**:
  - 원인: 시장점유율 1~2% 카드사는 변동폭이 큼
  - 조치: `share_change_threshold: 1.0`으로 상향

### 임계값 변경 후 검증 체크리스트

```bash
# 1. YAML 스키마 검증
python -c "
import yaml
from scripts.run_integrity_checks import validate_config_schema
with open('config/thresholds.yaml') as f:
    config = yaml.safe_load(f)
errors = validate_config_schema(config)
if errors:
    print('스키마 오류:')
    for e in errors: print(f'  - {e}')
else:
    print('스키마 검증 통과 ✅')
"

# 2. 단위 테스트 실행
pytest tests/test_integrity_checks.py -v

# 3. 샘플 데이터로 전체 파이프라인 테스트
python scripts/run_integrity_checks.py --config config/thresholds.yaml

# 4. DAG 수동 트리거 (실 데이터 검증)
# Airflow UI → metrics_quality_monitoring → Trigger DAG w/ config:
#   {"rerun_checks_only": true, "skip_refresh": true}
```

### 튜닝 이력 관리 권장사항

| 항목 | 권장 |
|------|------|
| 변경 커밋 | `config: sum_integrity tolerance 1→5 (반올림 오차 허용)` |
| PR 리뷰 | 최소 1명 데이터 엔지니어 리뷰 필수 |
| 모니터링 | 변경 후 1주일 FAIL 빈도 추이 관찰 |
| 롤백 기준 | False Negative 1건이라도 감지 → 즉시 이전 값으로 복원 |

---

## 3. Tableau 뷰 성능 최적화

### 문제 설명

`integrity_check_log` 테이블 기반 Tableau 대시보드가 데이터 누적에 따라 느려지거나, 필터 동작이 비정상적인 경우의 대응 방법입니다.

### 자주 발생하는 문제

#### 3-1. 대시보드 로딩 속도 저하

**증상**: Tableau 워크북 열기에 30초 이상 소요

**원인 분석**:
```sql
-- 로그 테이블 데이터 규모 확인
SELECT
    DATE_TRUNC('month', check_date) AS month,
    COUNT(*) AS row_count,
    pg_size_pretty(pg_relation_size('integrity_check_log')) AS table_size
FROM integrity_check_log
GROUP BY 1 ORDER BY 1 DESC LIMIT 6;
```

**해결 방법**:

1. **파티셔닝 적용** (데이터 90일 이상 누적 시 권장):
```sql
-- 월별 파티셔닝 전환
CREATE TABLE integrity_check_log_partitioned (
    LIKE integrity_check_log INCLUDING ALL
) PARTITION BY RANGE (check_date);

CREATE TABLE icl_2026_01 PARTITION OF integrity_check_log_partitioned
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE icl_2026_02 PARTITION OF integrity_check_log_partitioned
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
```

2. **Tableau 추출(Extract) 스케줄 최적화**:
   - Live 연결 → Extract로 전환
   - 추출 스케줄: 매일 05:00 KST (DAG 완료 이후)
   - 증분 새로고침: `check_date >= DATEADD('day', -7, TODAY())` 필터

3. **인덱스 추가**:
```sql
-- Tableau 필터 조건에 맞는 복합 인덱스
CREATE INDEX idx_icl_date_status_severity
    ON integrity_check_log (check_date, status, severity);

CREATE INDEX idx_icl_category_date
    ON integrity_check_log (check_category, check_date DESC);
```

#### 3-2. 필터 동작 이상 (날짜 범위 필터가 안 먹힘)

**증상**: 날짜 필터 변경해도 데이터가 갱신되지 않음

**원인**: Tableau 캐시 또는 추출 데이터 미갱신

**해결**:
1. Tableau Desktop: 데이터 소스 → 우클릭 → "추출 새로고침"
2. Tableau Server: 스케줄 → "지금 실행"
3. 캐시 삭제: 서버 관리 → 사이트 설정 → 캐시 무효화

#### 3-3. 검증 카테고리별 시각화 설계 권장사항

```
┌────────────────────────────────────────────────────────────┐
│  대시보드 레이아웃 권장                                       │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  [상단] KPI 요약 카드 4장                                    │
│    총 검증 수 │ 통과율 │ CRITICAL FAIL │ 연속 통과 일수        │
│                                                            │
│  [중단 좌] 일별 통과율 추이 (라인 차트)                        │
│    - X: check_date, Y: pass_rate                           │
│    - 색상: severity (CRITICAL=빨강, WARNING=노랑)             │
│                                                            │
│  [중단 우] 카테고리별 FAIL 히트맵                              │
│    - X: check_date, Y: check_category                      │
│    - 색상: status (PASS=초록, FAIL=빨강)                     │
│                                                            │
│  [하단] 최근 FAIL 상세 테이블                                 │
│    - 필터: severity, check_category, date_range             │
│    - 정렬: check_date DESC                                  │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

**성능 팁**:
- 히트맵 뷰는 최근 30일로 기본 필터 설정
- LOD 계산식 대신 DB 단에서 집계 후 커스텀 SQL 사용
- 대시보드 액션 필터는 최대 2단계까지만 체이닝

---

## 4. DAG 실행 실패 및 재처리

### 자주 발생하는 실패 패턴

#### 4-1. `refresh_kpi_tables` SQL 실행 타임아웃

**증상**: `Task exceeded execution_timeout (1800.0s)`

**원인**: KPI 테이블 재생성 시 대량 데이터 조인으로 인한 지연

**해결**:
```sql
-- 실행 계획 확인
EXPLAIN ANALYZE
SELECT year_month, card_company, SUM(usage_amount)
FROM credit_card_usage GROUP BY 1, 2;

-- 인덱스 확인
SELECT indexname, indexdef FROM pg_indexes
WHERE tablename = 'credit_card_usage';
```

- `execution_timeout` 상향: `thresholds.yaml` → `airflow.sla_minutes: 45`
- PostgreSQL `work_mem` 조정: `SET work_mem = '256MB';`

#### 4-2. `run_checks` DB 연결 실패

**증상**: `OperationalError: could not connect to server`

**원인**: PostgreSQL 커넥션 풀 소진 또는 네트워크 이슈

**해결**:
- 자동 재시도: `run_checks`는 `retries=3`, `retry_delay=2분`으로 설정됨
- 재시도 시 Slack 알림(`on_retry_callback`)으로 가시성 확보
- 수동 재처리:

```bash
# Airflow CLI로 특정 태스크만 재실행
airflow tasks run metrics_quality_monitoring integrity_checks.run_checks 2026-02-15
```

#### 4-3. Branch 하류 태스크 전체 SKIPPED

**증상**: `generate_report` 태스크가 실행되지 않음

**원인**: BranchPythonOperator 하류 태스크의 `trigger_rule` 미설정

**해결**: 이미 `NONE_FAILED_MIN_ONE_SUCCESS`로 설정됨. 여전히 발생 시:
1. Airflow UI에서 의존성 그래프 확인
2. `evaluate` 태스크의 반환값 확인 (XCom)
3. `escalate` 태스크가 `generate_report` 상류에 포함되었는지 확인 (포함되면 안 됨)

#### 4-4. 수동 재처리 방법

```bash
# 방법 1: 검증만 재실행 (KPI 테이블 갱신 생략)
# Airflow UI → Trigger DAG w/ config:
{"rerun_checks_only": true, "skip_refresh": true}

# 방법 2: 강제 알림 레벨 지정 (테스트용)
{"force_alert_level": "WARNING"}

# 방법 3: 특정 날짜 백필
airflow dags backfill metrics_quality_monitoring \
  --start-date 2026-02-10 \
  --end-date 2026-02-15 \
  --reset-dagruns
```

---

## 5. YAML 설정 검증 오류

### 스키마 검증 오류 유형

`validate_config_schema()` 함수가 DAG 파싱 및 스크립트 실행 시 자동으로 호출됩니다.

#### 5-1. 필수 섹션 누락

```
설정 스키마 검증 경고: 필수 섹션 누락: 'thresholds'
```

**해결**: `thresholds` 섹션이 `config/thresholds.yaml` 최상위에 존재하는지 확인

#### 5-2. 필수 키 누락

```
설정 스키마 검증 경고: thresholds 내 필수 키 누락: 'range_activation'
```

**해결**: 10종 검증 키가 모두 존재하는지 확인. 필수 키 목록:

```
sum_integrity, ratio_market_share, ratio_category,
formula_mom, formula_yoy, range_activation, range_hhi,
continuity, statistical_anomaly, cross_kpi
```

#### 5-3. 타입 오류

```
설정 스키마 검증 경고: thresholds.sum_integrity.tolerance 타입 오류: 기대=(int, float), 실제=str
```

**해결**: YAML에서 숫자값에 따옴표 제거
```yaml
# ❌ 잘못된 예
sum_integrity:
  tolerance: "1"    # 문자열로 인식

# ✅ 올바른 예
sum_integrity:
  tolerance: 1      # 정수로 인식
```

#### 5-4. severity 값 오류

```
설정 스키마 검증 경고: thresholds.sum_integrity.severity 값 오류: 'FATAL'
```

**해결**: 허용 값은 `CRITICAL`, `WARNING`, `INFO` 3가지만 가능

#### 5-5. 검증 명령어

```bash
# 빠른 스키마 검증
python -c "
import yaml
from scripts.run_integrity_checks import validate_config_schema
with open('config/thresholds.yaml') as f:
    errors = validate_config_schema(yaml.safe_load(f))
print('통과 ✅' if not errors else '\n'.join(errors))
"
```

---

## 6. 데이터 소스 관련 이슈

### 6-1. 원천 데이터 지연

**증상**: `continuity` 검증에서 최근 월 FAIL

**원인**: 여신금융협회 데이터 공개 주기 = 익월 말. 2월 15일 시점에 1월 데이터 미공개 가능

**해결**:
```yaml
# 임시로 1개월 누락 허용
continuity:
  max_missing_months: 1
  severity: WARNING    # CRITICAL → WARNING 하향
```

### 6-2. 카드사 코드 변경

**증상**: `sum_integrity` FAIL + 특정 카드사 금액 0

**원인**: 원천 데이터에서 카드사명 변경 (예: "우리카드" → "우리카드(구 우리BC)")

**해결**:
1. `credit_card_usage` 테이블에서 `DISTINCT card_company` 확인
2. `config/thresholds.yaml`의 `data_source.card_companies` 목록 갱신
3. SQL 변환 로직에 카드사명 정규화 추가

### 6-3. 업종 분류 체계 변경

**증상**: `ratio_category` 검증 대량 FAIL

**원인**: 업종 분류 코드 변경 (예: "기타" 카테고리 분할)

**해결**: `data_source.business_categories` 확인 후 SQL 매핑 테이블 갱신

---

## 7. 알림(Slack) 장애

### 7-1. Slack 알림 미발송

**증상**: CRITICAL FAIL인데 Slack 메시지 없음

**체크리스트**:
```bash
# 1. 환경변수 확인
echo $SLACK_WEBHOOK_URL

# 2. Webhook 수동 테스트
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"테스트 알림"}' \
  "$SLACK_WEBHOOK_URL"

# 3. Airflow 로그 확인
# Airflow UI → 해당 태스크 → Log 탭
# "Slack 알림 전송 실패" 메시지 검색
```

### 7-2. 알림 폭주 (Alert Storm)

**증상**: 수십 건의 알림이 한 번에 발송

**원인**: 월초 데이터 대량 갱신 후 일시적 정합성 불일치

**해결**:
- `on_retry_callback`이 재시도마다 알림 → 재시도 횟수 내에서 자연 해소되는지 확인
- 지속 시: `force_alert_level: "PASS"`로 수동 트리거하여 알림 중지 후 원인 파악

---

## 8. Docker 환경 이슈

### 8-1. 컨테이너 시작 실패

```bash
# 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs metrics-db
docker-compose logs airflow-webserver

# 전체 재시작
docker-compose down -v && docker-compose up -d
```

### 8-2. PostgreSQL 연결 거부

**증상**: `FATAL: password authentication failed for user "metrics"`

**해결**:
```bash
# 볼륨 초기화 (기존 데이터 삭제 주의)
docker-compose down -v
docker volume rm metrics-quality-dashboard_metrics_db_data
docker-compose up -d
```

### 8-3. Airflow 웹 UI 접속 불가

- URL: `http://localhost:8080`
- 계정: `admin` / `admin`
- `airflow-init` 컨테이너가 정상 종료(exit 0)되었는지 확인:
  ```bash
  docker-compose logs airflow-init | tail -5
  ```

---

## 빠른 참조: 긴급 대응 플로우

```
알림 수신 (Slack #data-quality-alerts)
    │
    ▼
severity 확인
    │
    ├─ CRITICAL ──→ 즉시 확인 (15분 내)
    │   ├─ Phase 1 검증 실패? → 원천 데이터 확인
    │   ├─ continuity 실패?   → 데이터 공개 일정 확인
    │   └─ range 실패?        → 이상 데이터 SQL 조회
    │
    ├─ WARNING ───→ 업무 시간 내 확인
    │   ├─ 임계값 조정 필요?   → thresholds.yaml 튜닝
    │   └─ 일시적 현상?       → 다음 실행 결과 모니터링
    │
    └─ INFO ──────→ 주간 리뷰에서 확인
        └─ cross_kpi 트렌드 변화? → 비즈니스 이벤트 확인
```

---

## 관련 문서

| 문서 | 경로 | 설명 |
|------|------|------|
| 임계값 설정 | `config/thresholds.yaml` | 모든 검증 기준 중앙 관리 |
| DAG 정의 | `dags/metrics_quality_dag.py` | Airflow 파이프라인 흐름 |
| 검증 스크립트 | `scripts/run_integrity_checks.py` | 10종 검증 엔진 |
| SQL 검증 쿼리 | `sql/integrity_checks.sql` | DB 레벨 정합성 쿼리 |
| KPI 산출 SQL | `sql/kpi_definitions.sql` | KPI 테이블 정의 |
| 단위 테스트 | `tests/test_integrity_checks.py` | 검증 로직 테스트 44건 |
