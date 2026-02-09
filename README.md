# 지표 정합성 모니터링 대시보드

> 카드사별 신용카드 이용실적 데이터를 기반으로 **핵심 KPI 7종**을 정의하고,
> **10가지 교차 정합성 자동 검증** 파이프라인을 구축하며,
> **Tableau 대시보드**로 지표 추이와 품질 상태를 시각화하는 프로젝트입니다.

---

## 프로젝트 목적

| 영역 | 내용 |
|------|------|
| **KPI 설계** | 여신금융협회 원천 데이터 → 7종 핵심 KPI SQL 산출 로직 정의 |
| **정합성 검증** | 10가지 교차 검증 (합계·비율·역산·범위·연속성·통계·교차) |
| **자동화** | Airflow DAG — 일별 KPI 갱신 → 검증 → 알림 → 리포트 |
| **시각화** | Tableau 대시보드 3종 (KPI 개요 / 성장 트렌드 / 정합성 모니터링) |
| **DevOps** | Docker Compose 원클릭 기동, pytest 단위 테스트, YAML 설정 관리 |

---

## 아키텍처

```
┌───────────────────────────────────────────────────────────────────┐
│                     Airflow DAG (Daily 04:00 KST)                │
│                                                                   │
│  ┌──────────────┐    ┌──────────────────┐    ┌────────────────┐  │
│  │ KPI 갱신 (SQL)│───▶│ 정합성 검증 (Py) │───▶│ 결과 평가      │  │
│  │  7종 KPI     │    │  10종 교차검증   │    │  (3-way 분기)  │  │
│  └──────────────┘    └──────────────────┘    └───┬────┬───┬───┘  │
│                                                  │    │   │      │
│                              ┌────────────────┐  │    │   │      │
│                   CRITICAL ──│ Slack 알림  │◀─┘    │   │      │
│                              │ + 온콜 에스컬  │       │   │      │
│                              └────────────────┘       │   │      │
│                              ┌────────────────┐       │   │      │
│                   WARNING ───│ Slack 경고  │◀──────┘   │      │
│                              └────────────────┘           │      │
│                              ┌────────────────┐           │      │
│                   PASS ──────│ 성공 로그   │◀──────────┘      │
│                              └────────────────┘                  │
│                                      │                           │
│                              ┌───────▼────────┐                  │
│                              │ 리포트 생성     │                  │
│                              │ CSV/JSON/HTML  │                  │
│                              └───────┬────────┘                  │
│                              ┌───────▼────────┐                  │
│                              │ 오래된 리포트   │                  │
│                              │ 자동 정리      │                  │
│                              └────────────────┘                  │
└───────────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────┐          ┌─────────────────────┐
│   PostgreSQL    │          │   Tableau Dashboard  │
│  (메트릭 DB)    │          │  (v_dashboard_main)  │
└─────────────────┘          └─────────────────────┘
```

---

## 핵심 KPI 정의 (7종)

| # | KPI | 정의 | SQL 핵심 로직 | 단위 |
|---|-----|------|--------------|------|
| 1 | **월별 이용금액** | 카드사별 월간 이용금액 합계 | `SUM(usage_amount)` + `LAG()` 전월/전년 비교 | 억원 |
| 2 | **시장 점유율** | 전체 대비 카드사 비율 | `company / total × 100` + 변동폭(pp) | % |
| 3 | **성장률** | MoM / YoY / 3M 이동평균 | `LAG()` + `AVG() OVER (ROWS 2 PRECEDING)` | % |
| 4 | **업종별 이용 패턴** | 카드사별·업종별 비중 | `WINDOW` 함수 + 업종 MoM 성장률 | % |
| 5 | **카드 활성화율** | 발급 대비 이용 카드 비율 | 활성화율 + 업계 평균 대비 차이 | % |
| 6 | **시장 집중도** | HHI(Σ점유율²) + CR3 | `POWER()` + 상위 3사 서브쿼리 | 지수 |
| 7 | **이상치 탐지** | Z-Score + IQR 기반 | `AVG()/STDDEV()` + `PERCENTILE_CONT()` | Z |

### Tableau 연동 뷰
- `v_dashboard_main` — 전체 KPI 통합 뷰 (7개 KPI 조인)
- `v_dashboard_market_structure` — 시장 구조 추이 뷰

---

## 정합성 검증 체계 (10종)

| # | 검증 항목 | 카테고리 | 심각도 | 판정 기준 |
|---|----------|---------|--------|----------|
| 1 | 전체 = 카드사별 합계 | `sum_integrity` | CRITICAL | 차이 < 1원 |
| 2 | 점유율 합계 = 100% | `ratio_integrity` | CRITICAL | 차이 < 0.1% |
| 3 | 업종별 비율 합계 = 100% | `ratio_integrity` | WARNING | 차이 < 0.5% |
| 4 | MoM 성장률 역산 | `formula_integrity` | WARNING | 차이 < 10원 |
| 5 | YoY 성장률 역산 | `formula_integrity` | WARNING | 차이 < 10원 |
| 6 | 활성화율 0~100% | `range_integrity` | CRITICAL | 범위 이탈 = FAIL |
| 7 | 데이터 연속성 (월 누락) | `continuity_integrity` | CRITICAL | 누락 = FAIL |
| 8 | HHI 범위 0~10000 | `range_integrity` | WARNING | 범위 이탈 = FAIL |
| 9 | Z-Score 이상치 비율 < 5% | `statistical_integrity` | WARNING | CRITICAL 비율 > 5% |
| 10 | 점유율 ↔ 성장률 방향성 | `cross_kpi_integrity` | INFO | 방향 불일치 = FAIL |

### 심각도 체계
- **CRITICAL** — 즉시 알림 + 온콜 에스컬레이션 (2회 연속 시)
- **WARNING** — Slack 경고 + 일별 리포트 포함
- **INFO** — 로그 기록 + 리포트 참고용

---

## Tableau 대시보드 구성

### KPI 개요 대시보드
- 카드사별 월별 이용금액 추이 (라인차트)
- 시장 점유율 파이차트 + 점유율 변동 히트맵
- 주요 KPI 카드 (전월 대비 변화 포함)

### 성장 트렌드 대시보드
- MoM / YoY 성장률 이중축 라인차트
- 3개월 이동평균 트렌드 라인
- 업종별 이용 패턴 히트맵 + 트리맵

### 정합성 모니터링 대시보드
- 검증 항목별 Pass/Fail 상태 매트릭스
- 일별 통과율 추이 (`v_integrity_daily_trend`)
- CRITICAL 실패 알림 로그

---

## 실행 방법

### 1. 정합성 검증 단독 실행 (데모 데이터)
```bash
# 기본 실행 (10종 검증 + 콘솔 리포트)
python scripts/run_integrity_checks.py

# YAML 설정 적용 + HTML 리포트
python scripts/run_integrity_checks.py --config config/thresholds.yaml --export html

# 전체 포맷 내보내기
python scripts/run_integrity_checks.py --export all --output reports/
```

### 2. 샘플 데이터 생성
```bash
# SQL INSERT 문 생성
python scripts/generate_sample_data.py --format sql --output data/seed.sql

# CSV 파일 생성
python scripts/generate_sample_data.py --format csv --output data/
```

### 3. 테스트 실행
```bash
pytest tests/ -v --tb=short
pytest tests/ -v --cov=scripts --cov-report=term-missing
```

### 4. Docker Compose (Airflow + PostgreSQL)
```bash
# 전체 기동
docker-compose up -d

# Airflow UI 접속
open http://localhost:8080    # admin / admin

# DAG 실행 확인
docker-compose logs -f airflow-scheduler

# 종료
docker-compose down -v
```

### 5. PostgreSQL에 샘플 데이터 적재
```bash
# 샘플 데이터 생성 후 적재
python scripts/generate_sample_data.py --format sql --output data/seed.sql
docker-compose exec metrics-db psql -U metrics -d metrics_db -f /docker-entrypoint-initdb.d/01_schema.sql
cat data/seed.sql | docker-compose exec -T metrics-db psql -U metrics -d metrics_db
```

---

## 프로젝트 구조

```
metrics-quality-dashboard/
├── dags/
│   └── metrics_quality_dag.py         # Airflow DAG (TaskGroup + SLA + 알림)
├── sql/
│   ├── kpi_definitions.sql            # KPI 7종 + Tableau 뷰
│   └── integrity_checks.sql           # 정합성 검증 10종 SQL
├── scripts/
│   ├── run_integrity_checks.py        # Python 검증 엔진 (10종 + 3 포맷 리포트)
│   └── generate_sample_data.py        # 샘플 데이터 생성기
├── config/
│   └── thresholds.yaml                # 검증 임계값 + 알림 + DAG 설정
├── tests/
│   └── test_integrity_checks.py       # pytest 단위/통합 테스트
├── reports/                           # 검증 리포트 출력 디렉토리
├── data/                              # 샘플 데이터 출력 디렉토리
├── docker-compose.yaml                # Airflow + PostgreSQL 인프라
├── requirements.txt
└── README.md
```

---

## 기술 스택

| 영역 | 기술 | 활용 |
|------|------|------|
| **지표 산출** | PostgreSQL 14+ | Window 함수, CTE, 통계 함수 |
| **정합성 검증** | Python 3.11 | Dataclass, Enum, 통계 모듈, YAML 설정 |
| **오케스트레이션** | Apache Airflow 2.7 | TaskGroup, BranchOperator, SLA, 콜백 |
| **시각화** | Tableau Public | 뷰 기반 라이브 연결, 3종 대시보드 |
| **인프라** | Docker Compose | Airflow + PostgreSQL 원클릭 기동 |
| **테스트** | pytest + coverage | 단위/통합 테스트 30+ 케이스 |
| **알림** | Slack Webhook | CRITICAL/WARNING 자동 알림 + 에스컬레이션 |

---

## 데이터 소스

| 항목 | 내용 |
|------|------|
| **출처** | [여신금융협회](https://www.crefia.or.kr) |
| **내용** | 카드사별 신용카드 이용실적 (월별 이용금액, 이용건수, 업종별 분류) |
| **기간** | 2024년 1월 ~ 2025년 12월 (24개월) |
| **갱신** | 월 1회 (익월 말 공개) |
| **카드사** | 신한, 삼성, KB국민, 현대, 우리, 하나, 롯데, BC (8개사) |
| **업종** | 음식점, 주유소, 대형마트, 온라인쇼핑, 교통, 의료, 교육, 여행/숙박, 보험, 기타 |

---

## 설계 의사결정

### 왜 10종 교차 검증인가?
단순 합계 검증만으로는 지표 품질을 보장할 수 없습니다. **비율 정합, 산출식 역산, 통계적 이상치, KPI 간 교차 검증**까지 다층적으로 검증해야 프로덕션 수준의 데이터 품질이 확보됩니다.

### 왜 3단계 심각도(Severity)인가?
모든 실패를 동일하게 처리하면 알림 피로(alert fatigue)가 발생합니다. CRITICAL은 즉시 대응, WARNING은 일별 리뷰, INFO는 참고용으로 분류하여 **운영 효율성**을 확보했습니다.

### 왜 YAML 설정 관리인가?
임계값을 코드에 하드코딩하면 변경 시마다 배포가 필요합니다. YAML로 분리하면 **코드 변경 없이 임계값 튜닝**이 가능하고, 환경별 설정 관리가 용이합니다.

### 왜 HTML 리포트인가?
JSON/CSV는 시스템 간 연동에 적합하지만, 비개발 직군(PM, 분석가)에게는 **시각적 리포트**가 필요합니다. HTML 리포트는 브라우저에서 바로 확인 가능하며, Slack/이메일 첨부도 용이합니다.
