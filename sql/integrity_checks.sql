-- ============================================================
-- 지표 정합성 검증 쿼리 (Cross-Validation Integrity Checks)
-- ============================================================
-- 10가지 교차 검증을 통해 KPI 지표의 데이터 정합성을 보장합니다.
-- 검증 결과는 integrity_check_log 에 기록되며
-- Airflow → Python 스크립트 → Slack/리포트 순으로 소비됩니다.
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 검증 로그 테이블
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS integrity_check_log (
    check_id        SERIAL PRIMARY KEY,
    check_date      DATE         NOT NULL DEFAULT CURRENT_DATE,
    check_name      VARCHAR(200) NOT NULL,
    check_category  VARCHAR(50)  NOT NULL,
    severity        VARCHAR(10)  NOT NULL DEFAULT 'WARNING',  -- CRITICAL / WARNING / INFO
    expected_value  NUMERIC,
    actual_value    NUMERIC,
    difference      NUMERIC,
    tolerance       NUMERIC      DEFAULT 0.01,
    status          VARCHAR(10)  NOT NULL,                     -- PASS / FAIL
    detail          TEXT,
    created_at      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_icl_date   ON integrity_check_log (check_date);
CREATE INDEX IF NOT EXISTS idx_icl_status ON integrity_check_log (status);


-- ════════════════════════════════════════════════════════════
-- CHECK 1. 전체 이용금액 = 카드사별 합계  [합계 정합성]
-- ════════════════════════════════════════════════════════════
-- 원천 SUM(usage_amount) 과 kpi_monthly_usage 합계 비교
-- severity: CRITICAL — 기본 합계가 불일치하면 전체 지표 신뢰 불가

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    '전체 이용금액 = 카드사별 합계',
    'sum_integrity',
    'CRITICAL',
    src.total_amount,
    agg.sum_by_company,
    ABS(src.total_amount - agg.sum_by_company),
    1,
    CASE WHEN ABS(src.total_amount - agg.sum_by_company) < 1 THEN 'PASS' ELSE 'FAIL' END,
    'year_month=' || src.year_month::TEXT
FROM (
    SELECT year_month, SUM(usage_amount) AS total_amount
    FROM credit_card_usage GROUP BY year_month
) src
JOIN (
    SELECT year_month, SUM(total_usage_amount) AS sum_by_company
    FROM kpi_monthly_usage GROUP BY year_month
) agg ON src.year_month = agg.year_month;


-- ════════════════════════════════════════════════════════════
-- CHECK 2. 시장 점유율 합계 = 100%  [비율 정합성]
-- ════════════════════════════════════════════════════════════

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    '시장 점유율 합계 = 100%',
    'ratio_integrity',
    'CRITICAL',
    100.00,
    SUM(market_share_pct),
    ABS(100.00 - SUM(market_share_pct)),
    0.1,
    CASE WHEN ABS(100.00 - SUM(market_share_pct)) < 0.1 THEN 'PASS' ELSE 'FAIL' END,
    'year_month=' || year_month::TEXT
FROM kpi_market_share
GROUP BY year_month;


-- ════════════════════════════════════════════════════════════
-- CHECK 3. 업종별 비율 합계 = 100% (카드사별)  [비율 정합성]
-- ════════════════════════════════════════════════════════════

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    '업종별 비율 합계 = 100% (카드사별)',
    'ratio_integrity',
    'WARNING',
    100.00,
    SUM(category_share_pct),
    ABS(100.00 - SUM(category_share_pct)),
    0.5,
    CASE WHEN ABS(100.00 - SUM(category_share_pct)) < 0.5 THEN 'PASS' ELSE 'FAIL' END,
    'year_month=' || year_month::TEXT || ', company=' || card_company
FROM kpi_category_usage
GROUP BY year_month, card_company;


-- ════════════════════════════════════════════════════════════
-- CHECK 4. MoM 성장률 역산 검증  [산출식 정합성]
-- ════════════════════════════════════════════════════════════
-- current_amount / (1 + mom_growth_rate/100) ≈ prev_month_amount

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    'MoM 성장률 역산 검증',
    'formula_integrity',
    'WARNING',
    prev_month_amount,
    ROUND(current_amount / (1 + mom_growth_rate / 100.0), 0),
    ABS(prev_month_amount - ROUND(current_amount / (1 + mom_growth_rate / 100.0), 0)),
    10,
    CASE
        WHEN ABS(prev_month_amount - ROUND(current_amount / (1 + mom_growth_rate / 100.0), 0)) < 10
        THEN 'PASS' ELSE 'FAIL'
    END,
    'year_month=' || year_month::TEXT || ', company=' || card_company
FROM kpi_growth_rate
WHERE mom_growth_rate IS NOT NULL
  AND prev_month_amount IS NOT NULL
  AND mom_growth_rate != 0;


-- ════════════════════════════════════════════════════════════
-- CHECK 5. YoY 성장률 역산 검증  [산출식 정합성]
-- ════════════════════════════════════════════════════════════

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    'YoY 성장률 역산 검증',
    'formula_integrity',
    'WARNING',
    prev_year_amount,
    ROUND(current_amount / (1 + yoy_growth_rate / 100.0), 0),
    ABS(prev_year_amount - ROUND(current_amount / (1 + yoy_growth_rate / 100.0), 0)),
    10,
    CASE
        WHEN ABS(prev_year_amount - ROUND(current_amount / (1 + yoy_growth_rate / 100.0), 0)) < 10
        THEN 'PASS' ELSE 'FAIL'
    END,
    'year_month=' || year_month::TEXT || ', company=' || card_company
FROM kpi_growth_rate
WHERE yoy_growth_rate IS NOT NULL
  AND prev_year_amount IS NOT NULL
  AND yoy_growth_rate != 0;


-- ════════════════════════════════════════════════════════════
-- CHECK 6. 활성화율 범위 검증 (0~100%)  [범위 정합성]
-- ════════════════════════════════════════════════════════════

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    '활성화율 범위 검증 (0~100%)',
    'range_integrity',
    'CRITICAL',
    100.00,
    activation_rate,
    CASE WHEN activation_rate < 0 OR activation_rate > 100 THEN ABS(activation_rate) ELSE 0 END,
    0,
    CASE WHEN activation_rate BETWEEN 0 AND 100 THEN 'PASS' ELSE 'FAIL' END,
    'year_month=' || year_month::TEXT || ', company=' || card_company
FROM kpi_activation_rate;


-- ════════════════════════════════════════════════════════════
-- CHECK 7. 데이터 연속성 검증 — 월 누락  [연속성 정합성]
-- ════════════════════════════════════════════════════════════

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    '데이터 연속성 검증 (월 누락)',
    'continuity_integrity',
    'CRITICAL',
    expected_months,
    actual_months,
    expected_months - actual_months,
    0,
    CASE WHEN expected_months = actual_months THEN 'PASS' ELSE 'FAIL' END,
    'company=' || card_company || ', missing=' || (expected_months - actual_months)::TEXT || ' months'
FROM (
    SELECT
        card_company,
        EXTRACT(YEAR  FROM AGE(MAX(year_month), MIN(year_month))) * 12 +
        EXTRACT(MONTH FROM AGE(MAX(year_month), MIN(year_month))) + 1 AS expected_months,
        COUNT(DISTINCT year_month) AS actual_months
    FROM kpi_monthly_usage
    GROUP BY card_company
) monthly_check;


-- ════════════════════════════════════════════════════════════
-- CHECK 8. HHI 범위 검증 (0~10000)  [범위 정합성]
-- ════════════════════════════════════════════════════════════

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    'HHI 범위 검증 (0~10000)',
    'range_integrity',
    'WARNING',
    10000,
    hhi_index,
    CASE WHEN hhi_index < 0 OR hhi_index > 10000 THEN ABS(hhi_index) ELSE 0 END,
    0,
    CASE WHEN hhi_index BETWEEN 0 AND 10000 THEN 'PASS' ELSE 'FAIL' END,
    'year_month=' || year_month::TEXT || ', hhi=' || hhi_index::TEXT
FROM kpi_market_concentration;


-- ════════════════════════════════════════════════════════════
-- CHECK 9. Z-Score 이상치 — 과도한 이상치 비율  [통계 정합성]
-- ════════════════════════════════════════════════════════════
-- 전체 데이터 중 |Z| > 3 인 비율이 5% 이상이면 데이터 품질 이슈

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    'Z-Score 이상치 비율 검증 (<5%)',
    'statistical_integrity',
    'WARNING',
    5.0,
    ROUND(anomaly_ratio * 100, 2),
    CASE WHEN anomaly_ratio * 100 > 5 THEN ROUND(anomaly_ratio * 100 - 5, 2) ELSE 0 END,
    5.0,
    CASE WHEN anomaly_ratio * 100 <= 5 THEN 'PASS' ELSE 'FAIL' END,
    'critical_count=' || critical_cnt::TEXT || ', total=' || total_cnt::TEXT
FROM (
    SELECT
        COUNT(*) FILTER (WHERE anomaly_level = 'CRITICAL')::NUMERIC / NULLIF(COUNT(*), 0) AS anomaly_ratio,
        COUNT(*) FILTER (WHERE anomaly_level = 'CRITICAL') AS critical_cnt,
        COUNT(*) AS total_cnt
    FROM kpi_anomaly_detection
) anomaly_summary;


-- ════════════════════════════════════════════════════════════
-- CHECK 10. 점유율 변동 ↔ 성장률 교차 검증  [교차 정합성]
-- ════════════════════════════════════════════════════════════
-- 점유율이 +인데 성장률이 −이면 논리 불일치 (시장 전체가 축소하지 않는 한)
-- 시장 전체 성장률이 양수일 때만 검증

INSERT INTO integrity_check_log
    (check_name, check_category, severity, expected_value, actual_value, difference, tolerance, status, detail)
SELECT
    '점유율 변동 ↔ 성장률 교차 검증',
    'cross_kpi_integrity',
    'INFO',
    0,
    inconsistent_count,
    inconsistent_count,
    0,
    CASE WHEN inconsistent_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    'month=' || ym::TEXT || ', inconsistent_companies=' || inconsistent_count::TEXT
FROM (
    SELECT
        ms.year_month AS ym,
        COUNT(*) FILTER (
            WHERE ms.share_change_pp > 0.5 AND g.mom_growth_rate < -1
        ) AS inconsistent_count
    FROM kpi_market_share ms
    JOIN kpi_growth_rate g
        ON ms.year_month = g.year_month AND ms.card_company = g.card_company
    WHERE ms.share_change_pp IS NOT NULL
      AND g.mom_growth_rate IS NOT NULL
    GROUP BY ms.year_month
) cross_check;


-- ════════════════════════════════════════════════════════════
-- 정합성 검증 결과 요약 뷰
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_integrity_summary AS
SELECT
    check_date,
    check_category,
    severity,
    COUNT(*)                                                       AS total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)              AS passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)              AS failed,
    ROUND(
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                              AS pass_rate
FROM integrity_check_log
GROUP BY check_date, check_category, severity
ORDER BY check_date DESC, pass_rate ASC;

-- CRITICAL 실패만 필터링 (알림 대상)
CREATE OR REPLACE VIEW v_integrity_critical_failures AS
SELECT *
FROM integrity_check_log
WHERE status = 'FAIL'
  AND severity = 'CRITICAL'
  AND check_date = CURRENT_DATE
ORDER BY created_at DESC;

-- 일별 통과율 추이 (Tableau 시계열)
CREATE OR REPLACE VIEW v_integrity_daily_trend AS
SELECT
    check_date,
    COUNT(*)                                                       AS total,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)              AS passed,
    ROUND(
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                              AS daily_pass_rate,
    SUM(CASE WHEN severity = 'CRITICAL' AND status = 'FAIL' THEN 1 ELSE 0 END)
                                                                   AS critical_failures
FROM integrity_check_log
GROUP BY check_date
ORDER BY check_date DESC;
