-- ============================================================
-- 핵심 지표(KPI) 정의 및 산출 로직
-- ============================================================
-- 데이터 소스  : 여신금융협회 신용카드 이용실적 데이터
-- 대상 기간    : 2024-01 ~ 2025-12 (24개월)
-- 갱신 주기    : 월 1회 (익월 말 공개)
-- 작성 기준    : PostgreSQL 14+
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 0. 원천 테이블 스키마 & 인덱스
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS credit_card_usage (
    year_month        DATE         NOT NULL,
    card_company      VARCHAR(50)  NOT NULL,
    business_category VARCHAR(100),
    usage_amount      BIGINT       NOT NULL DEFAULT 0,   -- 백만원 단위
    usage_count       BIGINT       NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS card_issuance_stats (
    year_month          DATE         NOT NULL,
    card_company        VARCHAR(50)  NOT NULL,
    total_issued_cards  BIGINT       NOT NULL DEFAULT 0,
    active_cards        BIGINT       NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ccu_ym_company
    ON credit_card_usage (year_month, card_company);
CREATE INDEX IF NOT EXISTS idx_ccu_ym_company_cat
    ON credit_card_usage (year_month, card_company, business_category);
CREATE INDEX IF NOT EXISTS idx_cis_ym_company
    ON card_issuance_stats (year_month, card_company);


-- ════════════════════════════════════════════════════════════
-- KPI 1. 월별 카드사별 이용금액 (Monthly Usage Amount)
-- ════════════════════════════════════════════════════════════
-- 정의  : 특정 월 · 카드사의 신용카드 이용금액 합계
-- 단위  : 억원 (원천 백만원 / 100)
-- 비고  : 후속 KPI(점유율, 성장률)에서 재활용하는 Base KPI

DROP TABLE IF EXISTS kpi_monthly_usage CASCADE;
CREATE TABLE kpi_monthly_usage AS
SELECT
    year_month,
    card_company,
    SUM(usage_amount)                                              AS total_usage_amount,
    SUM(usage_count)                                               AS total_usage_count,
    ROUND(SUM(usage_amount)::NUMERIC / NULLIF(SUM(usage_count), 0), 0)
                                                                   AS avg_transaction_amount,
    -- Window: 전월 이용금액
    LAG(SUM(usage_amount)) OVER (
        PARTITION BY card_company ORDER BY year_month
    )                                                              AS prev_month_amount,
    -- Window: 전년 동월 이용금액
    LAG(SUM(usage_amount), 12) OVER (
        PARTITION BY card_company ORDER BY year_month
    )                                                              AS prev_year_amount
FROM credit_card_usage
GROUP BY year_month, card_company
ORDER BY year_month DESC, total_usage_amount DESC;

CREATE INDEX IF NOT EXISTS idx_kmu_ym      ON kpi_monthly_usage (year_month);
CREATE INDEX IF NOT EXISTS idx_kmu_company ON kpi_monthly_usage (card_company);


-- ════════════════════════════════════════════════════════════
-- KPI 2. 카드사별 시장 점유율 (Market Share)
-- ════════════════════════════════════════════════════════════
-- 정의  : 월별 전체 이용금액 대비 카드사 비율
-- 단위  : % (소수 2자리)
-- 추가  : 전월 대비 점유율 변동(pp), 전년 동월 점유율

DROP TABLE IF EXISTS kpi_market_share CASCADE;
CREATE TABLE kpi_market_share AS
WITH market_totals AS (
    SELECT year_month,
           SUM(total_usage_amount) AS market_total
    FROM kpi_monthly_usage
    GROUP BY year_month
),
share_calc AS (
    SELECT
        u.year_month,
        u.card_company,
        u.total_usage_amount,
        m.market_total,
        ROUND(u.total_usage_amount::NUMERIC / NULLIF(m.market_total, 0) * 100, 2)
                                                                   AS market_share_pct,
        RANK() OVER (
            PARTITION BY u.year_month ORDER BY u.total_usage_amount DESC
        )                                                          AS market_rank
    FROM kpi_monthly_usage u
    JOIN market_totals m ON u.year_month = m.year_month
)
SELECT
    s.*,
    LAG(s.market_share_pct) OVER (
        PARTITION BY s.card_company ORDER BY s.year_month
    )                                                              AS prev_month_share,
    ROUND(
        s.market_share_pct - COALESCE(
            LAG(s.market_share_pct) OVER (
                PARTITION BY s.card_company ORDER BY s.year_month
            ), s.market_share_pct
        ), 2
    )                                                              AS share_change_pp,
    LAG(s.market_share_pct, 12) OVER (
        PARTITION BY s.card_company ORDER BY s.year_month
    )                                                              AS prev_year_share
FROM share_calc s;


-- ════════════════════════════════════════════════════════════
-- KPI 3. 성장률 — MoM / YoY / 3M 이동평균
-- ════════════════════════════════════════════════════════════
-- MoM  : (당월 − 전월) / 전월 × 100
-- YoY  : (당월 − 전년동월) / 전년동월 × 100
-- 3M MA: 최근 3개월 MoM 이동평균 (노이즈 제거)

DROP TABLE IF EXISTS kpi_growth_rate CASCADE;
CREATE TABLE kpi_growth_rate AS
WITH base AS (
    SELECT year_month, card_company,
           total_usage_amount  AS current_amount,
           prev_month_amount,
           prev_year_amount
    FROM kpi_monthly_usage
)
SELECT
    b.year_month,
    b.card_company,
    b.current_amount,
    b.prev_month_amount,
    b.prev_year_amount,
    -- MoM
    ROUND(
        (b.current_amount - COALESCE(b.prev_month_amount, 0))::NUMERIC
        / NULLIF(b.prev_month_amount, 0) * 100, 2
    )                                                              AS mom_growth_rate,
    -- YoY
    ROUND(
        (b.current_amount - COALESCE(b.prev_year_amount, 0))::NUMERIC
        / NULLIF(b.prev_year_amount, 0) * 100, 2
    )                                                              AS yoy_growth_rate,
    -- 3개월 이동평균 MoM
    ROUND(AVG(
        (b.current_amount - COALESCE(b.prev_month_amount, 0))::NUMERIC
        / NULLIF(b.prev_month_amount, 0) * 100
    ) OVER (
        PARTITION BY b.card_company
        ORDER BY b.year_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                                          AS mom_3m_moving_avg,
    -- 연환산 성장률 (12개월 CAGR 근사)
    CASE
        WHEN b.prev_year_amount > 0 THEN
            ROUND(
                (POWER(b.current_amount::NUMERIC / b.prev_year_amount, 1.0) - 1) * 100, 2
            )
        ELSE NULL
    END                                                            AS annual_growth_rate
FROM base b
ORDER BY b.year_month DESC, b.card_company;


-- ════════════════════════════════════════════════════════════
-- KPI 4. 업종별 이용 패턴 (Category Usage Pattern)
-- ════════════════════════════════════════════════════════════
-- 정의  : 카드사별·업종별 이용금액 및 비중
-- 추가  : 업종 순위, 전월 비교, 업종 MoM 성장률

DROP TABLE IF EXISTS kpi_category_usage CASCADE;
CREATE TABLE kpi_category_usage AS
WITH cat_base AS (
    SELECT
        year_month,
        card_company,
        business_category,
        SUM(usage_amount) AS category_amount,
        SUM(usage_count)  AS category_count
    FROM credit_card_usage
    WHERE business_category IS NOT NULL
    GROUP BY year_month, card_company, business_category
)
SELECT
    cb.year_month,
    cb.card_company,
    cb.business_category,
    cb.category_amount,
    cb.category_count,
    ROUND(
        cb.category_amount::NUMERIC
        / NULLIF(SUM(cb.category_amount) OVER w_company, 0) * 100, 2
    )                                                              AS category_share_pct,
    RANK() OVER (
        PARTITION BY cb.year_month, cb.business_category
        ORDER BY cb.category_amount DESC
    )                                                              AS category_rank,
    LAG(cb.category_amount) OVER (
        PARTITION BY cb.card_company, cb.business_category
        ORDER BY cb.year_month
    )                                                              AS prev_month_cat_amount,
    ROUND(
        (cb.category_amount - COALESCE(
            LAG(cb.category_amount) OVER (
                PARTITION BY cb.card_company, cb.business_category
                ORDER BY cb.year_month
            ), 0
        ))::NUMERIC / NULLIF(LAG(cb.category_amount) OVER (
            PARTITION BY cb.card_company, cb.business_category
            ORDER BY cb.year_month
        ), 0) * 100, 2
    )                                                              AS category_mom_growth
FROM cat_base cb
WINDOW w_company AS (PARTITION BY cb.year_month, cb.card_company)
ORDER BY cb.year_month DESC, cb.card_company, cb.category_amount DESC;


-- ════════════════════════════════════════════════════════════
-- KPI 5. 카드 활성화율 (Card Activation Rate)
-- ════════════════════════════════════════════════════════════
-- 정의  : 발급 카드 대비 실제 이용 카드 비율
-- 추가  : 전월 변동폭, 업계 평균 대비 차이

DROP TABLE IF EXISTS kpi_activation_rate CASCADE;
CREATE TABLE kpi_activation_rate AS
SELECT
    year_month,
    card_company,
    total_issued_cards,
    active_cards,
    ROUND(active_cards::NUMERIC / NULLIF(total_issued_cards, 0) * 100, 2)
                                                                   AS activation_rate,
    LAG(ROUND(active_cards::NUMERIC / NULLIF(total_issued_cards, 0) * 100, 2))
        OVER (PARTITION BY card_company ORDER BY year_month)
                                                                   AS prev_month_rate,
    ROUND(
        active_cards::NUMERIC / NULLIF(total_issued_cards, 0) * 100
        - COALESCE(LAG(active_cards::NUMERIC / NULLIF(total_issued_cards, 0) * 100)
            OVER (PARTITION BY card_company ORDER BY year_month), 0), 2
    )                                                              AS rate_change_pp,
    ROUND(
        active_cards::NUMERIC / NULLIF(total_issued_cards, 0) * 100
        - AVG(active_cards::NUMERIC / NULLIF(total_issued_cards, 0) * 100)
            OVER (PARTITION BY year_month), 2
    )                                                              AS vs_industry_avg
FROM card_issuance_stats
ORDER BY year_month DESC, activation_rate DESC;


-- ════════════════════════════════════════════════════════════
-- KPI 6. 시장 집중도 — HHI & CR3
-- ════════════════════════════════════════════════════════════
-- HHI  : Σ(점유율²), 0~10000
-- CR3  : 상위 3사 합산 점유율
-- 해석 : HHI<1500 경쟁적 / 1500~2500 보통 / >2500 과점

DROP TABLE IF EXISTS kpi_market_concentration CASCADE;
CREATE TABLE kpi_market_concentration AS
SELECT
    year_month,
    ROUND(SUM(POWER(market_share_pct, 2)), 2)                      AS hhi_index,
    CASE
        WHEN SUM(POWER(market_share_pct, 2)) < 1500 THEN '경쟁적'
        WHEN SUM(POWER(market_share_pct, 2)) < 2500 THEN '보통 집중'
        ELSE '고도 집중'
    END                                                            AS concentration_level,
    COUNT(DISTINCT card_company)                                   AS num_companies,
    MAX(market_share_pct)                                          AS top1_share,
    (SELECT SUM(sub.market_share_pct)
     FROM (SELECT market_share_pct
           FROM kpi_market_share ms2
           WHERE ms2.year_month = ms.year_month
           ORDER BY market_share_pct DESC LIMIT 3) sub
    )                                                              AS cr3_share
FROM kpi_market_share ms
GROUP BY year_month
ORDER BY year_month DESC;


-- ════════════════════════════════════════════════════════════
-- KPI 7. 이상치 탐지 — Z-Score 기반
-- ════════════════════════════════════════════════════════════
-- |Z| > 3 → CRITICAL,  |Z| > 2 → WARNING

DROP TABLE IF EXISTS kpi_anomaly_detection CASCADE;
CREATE TABLE kpi_anomaly_detection AS
WITH stats AS (
    SELECT card_company,
           AVG(total_usage_amount)    AS mean_amount,
           STDDEV(total_usage_amount) AS stddev_amount
    FROM kpi_monthly_usage
    GROUP BY card_company
)
SELECT
    u.year_month,
    u.card_company,
    u.total_usage_amount,
    s.mean_amount,
    s.stddev_amount,
    ROUND(
        (u.total_usage_amount - s.mean_amount)::NUMERIC / NULLIF(s.stddev_amount, 0), 3
    )                                                              AS z_score,
    CASE
        WHEN ABS((u.total_usage_amount - s.mean_amount)::NUMERIC
                 / NULLIF(s.stddev_amount, 0)) > 3 THEN 'CRITICAL'
        WHEN ABS((u.total_usage_amount - s.mean_amount)::NUMERIC
                 / NULLIF(s.stddev_amount, 0)) > 2 THEN 'WARNING'
        ELSE 'NORMAL'
    END                                                            AS anomaly_level,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY u.total_usage_amount)
        OVER (PARTITION BY u.card_company)                         AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY u.total_usage_amount)
        OVER (PARTITION BY u.card_company)                         AS q3
FROM kpi_monthly_usage u
JOIN stats s ON u.card_company = s.card_company
ORDER BY ABS((u.total_usage_amount - s.mean_amount)::NUMERIC
             / NULLIF(s.stddev_amount, 0)) DESC;


-- ════════════════════════════════════════════════════════════
-- 뷰: Tableau 대시보드 연동용 통합 뷰
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_dashboard_main AS
SELECT
    u.year_month,
    u.card_company,
    u.total_usage_amount,
    u.total_usage_count,
    u.avg_transaction_amount,
    ms.market_share_pct,
    ms.market_rank,
    ms.share_change_pp,
    g.mom_growth_rate,
    g.yoy_growth_rate,
    g.mom_3m_moving_avg,
    ar.activation_rate,
    ar.vs_industry_avg      AS activation_vs_avg,
    ad.z_score,
    ad.anomaly_level
FROM kpi_monthly_usage u
LEFT JOIN kpi_market_share ms
    ON u.year_month = ms.year_month AND u.card_company = ms.card_company
LEFT JOIN kpi_growth_rate g
    ON u.year_month = g.year_month AND u.card_company = g.card_company
LEFT JOIN kpi_activation_rate ar
    ON u.year_month = ar.year_month AND u.card_company = ar.card_company
LEFT JOIN kpi_anomaly_detection ad
    ON u.year_month = ad.year_month AND u.card_company = ad.card_company;

CREATE OR REPLACE VIEW v_dashboard_market_structure AS
SELECT
    mc.year_month,
    mc.hhi_index,
    mc.concentration_level,
    mc.num_companies,
    mc.top1_share,
    mc.cr3_share,
    LAG(mc.hhi_index) OVER (ORDER BY mc.year_month)                AS prev_hhi,
    ROUND(mc.hhi_index - COALESCE(
        LAG(mc.hhi_index) OVER (ORDER BY mc.year_month), mc.hhi_index
    ), 2)                                                          AS hhi_change
FROM kpi_market_concentration mc;
