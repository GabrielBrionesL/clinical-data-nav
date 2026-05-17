-- tableau_clinical_views.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Tableau-Ready BigQuery Views for Plume Clinical Analytics
--
-- Purpose:
--   These views are designed for direct connection from Tableau Desktop or
--   Tableau Server to BigQuery. They pre-join the dimensional model and
--   flatten the schema to minimise the number of joins Tableau needs to
--   perform, improving dashboard performance.
--
-- Usage:
--   1. Run this script against your BigQuery project to create the views
--      in the `plume_bi` dataset.
--   2. In Tableau, connect to BigQuery and select the `plume_bi` dataset.
--   3. Use the views below as data sources for your workbooks.
--
-- Replace `YOUR_PROJECT_ID` with your GCP project ID before running.
-- ─────────────────────────────────────────────────────────────────────────────

-- Create the BI dataset if it doesn't exist
-- (Run this in the BigQuery console or via bq CLI before executing the views)
-- CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.plume_bi`;


-- ─────────────────────────────────────────────────────────────────────────────
-- View 1: vw_clinical_dashboard
-- ─────────────────────────────────────────────────────────────────────────────
-- Business Purpose:
--   Primary data source for the Clinical Operations Dashboard in Tableau.
--   Provides one row per patient with their latest lab results and current
--   subscription status. Designed for the care team's daily patient review.
--
-- Key Metrics Enabled:
--   - Active patient count by state (map)
--   - HRT category distribution (pie/bar chart)
--   - Patients with out-of-range labs (filtered table for care outreach)
--   - Subscription status breakdown (stacked bar)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.plume_bi.vw_clinical_dashboard` AS
WITH latest_labs AS (
    -- Get the most recent lab result for each patient and test
    SELECT
        patient_token,
        test_name,
        result_value,
        unit,
        flag,
        is_in_therapeutic_target,
        lab_date,
        ROW_NUMBER() OVER (
            PARTITION BY patient_token, test_name
            ORDER BY lab_date DESC
        ) AS rn
    FROM `YOUR_PROJECT_ID.plume_gold.fact_lab_results`
),
pivoted_labs AS (
    -- Pivot the most recent lab values into columns for Tableau
    SELECT
        patient_token,
        MAX(CASE WHEN test_name = 'Estradiol'    THEN result_value END) AS latest_estradiol_value,
        MAX(CASE WHEN test_name = 'Estradiol'    THEN unit         END) AS estradiol_unit,
        MAX(CASE WHEN test_name = 'Estradiol'    THEN flag         END) AS estradiol_flag,
        MAX(CASE WHEN test_name = 'Testosterone' THEN result_value END) AS latest_testosterone_value,
        MAX(CASE WHEN test_name = 'Testosterone' THEN unit         END) AS testosterone_unit,
        MAX(CASE WHEN test_name = 'Testosterone' THEN flag         END) AS testosterone_flag,
        MAX(CASE WHEN test_name = 'Potassium'    THEN result_value END) AS latest_potassium_value,
        MAX(CASE WHEN test_name = 'Potassium'    THEN flag         END) AS potassium_flag,
        MAX(lab_date)                                                    AS latest_lab_date
    FROM latest_labs
    WHERE rn = 1
    GROUP BY patient_token
)
SELECT
    -- Patient demographics (no raw PHI — patient_token only)
    p.patient_token,
    p.gender_identity,
    p.hrt_category,
    p.hrt_regimen,
    p.state,
    p.age_band,
    p.start_date,
    p.tenure_months,

    -- Subscription info
    p.subscription_status,
    p.plan_type,
    p.mrr,
    p.is_active,

    -- Latest lab values (pivoted)
    l.latest_estradiol_value,
    l.estradiol_unit,
    l.estradiol_flag,
    l.latest_testosterone_value,
    l.testosterone_unit,
    l.testosterone_flag,
    l.latest_potassium_value,
    l.potassium_flag,
    l.latest_lab_date,

    -- Derived flags for Tableau filters
    CASE
        WHEN l.latest_lab_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
          OR l.latest_lab_date IS NULL
        THEN TRUE
        ELSE FALSE
    END AS labs_overdue_90d,

    CASE
        WHEN l.estradiol_flag != 'Normal' OR l.testosterone_flag != 'Normal'
          OR l.potassium_flag != 'Normal'
        THEN TRUE
        ELSE FALSE
    END AS has_out_of_range_lab

FROM `YOUR_PROJECT_ID.plume_gold.dim_patient` p
LEFT JOIN pivoted_labs l ON p.patient_token = l.patient_token;


-- ─────────────────────────────────────────────────────────────────────────────
-- View 2: vw_lab_trends
-- ─────────────────────────────────────────────────────────────────────────────
-- Business Purpose:
--   Longitudinal lab trends data source for time-series charts in Tableau.
--   Provides quarterly aggregated hormone levels across the patient population,
--   enabling the clinical team to track whether HRT effectiveness is improving
--   or declining over time at a population level.
--
-- Key Metrics Enabled:
--   - Average estradiol/testosterone by quarter and HRT category (line chart)
--   - Therapeutic target achievement rate trend over time (line chart)
--   - Population-level hormone distribution by quarter (box plot)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.plume_bi.vw_lab_trends` AS
SELECT
    -- Time dimensions
    DATE_TRUNC(l.lab_date, QUARTER)         AS lab_quarter_start,
    EXTRACT(YEAR  FROM l.lab_date)          AS lab_year,
    EXTRACT(QUARTER FROM l.lab_date)        AS lab_quarter_num,

    -- Segmentation dimensions
    l.test_name,
    l.unit,
    l.hrt_category,
    l.state,

    -- Aggregated metrics
    COUNT(*)                                AS lab_count,
    COUNT(DISTINCT l.patient_token)         AS unique_patients,
    AVG(l.result_value)                     AS avg_result_value,
    MIN(l.result_value)                     AS min_result_value,
    MAX(l.result_value)                     AS max_result_value,
    STDDEV(l.result_value)                  AS stddev_result_value,

    -- Clinical outcomes
    COUNTIF(l.is_in_therapeutic_target = TRUE)  AS in_target_count,
    COUNTIF(l.flag = 'Normal')                  AS normal_count,
    COUNTIF(l.flag = 'High')                    AS high_count,
    COUNTIF(l.flag = 'Low')                     AS low_count,

    -- Derived rate (for Tableau calculated fields or direct use)
    SAFE_DIVIDE(
        COUNTIF(l.is_in_therapeutic_target = TRUE),
        COUNT(*)
    )                                           AS therapeutic_target_rate,

    AVG(l.deviation_from_target_midpoint)       AS avg_deviation_from_target

FROM `YOUR_PROJECT_ID.plume_gold.fact_lab_results` l
GROUP BY 1, 2, 3, 4, 5, 6, 7;


-- ─────────────────────────────────────────────────────────────────────────────
-- View 3: vw_subscription_health
-- ─────────────────────────────────────────────────────────────────────────────
-- Business Purpose:
--   Finance and growth data source for subscription health dashboards.
--   Provides MRR, churn, and cohort retention metrics by state and HRT
--   category. Intentionally excludes all lab/clinical data — this view
--   is safe to share with finance and growth stakeholders who should not
--   have access to clinical information.
--
-- Key Metrics Enabled:
--   - MRR by state (choropleth map)
--   - Churn rate by plan type and tenure cohort (heatmap)
--   - New patient starts by month (bar chart)
--   - ARR projection by HRT category (bar chart)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW `YOUR_PROJECT_ID.plume_bi.vw_subscription_health` AS
SELECT
    -- Segmentation dimensions
    state,
    gender_identity,
    hrt_category,
    plan_type,
    subscription_status,

    -- Cohort dimensions
    DATE_TRUNC(start_date, MONTH)           AS cohort_month,
    CASE
        WHEN tenure_months < 3  THEN '0-3 months'
        WHEN tenure_months < 6  THEN '3-6 months'
        WHEN tenure_months < 12 THEN '6-12 months'
        WHEN tenure_months < 24 THEN '1-2 years'
        ELSE '2+ years'
    END                                     AS tenure_cohort,

    -- Aggregated financial metrics
    COUNT(DISTINCT patient_token)           AS patient_count,
    COUNTIF(subscription_status = 'active') AS active_count,
    COUNTIF(subscription_status = 'cancelled') AS churned_count,
    SUM(mrr)                                AS total_mrr,
    SUM(mrr) * 12                           AS arr_projection,
    AVG(mrr)                                AS avg_mrr_per_patient,
    AVG(tenure_months)                      AS avg_tenure_months,

    -- Derived rates
    SAFE_DIVIDE(
        COUNTIF(subscription_status = 'cancelled'),
        COUNT(DISTINCT patient_token)
    )                                       AS churn_rate

FROM `YOUR_PROJECT_ID.plume_gold.dim_patient`
GROUP BY 1, 2, 3, 4, 5, 6, 7;
