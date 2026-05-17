-- silver_subscriptions: Cleaned Stripe subscription records with derived metrics.

WITH source AS (
    SELECT * FROM {{ ref('bronze_subscriptions') }}
),

cleaned AS (
    SELECT
        CAST(subscription_id AS STRING)     AS subscription_id,
        CAST(patient_id AS STRING)          AS patient_id_raw,
        CAST(plan_type AS STRING)           AS plan_type,
        CAST(status AS STRING)              AS status,
        CAST(mrr AS FLOAT64)                AS mrr,
        CAST(start_date AS DATE)            AS start_date,
        CAST(end_date AS DATE)              AS end_date,

        -- Derived: annualised revenue
        CAST(mrr AS FLOAT64) * 12           AS arr,

        -- Derived: tenure in months
        DATE_DIFF(
            COALESCE(CAST(end_date AS DATE), CURRENT_DATE()),
            CAST(start_date AS DATE),
            MONTH
        )                                   AS tenure_months,

        -- Derived: churn flag
        CASE WHEN status = 'Canceled' THEN TRUE ELSE FALSE END AS is_churned,

        _loaded_at

    FROM source
    WHERE subscription_id IS NOT NULL
      AND patient_id IS NOT NULL
)

SELECT * FROM cleaned
