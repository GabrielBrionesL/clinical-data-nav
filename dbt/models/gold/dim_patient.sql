-- dim_patient: Patient dimension table (Type 1 SCD).
--
-- Joins patient demographics with their current subscription status to produce
-- a single, enriched patient dimension for use in all Gold-layer fact joins.

WITH patients AS (
    SELECT * FROM {{ ref('silver_patients') }}
),

subscriptions AS (
    SELECT
        patient_id_raw,
        status           AS subscription_status,
        plan_type,
        mrr,
        arr,
        tenure_months,
        is_churned,
        start_date       AS subscription_start_date,
        end_date         AS subscription_end_date
    FROM {{ ref('silver_subscriptions') }}
),

joined AS (
    SELECT
        p.patient_token,
        p.patient_id_raw,
        p.state,
        p.gender_identity,
        p.hrt_regimen,
        p.hrt_category,
        p.birth_date,
        p.age_years,
        p.start_date         AS plume_start_date,
        p.is_active,

        -- Subscription attributes
        s.subscription_status,
        s.plan_type,
        s.mrr,
        s.arr,
        s.tenure_months,
        s.is_churned,
        s.subscription_start_date,
        s.subscription_end_date,

        -- Segment for analytics
        CASE
            WHEN p.age_years < 25                           THEN '18-24'
            WHEN p.age_years BETWEEN 25 AND 34              THEN '25-34'
            WHEN p.age_years BETWEEN 35 AND 44              THEN '35-44'
            WHEN p.age_years BETWEEN 45 AND 54              THEN '45-54'
            ELSE '55+'
        END                                                 AS age_band,

        CURRENT_TIMESTAMP()                                 AS _updated_at

    FROM patients p
    LEFT JOIN subscriptions s
        ON p.patient_id_raw = s.patient_id_raw
)

SELECT * FROM joined
