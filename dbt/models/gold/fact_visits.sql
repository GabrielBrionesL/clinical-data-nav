-- fact_visits: Clinical visit fact table joined to the patient dimension.
--
-- Used for provider utilisation, no-show rate analysis, and
-- patient engagement scoring.

WITH visits AS (
    SELECT * FROM {{ ref('silver_clinical_visits') }}
),

patients AS (
    SELECT patient_id_raw, patient_token, hrt_category, state, age_band
    FROM {{ ref('dim_patient') }}
),

joined AS (
    SELECT
        v.visit_id,
        p.patient_token,
        v.patient_id_raw,
        v.provider_id,
        p.hrt_category,
        p.state,
        p.age_band,
        v.visit_date,
        v.visit_type,
        v.visit_category,
        v.status,
        v.is_completed,
        v.is_no_show,
        v.is_canceled,

        EXTRACT(YEAR    FROM v.visit_date)          AS visit_year,
        EXTRACT(MONTH   FROM v.visit_date)          AS visit_month,
        EXTRACT(QUARTER FROM v.visit_date)          AS visit_quarter,

        v._loaded_at

    FROM visits v
    LEFT JOIN patients p
        ON v.patient_id_raw = p.patient_id_raw
)

SELECT * FROM joined
