-- fact_lab_results: Lab result fact table joined to the patient dimension.
--
-- This is the primary analytical table for clinical outcomes monitoring.
-- Includes therapeutic target range assessment for HRT monitoring.

WITH labs AS (
    SELECT * FROM {{ ref('silver_lab_results') }}
),

patients AS (
    SELECT patient_id_raw, patient_token, hrt_category, state
    FROM {{ ref('dim_patient') }}
),

joined AS (
    SELECT
        l.lab_id,
        p.patient_token,
        l.patient_id_raw,
        p.hrt_category,
        p.state,
        l.lab_date,
        l.test_name,
        l.result_value,
        l.unit,
        l.flag,
        l.ref_range_low,
        l.ref_range_high,
        l.therapeutic_target_low,
        l.therapeutic_target_high,

        -- Is the result within the therapeutic target (more specific than clinical range)?
        CASE
            WHEN l.therapeutic_target_low IS NOT NULL
             AND l.result_value BETWEEN l.therapeutic_target_low AND l.therapeutic_target_high
            THEN TRUE
            ELSE FALSE
        END                                         AS is_in_therapeutic_target,

        -- Deviation from midpoint of therapeutic target (for trend analysis)
        CASE
            WHEN l.therapeutic_target_low IS NOT NULL
            THEN l.result_value - ((l.therapeutic_target_low + l.therapeutic_target_high) / 2)
            ELSE NULL
        END                                         AS deviation_from_target_midpoint,

        EXTRACT(YEAR  FROM l.lab_date)              AS lab_year,
        EXTRACT(MONTH FROM l.lab_date)              AS lab_month,
        EXTRACT(QUARTER FROM l.lab_date)            AS lab_quarter,

        l._loaded_at

    FROM labs l
    LEFT JOIN patients p
        ON l.patient_id_raw = p.patient_id_raw
)

SELECT * FROM joined
