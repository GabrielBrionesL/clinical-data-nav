-- silver_lab_results: Cleaned lab results with clinical reference ranges added.
--
-- Reference ranges are sourced from WPATH Standards of Care v8 and
-- standard clinical laboratory guidelines for gender-affirming HRT monitoring.

WITH source AS (
    SELECT * FROM {{ ref('bronze_lab_results') }}
),

with_ranges AS (
    SELECT
        CAST(lab_id AS STRING)              AS lab_id,
        CAST(patient_id AS STRING)          AS patient_id_raw,
        CAST(date AS DATE)                  AS lab_date,
        CAST(test_name AS STRING)           AS test_name,
        CAST(result_value AS FLOAT64)       AS result_value,
        CAST(unit AS STRING)                AS unit,
        CAST(flag AS STRING)                AS flag,

        -- Clinical reference ranges for HRT monitoring
        CASE test_name
            WHEN 'Estradiol'    THEN 50.0
            WHEN 'Testosterone' THEN 30.0
            WHEN 'Hemoglobin'   THEN 11.5
            WHEN 'Potassium'    THEN 3.5
        END                                 AS ref_range_low,

        CASE test_name
            WHEN 'Estradiol'    THEN 300.0
            WHEN 'Testosterone' THEN 900.0
            WHEN 'Hemoglobin'   THEN 17.5
            WHEN 'Potassium'    THEN 5.5
        END                                 AS ref_range_high,

        -- Therapeutic target ranges for feminising HRT (estradiol)
        CASE test_name
            WHEN 'Estradiol'    THEN 100.0
            WHEN 'Testosterone' THEN 0.0
            ELSE NULL
        END                                 AS therapeutic_target_low,

        CASE test_name
            WHEN 'Estradiol'    THEN 200.0
            WHEN 'Testosterone' THEN 55.0
            ELSE NULL
        END                                 AS therapeutic_target_high,

        _loaded_at

    FROM source
    WHERE lab_id IS NOT NULL
      AND patient_id IS NOT NULL
      AND result_value IS NOT NULL
      AND result_value >= 0
)

SELECT * FROM with_ranges
