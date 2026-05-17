-- silver_patients: Cleaned, typed, and PHI-tokenised patient records.
--
-- Key transformations:
--   1. Cast all columns to correct data types.
--   2. Hash the patient_id using the hash_phi macro (HIPAA-compliant tokenisation).
--      The original UUID is preserved as patient_token for downstream joins.
--   3. Derive age_years and hrt_category for analytical convenience.
--   4. Exclude records with null patient_id or start_date (data quality gate).

WITH source AS (
    SELECT * FROM {{ ref('bronze_patients') }}
),

cleaned AS (
    SELECT
        -- PHI Tokenisation: hash the raw UUID to a stable, non-reversible token
        {{ hash_phi('patient_id') }}                        AS patient_token,
        patient_id                                          AS patient_id_raw,  -- kept for internal joins only

        CAST(state AS STRING)                               AS state,
        CAST(gender_identity AS STRING)                     AS gender_identity,
        CAST(hrt_regimen AS STRING)                         AS hrt_regimen,
        CAST(birth_date AS DATE)                            AS birth_date,
        CAST(start_date AS DATE)                            AS start_date,
        CAST(is_active AS BOOL)                             AS is_active,

        -- Derived fields
        DATE_DIFF(CURRENT_DATE(), CAST(birth_date AS DATE), YEAR)   AS age_years,

        CASE
            WHEN hrt_regimen LIKE '%Estradiol%'    THEN 'Feminising HRT'
            WHEN hrt_regimen LIKE '%Testosterone%' THEN 'Masculinising HRT'
            ELSE 'No HRT / Other'
        END                                                 AS hrt_category,

        _loaded_at

    FROM source
    WHERE patient_id IS NOT NULL
      AND start_date IS NOT NULL
)

SELECT * FROM cleaned
