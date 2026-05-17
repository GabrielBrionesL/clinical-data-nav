-- silver_clinical_visits: Cleaned Healthie visit records with derived engagement metrics.

WITH source AS (
    SELECT * FROM {{ ref('bronze_clinical_visits') }}
),

cleaned AS (
    SELECT
        CAST(visit_id AS STRING)            AS visit_id,
        CAST(patient_id AS STRING)          AS patient_id_raw,
        CAST(provider_id AS STRING)         AS provider_id,
        CAST(date AS DATE)                  AS visit_date,
        CAST(visit_type AS STRING)          AS visit_type,
        CAST(status AS STRING)              AS status,

        -- Derived: engagement flags
        CASE WHEN status = 'Completed' THEN TRUE ELSE FALSE END  AS is_completed,
        CASE WHEN status = 'No-Show'   THEN TRUE ELSE FALSE END  AS is_no_show,
        CASE WHEN status = 'Canceled'  THEN TRUE ELSE FALSE END  AS is_canceled,

        -- Derived: visit category for aggregation
        CASE
            WHEN visit_type = 'Intake'          THEN 'Onboarding'
            WHEN visit_type LIKE '%HRT%'        THEN 'HRT Management'
            WHEN visit_type = 'Mental Health'   THEN 'Behavioural Health'
            WHEN visit_type = 'Urgent'          THEN 'Urgent Care'
            ELSE 'Administrative'
        END                                 AS visit_category,

        _loaded_at

    FROM source
    WHERE visit_id IS NOT NULL
      AND patient_id IS NOT NULL
)

SELECT * FROM cleaned
