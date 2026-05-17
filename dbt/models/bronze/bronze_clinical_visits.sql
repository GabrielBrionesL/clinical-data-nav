-- bronze_clinical_visits: Raw Healthie appointment records pass-through.
SELECT
    visit_id,
    patient_id,
    provider_id,
    date,
    visit_type,
    status,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ source('plume_bronze', 'clinical_visits') }}
