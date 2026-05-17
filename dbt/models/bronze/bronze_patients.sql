-- bronze_patients: Raw pass-through from the BigQuery Bronze ingestion layer.
-- No transformations are applied here; this model exists to enforce schema
-- contracts via dbt tests and to provide a stable reference for Silver models.

SELECT
    patient_id,
    state,
    gender_identity,
    hrt_regimen,
    birth_date,
    start_date,
    is_active,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ source('plume_bronze', 'patients') }}
