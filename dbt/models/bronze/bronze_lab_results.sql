-- bronze_lab_results: Raw lab results from Quest/Labcorp integrations.
SELECT
    lab_id,
    patient_id,
    date,
    test_name,
    result_value,
    unit,
    flag,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ source('plume_bronze', 'lab_results') }}
