-- dim_state_policy: State-level gender-affirming care policy dimension (SCD Type 2).
--
-- Tracks the history of state-level restrictions on gender-affirming care.
-- This is a manually seeded/maintained table that would be updated by the
-- update_guidelines Airflow DAG in production (via a legislative tracking API).
--
-- SCD Type 2: Each row represents a policy state for a given date range.
-- The current record has dbt_valid_to = NULL.
--
-- In production, this would be populated from a legislative tracking API
-- (e.g., KFF State Health Facts, Movement Advancement Project).

WITH policy_seed AS (
    -- Seed data representing known state policy statuses as of 2024
    -- In production, this comes from a legislative tracking API
    SELECT * FROM {{ ref('seed_state_policy') }}
)

SELECT
    state,
    policy_status,
    restriction_type,
    effective_date,
    expiry_date,
    notes,
    -- SCD2 fields
    effective_date                                      AS dbt_valid_from,
    COALESCE(expiry_date, DATE('9999-12-31'))           AS dbt_valid_to,
    expiry_date IS NULL                                 AS is_current,
    CURRENT_TIMESTAMP()                                 AS _updated_at
FROM policy_seed
