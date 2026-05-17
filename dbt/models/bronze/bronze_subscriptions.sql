-- bronze_subscriptions: Raw Stripe subscription data pass-through.
SELECT
    subscription_id,
    patient_id,
    plan_type,
    status,
    mrr,
    start_date,
    end_date,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM {{ source('plume_bronze', 'subscriptions') }}
