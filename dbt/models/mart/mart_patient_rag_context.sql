-- mart_patient_rag_context: The architectural bridge between the structured
-- data warehouse and the LangChain RAG pipeline.
--
-- This model aggregates each patient's full clinical state into a single,
-- anonymised text column. The LangChain agent reads this text as context
-- alongside retrieved WPATH guideline chunks, enabling the LLM to answer
-- clinical questions without requiring complex Text-to-SQL logic.
--
-- HIPAA NOTE: This model uses patient_token (hashed UUID) as the identifier.
-- No names, DOBs, or other direct identifiers are included. The Presidio
-- PII scrubber in the RAG pipeline provides an additional guardrail before
-- any text reaches the LLM API (Claude via Anthropic).
--
-- Example output for a single patient:
-- "Patient TKN-a3f2...: Trans Woman, age 28, state: CO (Protected).
--  HRT: Feminising HRT (Estradiol + Spironolactone). Active subscription
--  (Monthly, $99/mo, 14 months tenure). Last labs (2024-09-15):
--  Estradiol 162 pg/mL [IN THERAPEUTIC TARGET 100-200], Testosterone
--  38 ng/dL [IN THERAPEUTIC TARGET 0-55], Hemoglobin 13.1 g/dL [Normal],
--  Potassium 4.1 mmol/L [Normal]. Visits last 90 days: 2 completed,
--  0 no-shows."

WITH patients AS (
    SELECT * FROM {{ ref('dim_patient') }}
),

-- Get the most recent lab result per patient per test
latest_labs AS (
    SELECT
        patient_id_raw,
        test_name,
        result_value,
        unit,
        flag,
        is_in_therapeutic_target,
        therapeutic_target_low,
        therapeutic_target_high,
        lab_date,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id_raw, test_name
            ORDER BY lab_date DESC
        ) AS rn
    FROM {{ ref('fact_lab_results') }}
),

latest_labs_filtered AS (
    SELECT * FROM latest_labs WHERE rn = 1
),

-- Aggregate labs into a single text string per patient
lab_summaries AS (
    SELECT
        patient_id_raw,
        MAX(lab_date)   AS latest_lab_date,
        STRING_AGG(
            CONCAT(
                test_name, ' ', CAST(ROUND(result_value, 1) AS STRING), ' ', unit,
                ' [',
                CASE
                    WHEN is_in_therapeutic_target THEN
                        CONCAT('IN THERAPEUTIC TARGET ',
                               CAST(CAST(therapeutic_target_low AS INT64) AS STRING),
                               '-',
                               CAST(CAST(therapeutic_target_high AS INT64) AS STRING))
                    ELSE CONCAT(flag, ' — outside target')
                END,
                ']'
            ),
            ', ' ORDER BY test_name
        ) AS lab_text
    FROM latest_labs_filtered
    GROUP BY patient_id_raw
),

-- Get visit engagement stats for the last 90 days
recent_visits AS (
    SELECT
        patient_id_raw,
        COUNTIF(is_completed AND visit_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) AS completed_90d,
        COUNTIF(is_no_show   AND visit_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) AS no_shows_90d
    FROM {{ ref('fact_visits') }}
    GROUP BY patient_id_raw
),

-- Get current state policy
state_policy AS (
    SELECT state, policy_status, restriction_type
    FROM {{ ref('dim_state_policy') }}
    WHERE is_current = TRUE
),

-- Assemble the final text context
assembled AS (
    SELECT
        p.patient_token,
        p.patient_id_raw,
        p.state,
        p.gender_identity,
        p.hrt_regimen,
        p.hrt_category,
        p.age_years,
        p.age_band,
        p.subscription_status,
        p.plan_type,
        p.mrr,
        p.tenure_months,
        l.latest_lab_date,
        l.lab_text,
        v.completed_90d,
        v.no_shows_90d,
        sp.policy_status,

        -- The primary RAG context column
        CONCAT(
            'Patient ', SUBSTR(p.patient_token, 1, 8), ': ',
            p.gender_identity, ', age ', CAST(p.age_years AS STRING), ', state: ',
            p.state,
            ' (', COALESCE(sp.policy_status, 'Unknown policy'), '). ',
            'HRT: ', p.hrt_category, ' (', p.hrt_regimen, '). ',
            'Subscription: ', COALESCE(p.subscription_status, 'Unknown'), ' (',
            COALESCE(p.plan_type, 'Unknown'), ', $', CAST(CAST(p.mrr AS INT64) AS STRING), '/mo, ',
            CAST(COALESCE(p.tenure_months, 0) AS STRING), ' months tenure). ',
            CASE
                WHEN l.lab_text IS NOT NULL
                THEN CONCAT('Last labs (', CAST(l.latest_lab_date AS STRING), '): ', l.lab_text, '. ')
                ELSE 'No lab results on file. '
            END,
            'Visits last 90 days: ',
            CAST(COALESCE(v.completed_90d, 0) AS STRING), ' completed, ',
            CAST(COALESCE(v.no_shows_90d, 0) AS STRING), ' no-shows.'
        )                                       AS rag_context_text,

        CURRENT_TIMESTAMP()                     AS _updated_at

    FROM patients p
    LEFT JOIN lab_summaries l   ON p.patient_id_raw = l.patient_id_raw
    LEFT JOIN recent_visits v   ON p.patient_id_raw = v.patient_id_raw
    LEFT JOIN state_policy sp   ON p.state = sp.state
)

SELECT * FROM assembled
