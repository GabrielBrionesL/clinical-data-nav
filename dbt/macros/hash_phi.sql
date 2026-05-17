{% macro hash_phi(column_name) %}
    {#-
      hash_phi: HIPAA-compliant one-way hash for Protected Health Information.
      Uses SHA-256 via BigQuery's TO_HEX(SHA256()) to irreversibly tokenize
      identifiers (names, DOBs, MRNs) while preserving join-ability.

      Usage:
        {{ hash_phi('patient_id') }}   -- returns a 64-char hex string
    -#}
    TO_HEX(SHA256(CAST({{ column_name }} AS STRING)))
{% endmacro %}
