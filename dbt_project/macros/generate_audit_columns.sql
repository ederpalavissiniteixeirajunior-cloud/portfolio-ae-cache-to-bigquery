{% macro generate_audit_columns() %}
    {# 
       Macro for lineage traceability and auditing.
        - _processed_at: Exact timestamp of processing (UTC).
        - _dbt_model_name: Name of the dbt model that generated the data.
        - _dbt_invocation_id: Unique execution ID (allows linking with the GitHub Actions log).
    #}

    CURRENT_TIMESTAMP() as _processed_at,
    '{{ model.name }}' as _dbt_model_name,
    '{{ invocation_id }}' as _dbt_invocation_id

{% endmacro %}