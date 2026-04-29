{% macro clean_legacy_string(column_name) %}
    CASE 
        WHEN TRIM({{ column_name }}) IN ('', 'null', 'None', 'N/A', 'undefined') THEN NULL
        ELSE TRIM({{ column_name }})
    END
{% endmacro %}