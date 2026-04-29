{% macro mask_pii_info(column_name, pii_type='generic') %}
    {# 
       Macro for anonymizing sensitive data (PII). Supported types: email, name, generic. In 'dev' environments, the data is masked. 
    #}

    {%- if target.name == 'prod' -%}
        {# In production, it keeps the original data (or applies a specific access rule) #}
        {{ column_name }}
    {%- else -%}
        {# In test/dev environments, apply the mask #}
        CASE 
            WHEN {{ column_name }} IS NULL THEN NULL
            
            {% if pii_type == 'email' %}
                {# Transform eder@exemplo.com into e*****@exemplo.com #}
                WHEN {{ column_name }} LIKE '%@%' THEN 
                    CONCAT(
                        SUBSTR({{ column_name }}, 1, 1),
                        '*****',
                        REGEXP_EXTRACT({{ column_name }}, r'(@.*)')
                    )
            
            {% elif pii_type == 'name' %}
                {# Transform 'Eder Junior' into 'E****' #}
                ELSE CONCAT(SUBSTR({{ column_name }}, 1, 1), '*****')
            
            {% else %}
                {# Full masking for documents and generic fields#}
                ELSE '*****'
            {% endif %}
        END
    {%- endif -%}
{% endmacro %}