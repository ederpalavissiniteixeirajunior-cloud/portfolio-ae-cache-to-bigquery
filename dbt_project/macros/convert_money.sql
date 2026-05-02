{% macro convert_money(column_name, input_is_cents=True, precision=2) %}
    {# 
       Macro for standardization of monetary values.
        - Converts cents to units (division by 100) if necessary.
        - Ensures NUMERIC type (ideal for finance in BigQuery).
        - Applies rounding according to the desired precision.
    #}

    ROUND(
        SAFE_CAST(
            {% if input_is_cents %}
                (safe_cast({{ column_name }} as float64) / 100.0)
            {% else %}
                {{ column_name }}
            {% endif %} 
            AS NUMERIC
        ), {{ precision }}
    )
{% endmacro %}