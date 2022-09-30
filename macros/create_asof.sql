{% macro create_asof(source_relation,composite_keys,ts_key,ts_value) %}
    {% set create_asof_stmt %}
        select * from {{source_relation}}
        where {{ts_key}} <= '{{ts_value}}'
        qualify row_number() over (partition by 
        {%- for composite_key in composite_keys %}
        {{composite_key}} {% if not loop.last %},{% endif %}
        {%- endfor %}
        order by {{ts_key}} desc) = 1
    {% endset %}
    {% do return(create_asof_stmt) %}
{% endmacro %}