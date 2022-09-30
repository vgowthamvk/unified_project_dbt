{% macro create_snapshot(history_relation,composite_keys,ts_key,ts_value) %}
    {% set create_snapshot_stmt %}
        select *,
        {{ts_key}} as VALID_FROM,
        lag(VALID_FROM) over (partition by 
        {%- for composite_key in composite_keys %}
        {{composite_key}} {% if not loop.last %},{% endif %}
        {%- endfor %} order by VALID_FROM desc) as VALID_TO
        from {{history_relation}}
        where {{ts_key}} <= '{{ts_value}}'
    {% endset %}
    {% do return(create_snapshot_stmt) %}
{% endmacro %}

