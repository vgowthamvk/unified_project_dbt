{% macro update_asof(asof_relation,history_relation,ts_key,begin_ts_value,end_ts_value) %}
    {% set update_asof_stmt %}
        insert into {{asof_relation}}
        select * from {{history_relation}}
        where {{ts_key}} > '{{begin_ts_value}}'
        and {{ts_key}} <= '{{end_ts_value}}'
    {% endset %}
    {% set update_asof_stmt_results = run_query(update_asof_stmt) %}
    {% do return(update_asof_stmt) %}
{% endmacro %}