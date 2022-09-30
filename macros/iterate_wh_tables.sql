{% macro iterate_wh_tables(whs, table_names) %}
    {%- for wh in whs %}
        {%- for table_name in table_names %}
            {%- set history_wh = wh~'_HISTORY' -%}
            {%- set history_table_name = 'HISTORY_'~table_name -%}
            {{execute_tasks_and_streams(wh,table_name,source(history_wh,history_table_name))}}
        {%- endfor %}
    {%- endfor %}
{% endmacro %}