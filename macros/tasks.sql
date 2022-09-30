{% macro tasks(source_name, source_table_name, dest_relation) %} 
        {% set create_task_stmt %}
        {%- set source_relation = source(source_name, source_table_name) -%} {%- set columns = adapter.get_columns_in_relation(source_relation) -%} {% set column_names = columns | map(attribute='name') %}
        {% set stream_name = this.database~'.'~this.schema~'.'~source_name~'_'~source_relation.identifier~'_STREAM' %} 
        {% set task_name = dest_relation.database~'.'~dest_relation.schema~'.'~source_name~'_'~source_table_name~'_TASK' %}
        {% set table_name = dest_relation.database~'.'~dest_relation.schema~'.HISTORY_'~source_relation.identifier %}
            create or replace task {{task_name}}
            warehouse=SANDBOX_DB_S_CS_WH schedule ='1 minute' when
            SYSTEM$STREAM_HAS_DATA('{{stream_name}}') as
            INSERT INTO {{table_name}}
            select '{{source_name}}',
            {%- for column_name in column_names %}
            {{column_name}} {% if not loop.last %},{% endif %}
            {%- endfor %}
            , current_timestamp as MODIFICATION_TIMESTAMP
            from {{stream_name}} WHERE METADATA$ACTION='INSERT';
        {% endset %}
        {% set create_task_results = run_query(create_task_stmt) %}
        

        {% set alter_task_stmt %}
        {% set task_name = dest_relation.database~'.'~dest_relation.schema~'.'~source_name~'_'~source_table_name~'_TASK' %}
            alter task {{task_name}} resume;
        {% endset %}
        {% set alter_task_results = run_query(alter_task_stmt) %}
{% endmacro %}