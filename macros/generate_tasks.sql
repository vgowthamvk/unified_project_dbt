{% macro generate_tasks(source_name, source_table_name, dest_relation) %}
    {% set task_name = dest_relation.database~'.'~dest_relation.schema~'.'~source_name~'_'~source_table_name~'_TASK' %}
            create or replace task {{task_name}}
            warehouse=PRD_ETL_S_CS_WH  schedule ='1 minute' when
            SYSTEM$STREAM_HAS_DATA('{{stream_name}}') as
            INSERT INTO {{table_name}}
            select '{{source_name}}',
            {%- for column_name in column_names %}
            {{column_name}} {% if not loop.last %},{% endif %}
            {%- endfor %}
            , current_timestamp as MODIFICATION_TIMESTAMP
            from {{stream_name}} WHERE METADATA$ACTION='INSERT';
            alter task {{task_name}} resume;
{% endmacro %}