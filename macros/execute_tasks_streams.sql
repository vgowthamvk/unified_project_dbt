{% macro execute_tasks_and_streams(source_name, source_table_name, dest_relation) %} 
{%- set source_relation = source(source_name, source_table_name) -%} {%- set columns = adapter.get_columns_in_relation(source_relation) -%} {% set column_names = columns | map(attribute='name') %} {% set dtypes = columns | map(attribute='dtype') %} {% set stream_name = this.database~'.'~this.schema~'.'~source_name~'_'~source_relation.identifier~'_STREAM' %} 
        {% set create_stream_stmt %}
            CREATE OR REPLACE STREAM {{stream_name}} 
            ON TABLE {{ source(source_name, source_table_name) }};
        {% endset %}
        {% set create_stream_results = run_query(create_stream_stmt) %}

        {% set create_table_stmt %}
        {% set table_name = dest_relation.database~'.'~dest_relation.schema~'.HISTORY_'~source_relation.identifier %}
            CREATE OR REPLACE TABLE {{table_name}} (
            WAREHOUSE VARCHAR(3),
            {%- for column in columns %}
                {% if  column.is_string() == True or column.dtype == 'TIMESTAMP_NTZ' or column.dtype == 'BINARY' or column.dtype == 'TIMESTAMP_TZ' %} {{column.column}} {{column.dtype}}({{column.char_size}}), {% elif column.dtype == 'BOOLEAN' or column.dtype == 'FLOAT' %} {{column.column}} {{column.dtype}}, {% elif  column.is_number() == True %} {{column.column}} {{column.dtype}}{{column.numeric_precision,column.numeric_scale}}, {% endif %}
            {%- endfor %}
            MODIFICATION_TIMESTAMP TIMESTAMP_NTZ(9)
            );
        {% endset %}
        {% set create_table_results = run_query(create_table_stmt) %}

        {% set insert_table_stmt %}
        {% set table_name = dest_relation.database~'.'~dest_relation.schema~'.HISTORY_'~source_relation.identifier %}
            INSERT INTO {{table_name}}
            SELECT '{{source_name}}',*,current_timestamp as MODIFICATION_TIMESTAMP FROM {{ source(source_name, source_table_name) }};
        {% endset %}
        {% set insert_table_results = run_query(insert_table_stmt) %}

        {% set create_task_stmt %}
        {% set task_name = dest_relation.database~'.'~dest_relation.schema~'.'~source_name~'_'~source_table_name~'_TASK' %}
        {% set table_name = dest_relation.database~'.'~dest_relation.schema~'.HISTORY_'~source_relation.identifier %}
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
        {% endset %}
        {% set create_task_results = run_query(create_task_stmt) %}

        {% set alter_task_stmt %}
        {% set task_name = dest_relation.database~'.'~dest_relation.schema~'.'~source_name~'_'~source_table_name~'_TASK' %}
            alter task {{task_name}} resume;
        {% endset %}
        {% set alter_task_results = run_query(alter_task_stmt) %}
{% endmacro %}
