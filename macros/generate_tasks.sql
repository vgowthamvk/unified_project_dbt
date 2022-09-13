{% macro generate_task(source_name, source_table_name, dest_relation) %}
{%- set source_relation = source(source_name, source_table_name) -%}
{%- set columns = adapter.get_columns_in_relation(source_relation) -%}

{% set column_names = columns | map(attribute='name') %}
{% set output %}
{{columns}}

{% set stream_name = source_relation.database~'.'~source_relation.schema~'.'~source_relation.identifier~'_STREAM' %}
CREATE OR REPLACE STREAM {{stream_name}} 
ON TABLE {{ source(source_name, source_table_name) }};

{% set task_name = source_name~'_'~source_table_name~'_TASK' %}
create or replace task {{task_name}}
warehouse=PRD_ETL_S_CS_WH  schedule ='1 minute' when
SYSTEM$STREAM_HAS_DATA('{{stream_name}}') as
INSERT INTO {{dest_relation}}
select 'ATL',
{%- for column_name in column_names %}
{{column_name}}
{% if not loop.last %},{% endif %}
{%- endfor %}
, current_timestamp as MODIFICATION_TIMESTAMP
from {{stream_name}} WHERE METADATA$ACTION='INSERT';

alter task {{task_name}} resume;
{% endset %}
{% do return(output) %}
{% endmacro %}



