{% macro generate_taskv2(source_relation, dest_relation) %}
{%- set columns = adapter.get_columns_in_relation(source_relation) -%}

{% set column_names = columns | map(attribute='name') %}
{% set output %}
{% set stream_name = source_relation.database~'.'~source_relation.schema~'.'~source_relation.identifier~'_STREAM' %}
CREATE OR REPLACE STREAM {{stream_name}} 
ON TABLE {{ source(source_relation.schema, source_relation.identifier) }}
create or replace task {{source_relation.schema}}_{{source_relation.identifier}}_TASK
warehouse=PRD_ETL_S_CS_WH  schedule ='1 minute' when
SYSTEM$STREAM_HAS_DATA('{{stream_name}}') as
INSERT INTO {{dest_relation}}
select 'ATL',
{%- for column_name in column_names %}
{{column_name}}
{% if not loop.last %},{% endif %}
{%- endfor %}
, current_timestamp as MODIFICATION_TIMESTAMP
from {{stream_name}} WHERE METADATA$ACTION='INSERT'
{% endset %}
{% do return(output) %}
{% endmacro %}