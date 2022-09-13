{% set source_dbs = ["ATL_SOURCE", "CHI_SOURCE", "DAL_SOURCE", "DVN_SOURCE"] %}
{% set source_tbls = ["BINTYPES"] %}


{% for src_db in source_dbs %}
    {% for src_tbl in source_tbls %}
create or replace task {{src_tbl}}_{{src_db}}_TASK
warehouse=PRD_ETL_S_CS_WH schedule ='5 minute' when
SYSTEM$STREAM_HAS_DATA({{db_name}}.{{schema_name}}.{{src_tbl}}_{{src_db}}_STREAM) as
INSERT INTO {{ ref('raw_bintypes') }}
SELECT 'ATL', colum_names FROM {{db_name}}.{{schema_name}}.{{src_tbl}}_{{src_db}}_STREAM WHERE METADATA$ACTION='INSERT';
    {% endfor %}
{% endfor %}


{% set source_dbs = ["ATL_SOURCE", "CHI_SOURCE", "DAL_SOURCE", "DVN_SOURCE"] %}
    {% set source_tbls = ["BINTYPES"] %}

    {% for src_db in source_dbs %}
    {% for src_tbl in source_tbls %}
    CREATE OR REPLACE STREAM {{db_name}}.{{schema_name}}.{{src_tbl}}_{{src_db}}_STREAM ON TABLE {{ source(src_db, src_tbl) }}
    {% endfor %}
    {% endfor %}