{% macro create_stream(db_name, schema_name) %}
    {% set source_dbs = ["ATL_SOURCE", "CHI_SOURCE", "DAL_SOURCE", "DVN_SOURCE"] %}
    {% set source_tbls = ["BINTYPES"] %}

    {% for src_db in source_dbs %}
    {% for src_tbl in source_tbls %}
    CREATE OR REPLACE STREAM {{db_name}}.{{schema_name}}.{{src_tbl}}_{{src_db}}_STREAM ON TABLE {{ source(src_db, src_tbl) }}
    {% endfor %}
    {% endfor %}
{% endmacro %}
