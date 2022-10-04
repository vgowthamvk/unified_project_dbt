{% macro create_update_snapshot(history_relation, ts_key,ts_value,composite_keys) %}
 {%- set target_table = model.get('alias', model.get('name')) -%}

 {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

{% if not target_relation_exists %}
    {{create_snapshot(history_relation,composite_keys,ts_key,ts_value)}}
{% else %}
    {{incremental_snapshot(history_relation, target_relation, ts_key,ts_value,composite_keys)}}
{% endif %}

{% endmacro %}