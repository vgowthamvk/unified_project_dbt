{% macro incremental_snapshot(history_relation, old_snap_relation, ts_key,ts_value,composite_keys) %}
with old_snapshot_ts as(
  select max(VALID_FROM) as snapshot_ts from {{old_snap_relation}}
),
incremental_history_data as(
  select * from {{history_relation}}
  where {{ts_key}} between (select snapshot_ts from old_snapshot_ts) and {{ts_value}}
),
closed_snap_values as(
  select * from {{old_snap_relation}}
    where valid_to is not null
),
asof_oldts_snap_values as(
  select * from {{old_snap_relation}}
    where valid_to is null
),
asof_oldts_values as(
{%- set columns = adapter.get_columns_in_relation(history_relation) -%} {% set column_names = columns | map(attribute='name') %}
  select
 {%- for column_name in column_names %}
      {{column_name}} {% if not loop.last %},{% endif %}
  {%- endfor %} 
  from asof_oldts_snap_values
),
ts_range_history as(
  select * from asof_oldts_values
  union
  select * from incremental_history_data
),
 new_snapshot as (
  {{create_snapshot('ts_range_history',composite_keys,ts_key,ts_value)}}
),
current_snapshot as (
  select * from closed_snap_values
  union
  select * from new_snapshot 
)
select * from current_snapshot
{% endmacro %}

