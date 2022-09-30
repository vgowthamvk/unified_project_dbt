{% macro create_union(warehouses,table_name) %}
    {% set output %}
    {% set sources = [] %}
    {% for wh in warehouses %}
        {% set refname = wh~table_name %}
        {% do sources.append(ref(refname)) %}
    {% endfor %}
    {{ dbt_utils.union_relations(sources,source_column_name="DBT_SOURCE_RELATION") }}
    {% endset %}
    {% do return(output) %}
{% endmacro %}






