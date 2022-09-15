{% macro create_unionv1(table_name) %}
    {% set output %}
    {% set whs = ['atl_','chi_','dvn_'] %}
    {% set sources = [] %}
    {% for wh in whs %}
        {% set refname = wh~table_name %}
        {% do sources.append(ref(refname)) %}
    {% endfor %}
    {{ dbt_utils.union_relations(sources) }}
    {% endset %}
    {% do return(output) %}
{% endmacro %}






