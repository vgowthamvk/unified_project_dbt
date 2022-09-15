{% macro create_union(table_name) %}
    {% set whs = ['ATL','CHI','DAL','DVN'] %}

    {% for wh in whs %}
    select '{{wh}}' as WAREHOUSE, * from {{source(wh,table_name)}}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endmacro %}
