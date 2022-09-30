{% set whs = ['ATL','CHI','DVN','DV2','DV3','DAL','HAZ','JAX','LA'] %}
{% set table_names = ['BINTYPES','CONTAINERS','CUSTOWNERS','ITEMDETAIL','ITEMMASTER','ITEMHISTORY','PODETAIL','POHEADER','SHIPDETAIL','SHIPMASTER','VENDMASTER'] %}
{{iterate_wh_tables(whs,table_names)}}