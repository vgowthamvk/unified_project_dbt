{{
    config(
        materialized="table"
    )
}}
{{create_update_snapshot(source('ATL_HISTORY','HISTORY_CONTAINERS'), ref('atl_containers_snapshot'), 'MODIFICATION_TIMESTAMP','2022-09-28 03:55:17.122',['CONTAINERID'])}}
