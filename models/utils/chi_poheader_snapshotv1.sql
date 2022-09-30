{
{{
    config(materialized = 'table')
}}

{{create_snapshot(source('CHI_HISTORY','HISTORY_POHEADER'),['CUSTOWNER','PONO'],'MODIFICATION_TIMESTAMP')}}
#}