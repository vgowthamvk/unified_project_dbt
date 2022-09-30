{{ config(
    materialized="table"  
) }}

{{create_asof(source('CHI_HISTORY','HISTORY_POHEADER'),['CUSTOWNER','PONO'],'MODIFICATION_TIMESTAMP','2022-09-20 03:11:46.430')}}