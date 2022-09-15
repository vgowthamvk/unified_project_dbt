{% snapshot dvn_bintypes_snapshot %}

{{
    config(
      unique_key='BINTYPE',
      target_schema='GOWTHAMV_DVNSRVDB_DBT',
      strategy='timestamp',
      updated_at='MODIFICATION_TIMESTAMP',
    )
}}

select * from {{ source('DVN_RAW', 'RAW_BINTYPES') }}

{% endsnapshot %}