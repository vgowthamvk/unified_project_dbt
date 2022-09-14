{% snapshot atl_bintypes_snapshot %}

{{
    config(
      unique_key='BINTYPE',
      target_schema='GOWTHAMV',
      strategy='timestamp',
      updated_at='MODIFICATION_TIMESTAMP',
    )
}}

select * from {{ source('ATL_RAW', 'RAW_ATLSRVDB_BINTYPES') }}

{% endsnapshot %}