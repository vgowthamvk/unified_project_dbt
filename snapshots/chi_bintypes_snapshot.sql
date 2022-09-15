{% snapshot chi_bintypes_snapshot %}

{{
    config(
      unique_key='BINTYPE',
      target_schema='GOWTHAMV_CHISRVDB_DBT',
      strategy='timestamp',
      updated_at='MODIFICATION_TIMESTAMP',
    )
}}

select * from {{ source('CHI_RAW', 'RAW_BINTYPES') }}

{% endsnapshot %}