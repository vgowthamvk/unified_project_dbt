

{{ config(materialized='table') }}

SELECT 'ATL' as WAREHOUSE,*, current_timestamp as MODIFICATION_TIMESTAMP FROM {{ source('ATL_SOURCE', 'BINTYPES') }}