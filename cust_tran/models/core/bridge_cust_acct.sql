{{
  config(
    database='iceberg_dev', 
    schema='il_silver',
    materialized='scd2_trino',
    unique_key=['cust_id', 'acct_id'],
    check_cols=['cust_id', 'acct_id'],
    tags=['daily_ingestion']
  )
}}

select 
    cust_id,
    acct_id,
    record_date
from {{ source('raw_layer', 'cust_acct') }}

{% if var('initial_load', false) == false %}
    WHERE record_date = cast('{{ var("business_date") }}' as date)
{% endif %}