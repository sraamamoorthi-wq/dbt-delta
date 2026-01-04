{{
  config(
    database='iceberg_dev', 
    schema='il_silver',
    materialized='scd2_trino',
    unique_key='acct_id',
    check_cols=['acct_no', 'product', 'sub_product', 'is_actv', 'retail_acct', 'holding_type', 'acct_open_date'],
    tags=['daily_ingestion']
  )
}}

select 
    acct_id,
    acct_no,
    product,
    sub_product,
    is_actv,
    retail_acct,
    holding_type,
    acct_open_date,
    record_date
from {{ source('raw_layer', 'accounts') }}

{% if var('initial_load', false) == false %}
    WHERE record_date = cast('{{ var("business_date") }}' as date)
{% endif %}