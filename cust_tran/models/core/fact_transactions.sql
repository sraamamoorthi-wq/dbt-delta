{{
  config(
    database='iceberg_dev', 
    schema='il_silver',
    materialized='scd2_trino',
    unique_key='tran_id',
    check_cols=['acct_id', 'tran_type', 'amount', 'tran_date'],
    tags=['daily_ingestion']
  )
}}

select 
    tran_id,
    acct_id,
    tran_type,
    amount,
    tran_date,
    record_date
from {{ source('raw_layer', 'transactions') }}

{% if var('initial_load', false) == false %}
    WHERE record_date = cast('{{ var("business_date") }}' as date)
{% endif %}