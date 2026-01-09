{{
  config(
    database='iceberg_dev', 
    schema='il_silver',
    materialized='scd2_trino',
    unique_key='cust_id',
    check_cols=['cust_no', 'cust_name', 'cust_join_date'],
    tags=['daily_ingestion']
  )
}}

select 
    cust_id,
    cust_no,
    cust_name,
    cust_join_date,
    record_date
from {{ source('raw_layer', 'customers') }}

{% if var('initial_load', false) == false %}
    WHERE record_date = cast('{{ var("business_date") }}' as date)
{% endif %}