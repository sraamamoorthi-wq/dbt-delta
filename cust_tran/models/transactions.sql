{{ config(
     materialized='incremental',
    unique_key='transaction_id'
) }}

WITH raw_source AS (
    SELECT json_payload 
    FROM {{ source('raw_layer', 'transactions') }}
)

SELECT
    -- Rename here
    tran_id,
    acct_id ,
    tran_type,
    amount,
    
    -- Cast dates
    TRY_CAST(tran_date AS DATE) AS tran_date

FROM {{ source('raw_layer', 'transactions') }}