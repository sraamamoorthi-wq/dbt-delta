{{ config(
     materialized='incremental',
    unique_key='transaction_id'
) }}

WITH raw_source AS (
    SELECT json_payload 
    FROM {{ source('raw_layer', 'transactions_json') }}
)

SELECT
    -- 1. Extract IDs (Using your specific keys 'tran_id', 'acct_id')
    trim(json_extract_scalar(json_payload, '$.tran_id'))   AS transaction_id,
    trim(json_extract_scalar(json_payload, '$.acct_id'))   AS account_id,
    trim(json_extract_scalar(json_payload, '$.tran_type')) AS transaction_type,

    -- 2. Extract Amount (Key is 'amount')
    TRY_CAST(
        json_extract_scalar(json_payload, '$.amount') AS DECIMAL(18, 2)
    ) AS amount,

    -- 3. Extract Date (Key is 'tran_date')
    TRY_CAST(
        json_extract_scalar(json_payload, '$.tran_date') AS DATE
    ) AS transaction_date

FROM raw_source
WHERE json_payload IS NOT NULL 
  AND length(json_payload) > 2