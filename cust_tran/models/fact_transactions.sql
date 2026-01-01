{% set business_date = var("business_date") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        partitioned_by=['tran_month'],
        database='delta_dev',
        schema='silver',
        file_format='PARQUET',
        pre_hook=[
          "
          {% if is_incremental() %}
            DELETE FROM {{ this }} 
            WHERE tran_month = CAST(date_trunc('month', CAST('{{ business_date }}' AS DATE)) AS DATE)
          {% endif %}
          "
        ]
    )
}}

WITH source_data AS (
    SELECT
        CAST(tran_date AS DATE) as transaction_date,
        CAST('{{ business_date }}' AS DATE) as run_business_date,
        current_timestamp as ingested_at,
        
        -- Calculate the partition column
        CAST(date_trunc('month', CAST(tran_date AS DATE)) AS DATE) as tran_month,
        
        tran_id as transaction_id,
        acct_id as account_id,
        tran_type as transaction_type,
        amount
        
    FROM {{ source('raw_layer', 'transactions') }}
    
    -- Filter source to only pick up data for this specific month
    WHERE 
        date_trunc('month', CAST(tran_date AS DATE)) = CAST(date_trunc('month', CAST('{{ business_date }}' AS DATE)) AS DATE)
)

SELECT * FROM source_data