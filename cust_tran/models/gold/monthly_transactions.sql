{{
    config(
        database='iceberg_dev', 
        schema='consumption_gold',
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['acct_id', 'tran_type', 'snp_dt_mth_prtn'],
        incremental_predicates = [
            "snp_dt_mth_prtn = CAST('" ~ var('business_date') ~ "' AS DATE)"
        ]
    )
}}

{% set business_date = var('business_date') %}

SELECT
-- Partition is directly the passed business date (1st of month)
   CAST('{{ business_date }}' AS DATE) as snp_dt_mth_prtn,
    acct_id,
    tran_type,
    COUNT(tran_id) AS total_trans_count,
    SUM(amount) AS total_amount,
    MAX(CURRENT_TIMESTAMP) AS last_updated_at
FROM {{ ref('fact_transactions') }}
WHERE dbt_delete_flag = 0
-- Match transactions belonging to this specific month
AND date_trunc('month', tran_date) = CAST('{{ business_date }}' AS DATE)
GROUP BY 1, 2, 3