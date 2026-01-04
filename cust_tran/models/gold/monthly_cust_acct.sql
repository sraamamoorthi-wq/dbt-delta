{{
    config(
        database='iceberg_dev', 
        schema='consumption_gold',
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['link_id', 'snp_dt_mth_prtn'],
        incremental_predicates = [
            "snp_dt_mth_prtn = CAST('" ~ var('business_date') ~ "' AS DATE)"
        ]
    )
}}

{% set business_date = var('business_date') %}

SELECT
    -- Partition is directly the passed business date (1st of month)
    CAST('{{ business_date }}' AS DATE) as snp_dt_mth_prtn,
    
    link_id,
    cust_id,
    acct_id,
    current_timestamp as last_updated_at

FROM {{ ref('bridge_cust_acct') }}
WHERE dbt_delete_flag = 0
-- Snapshot Logic: Active as of Month End
AND dbt_valid_from <= last_day_of_month(CAST('{{ business_date }}' AS DATE))
AND (dbt_valid_to > last_day_of_month(CAST('{{ business_date }}' AS DATE)) OR dbt_valid_to IS NULL)