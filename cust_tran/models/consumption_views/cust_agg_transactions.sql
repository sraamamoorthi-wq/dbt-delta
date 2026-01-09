{{
    config(
        materialized='view',
        database='iceberg_dev',
        schema='consumption_views'
    )
}}

SELECT
    -- 1. Partition/Grain
    c.snp_dt_mth_prtn,
    c.cust_id,
    c.cust_name,
    c.cust_no,
    
    -- 2. Account Metrics
    COUNT(DISTINCT a.acct_id) AS total_active_accounts,
    
    -- 3. DEBIT Metrics
    COALESCE(SUM(CASE WHEN t.tran_type = 'Debit' THEN t.total_trans_count ELSE 0 END), 0) AS debit_trans_count,
    COALESCE(SUM(CASE WHEN t.tran_type = 'Debit' THEN t.total_amount ELSE 0 END), 0)      AS debit_amount,

    -- 4. CREDIT Metrics
    COALESCE(SUM(CASE WHEN t.tran_type = 'Credit' THEN t.total_trans_count ELSE 0 END), 0) AS credit_trans_count,
    COALESCE(SUM(CASE WHEN t.tran_type = 'Credit' THEN t.total_amount ELSE 0 END), 0)      AS credit_amount

FROM {{ ref('monthly_customers') }} c

-- Link Customer to Account
INNER JOIN {{ ref('monthly_cust_acct') }} b
  ON c.cust_id = b.cust_id
 AND c.snp_dt_mth_prtn = b.snp_dt_mth_prtn

-- Link to Account Details (Filter for Active/Retail)
INNER JOIN {{ ref('monthly_accounts') }} a
  ON b.acct_id = a.acct_id
 AND b.snp_dt_mth_prtn = a.snp_dt_mth_prtn

-- Link to Transactions
LEFT JOIN {{ ref('monthly_transactions') }} t
  ON a.acct_id = t.acct_id
 AND a.snp_dt_mth_prtn = t.snp_dt_mth_prtn

WHERE 
    a.retail_acct = 'Y'
    AND a.is_actv = 'Y'

GROUP BY 
    1, 2, 3, 4