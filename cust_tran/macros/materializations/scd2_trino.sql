{% materialization scd2_trino, default %}
  
  -- 1. Configuration
  {%- set unique_key = config.require('unique_key') -%}
  {%- set check_cols = config.require('check_cols') -%}
  {%- set business_date = var('business_date', run_started_at.strftime('%Y-%m-%d')) -%}
  {%- set is_initial_load = var('initial_load', false) -%}
  
  -- Define the "High Water Mark" date variable for consistency
  {%- set future_date = "cast('9999-12-31' as date)" -%}

  {%- set target_relation = this -%}
  {%- set temp_relation = make_temp_relation(this) -%}
  
  -- 2. SAFETY DROP
  {% call statement('drop_temp_pre') -%}
    DROP TABLE IF EXISTS {{ temp_relation }}
  {%- endcall %}

  -- 3. Temp Table Creation
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  
  {% call statement('main') -%}
    CREATE TABLE {{ temp_relation }}
    WITH (format = 'PARQUET') 
    AS 
    {{ sql }}
  {%- endcall %}

  -- 4. WRAPPER
  {%- set source_sql -%}
    SELECT 
        src.*,
        {{ dbt_utils.generate_surrogate_key(check_cols) }} as row_hash
    FROM {{ temp_relation }} src
  {%- endset -%}

  -- 5. LOGIC BRANCH
  
  {% if is_initial_load %}
    
    -- PATH A: INITIAL LOAD
    {{ log("Starting Full History Build (High-Water Mark 9999-12-31): " ~ target_relation, info=True) }}

    {% call statement('initial_build') -%}
      CREATE OR REPLACE TABLE {{ target_relation }} 
      WITH (format = 'PARQUET') 
      AS
      SELECT
          src.*,
          record_date as dbt_valid_from,
          
          -- LOGIC CHANGE:
          -- If LEAD is null (latest record), default to 9999-12-31
          -- Otherwise, close the record (next_date - 1 day)
          COALESCE(
              date_add('day', -1, 
                  LEAD(record_date) OVER (
                      PARTITION BY {{ unique_key }} 
                      ORDER BY record_date ASC
                  )
              ),
              {{ future_date }}
          ) as dbt_valid_to,
          
          integer '0' as dbt_delete_flag,
          current_timestamp as dbt_updated_at
      FROM ({{ source_sql }}) src
    {%- endcall %}

  {% else %}

    -- PATH B: INCREMENTAL
    {%- set relation_exists = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) -%}

    {% if not relation_exists %}
        {% call statement('create_target_empty') -%}
          CREATE TABLE {{ target_relation }} 
          WITH (format = 'PARQUET') 
          AS
          SELECT 
            *, 
            cast('{{ business_date }}' as date) as dbt_valid_from,
            -- Default new table to High Water Mark
            {{ future_date }} as dbt_valid_to,
            integer '0' as dbt_delete_flag,
            current_timestamp as dbt_updated_at
          FROM ({{ source_sql }}) 
          WHERE 1=0 
        {%- endcall %}
    {% endif %}

    {{ log("Running Incremental Update: " ~ target_relation, info=True) }}

    {% call statement('update_existing') -%}
      MERGE INTO {{ target_relation }} t
      USING (
          SELECT t.{{ unique_key }},
              CASE WHEN s.{{ unique_key }} IS NULL THEN 1 ELSE 0 END as calc_delete_flag
          FROM {{ target_relation }} t
          LEFT JOIN ({{ source_sql }}) s ON t.{{ unique_key }} = s.{{ unique_key }}
          -- Match only ACTIVE records (where valid_to is 9999-12-31)
          WHERE t.dbt_valid_to = {{ future_date }}
            AND t.dbt_valid_from < cast('{{ business_date }}' as date)
            AND (s.{{ unique_key }} IS NULL OR t.row_hash != s.row_hash)
      ) updates
      -- Merge condition uses = 9999-12-31 instead of IS NULL
      ON (t.{{ unique_key }} = updates.{{ unique_key }} AND t.dbt_valid_to = {{ future_date }})
      WHEN MATCHED THEN UPDATE SET 
          dbt_valid_to = date_add('day', -1, cast('{{ business_date }}' as date)),
          dbt_delete_flag = updates.calc_delete_flag,
          dbt_updated_at = current_timestamp
    {%- endcall %}

    {% call statement('insert_new') -%}
      INSERT INTO {{ target_relation }}
      SELECT 
        s.*,
        cast('{{ business_date }}' as date) as dbt_valid_from,
        -- New records get High Water Mark
        {{ future_date }} as dbt_valid_to,
        integer '0' as dbt_delete_flag,
        current_timestamp as dbt_updated_at
      FROM ({{ source_sql }}) s
      LEFT JOIN {{ target_relation }} t 
        ON s.{{ unique_key }} = t.{{ unique_key }} 
        AND t.dbt_valid_to = {{ future_date }} -- Join on Active
      WHERE (t.{{ unique_key }} IS NULL OR s.row_hash != t.row_hash)
        AND NOT EXISTS (
            SELECT 1 FROM {{ target_relation }} e 
            WHERE e.{{ unique_key }} = s.{{ unique_key }} 
            AND e.dbt_valid_from = cast('{{ business_date }}' as date)
        )
    {%- endcall %}

  {% endif %}

  -- Cleanup
  {% call statement('drop_temp_post') -%}
    DROP TABLE IF EXISTS {{ temp_relation }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}