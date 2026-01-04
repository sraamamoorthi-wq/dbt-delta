{% materialization scd2_trino, default %}
  
  -- 1. Configuration
  {%- set unique_key = config.require('unique_key') -%}
  {%- set check_cols = config.require('check_cols') -%}
  {%- set business_date = var('business_date', run_started_at.strftime('%Y-%m-%d')) -%}
  {%- set is_initial_load = var('initial_load', false) -%}
  {%- set future_date = "cast('9999-12-31' as date)" -%}

  -- 2. Handle Composite Keys (Force List Format)
  {%- if unique_key is sequence and unique_key is not string -%}
      {%- set key_list = unique_key -%}
  {%- else -%}
      {%- set key_list = [unique_key] -%}
  {%- endif -%}

  {%- set target_relation = this -%}
  {%- set temp_relation = make_temp_relation(this) -%}
  
  -- 3. SAFETY DROP
  {% call statement('drop_temp_pre') -%}
    DROP TABLE IF EXISTS {{ temp_relation }}
  {%- endcall %}

  -- 4. Temp Table Creation
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  
  {% call statement('main') -%}
    CREATE TABLE {{ temp_relation }}
    WITH (format = 'PARQUET') 
    AS 
    {{ sql }}
  {%- endcall %}

  -- 5. WRAPPER (Generate Hash)
  {%- set source_sql -%}
    SELECT 
        src.*,
        {{ dbt_utils.generate_surrogate_key(check_cols) }} as row_hash
    FROM {{ temp_relation }} src
  {%- endset -%}

  -- 6. LOGIC BRANCH
  
  {% if is_initial_load %}
    
    -- PATH A: INITIAL LOAD
    {{ log("Starting Full History Build (Composite Key Supported)", info=True) }}

    {% call statement('initial_build') -%}
      CREATE OR REPLACE TABLE {{ target_relation }} 
      WITH (format = 'PARQUET') 
      AS
      SELECT
          src.*,
          record_date as dbt_valid_from,
          COALESCE(
              date_add('day', -1, 
                  LEAD(record_date) OVER (
                      -- DYNAMIC PARTITION: Supports multiple keys
                      PARTITION BY {% for key in key_list %} {{ key }} {% if not loop.last %}, {% endif %} {% endfor %}
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
            {{ future_date }} as dbt_valid_to,
            integer '0' as dbt_delete_flag,
            current_timestamp as dbt_updated_at
          FROM ({{ source_sql }}) 
          WHERE 1=0 
        {%- endcall %}
    {% endif %}

    {{ log("Running Incremental Update (Composite Key Supported)", info=True) }}

    {% call statement('update_existing') -%}
      MERGE INTO {{ target_relation }} t
      USING (
          SELECT 
              -- Select all keys dynamically
              {% for key in key_list %} t.{{ key }}, {% endfor %}
              
              -- Check if source is missing (Use first key for null check)
              CASE WHEN s.{{ key_list[0] }} IS NULL THEN 1 ELSE 0 END as calc_delete_flag
          FROM {{ target_relation }} t
          LEFT JOIN ({{ source_sql }}) s 
            -- DYNAMIC JOIN CONDITION
            ON {% for key in key_list %} t.{{ key }} = s.{{ key }} {% if not loop.last %} AND {% endif %} {% endfor %}
          
          WHERE t.dbt_valid_to = {{ future_date }}
            AND t.dbt_valid_from < cast('{{ business_date }}' as date)
            -- Change Detection: Source Missing OR Hash Changed
            AND (s.{{ key_list[0] }} IS NULL OR t.row_hash != s.row_hash)
      ) updates
      
      -- MERGE ON CONDITION
      ON (
        {% for key in key_list %} t.{{ key }} = updates.{{ key }} AND {% endfor %}
        t.dbt_valid_to = {{ future_date }}
      )
      
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
        {{ future_date }} as dbt_valid_to,
        integer '0' as dbt_delete_flag,
        current_timestamp as dbt_updated_at
      FROM ({{ source_sql }}) s
      LEFT JOIN {{ target_relation }} t 
        -- DYNAMIC JOIN
        ON {% for key in key_list %} s.{{ key }} = t.{{ key }} {% if not loop.last %} AND {% endif %} {% endfor %}
        AND t.dbt_valid_to = {{ future_date }}
      
      -- Filter: New Row OR Changed Row
      WHERE (t.{{ key_list[0] }} IS NULL OR s.row_hash != t.row_hash)
        AND NOT EXISTS (
            SELECT 1 FROM {{ target_relation }} e 
            WHERE 
            {% for key in key_list %} e.{{ key }} = s.{{ key }} AND {% endfor %}
            e.dbt_valid_from = cast('{{ business_date }}' as date)
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