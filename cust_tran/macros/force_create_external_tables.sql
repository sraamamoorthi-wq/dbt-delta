{% macro force_create_external_tables(source_to_process=None) %}

    {# Loop through every source defined in your project #}
    {% for node in graph.sources.values() -%}
        
        {# Check 1: Does it have external config? #}
        {# Check 2: If we passed a specific source name, does it match? #}
        {% if node.external and (source_to_process is none or node.source_name == source_to_process) %}
            
            {% set table_full_name = node.database ~ '.' ~ node.schema ~ '.' ~ node.identifier %}
            {{ log("--- Processing: " ~ table_full_name ~ " ---", info=True) }}

            {# 1. Drop the table if it exists (to refresh schema) #}
            {% set drop_query = 'DROP TABLE IF EXISTS ' ~ table_full_name %}
            {% do run_query(drop_query) %}

            {# 2. Construct the CREATE TABLE statement #}
            {% set create_query %}
                CREATE TABLE {{ table_full_name }} (
                    {% for col in node.columns.values() %}
                        {{ col.name }} {{ col.data_type }}{% if not loop.last %},{% endif %}
                    {% endfor %}
                )
                WITH (
                    external_location = '{{ node.external.location }}',
                    format = '{{ node.external.file_format }}'
                )
            {% endset %}

            {# 3. Execute the Query #}
            {% do run_query(create_query) %}
            {{ log("Success: Created " ~ table_full_name, info=True) }}
            
        {% endif %}
    {%- endfor %}

{% endmacro %}