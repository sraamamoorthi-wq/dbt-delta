{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# No custom schema provided? Use the default from profiles.yml #}
        {{ default_schema }}
    {%- else -%}
        {# Custom schema provided? Use it exactly as written #}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}