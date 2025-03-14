{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ adapter.quote('STAGING_' ~ var('year', 2023) | string ~ '_Q' ~ var('quarter', 4) | string) }}
    {%- endif -%}
{%- endmacro %}