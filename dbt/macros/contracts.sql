{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}

{% macro run_date() %}
  {% set value = var('run_date') %}
  {% set parsed = modules.datetime.datetime.strptime(value, '%Y-%m-%d') %}
  {% if parsed.strftime('%Y-%m-%d') != value %}
    {{ exceptions.raise_compiler_error('run_date must use YYYY-MM-DD') }}
  {% endif %}
  {{ return(value) }}
{% endmacro %}

{% macro source_window() %}
  {% set end = run_date() %}
  {% set start = var('history_start', '2026-01-01') %}
  {% set parsed_start = modules.datetime.datetime.strptime(start, '%Y-%m-%d') %}
  {% set parsed_end = modules.datetime.datetime.strptime(end, '%Y-%m-%d') %}
  {% if parsed_start.strftime('%Y-%m-%d') != start or parsed_start > parsed_end %}
    {{ exceptions.raise_compiler_error('history_start must be YYYY-MM-DD and no later than run_date') }}
  {% endif %}
  {% if is_incremental() %}
    {% set days = var('lookback_days', 3) | int %}
    {% if days < 0 or days > 365 %}
      {{ exceptions.raise_compiler_error('lookback_days must be between 0 and 365') }}
    {% endif %}
    {% set start = (parsed_end - modules.datetime.timedelta(days=days)).strftime('%Y-%m-%d') %}
  {% endif %}
  dt between cast('{{ start }}' as date) and cast('{{ end }}' as date)
{% endmacro %}

{% macro stable_key(columns) %}
  {% set parts = [] %}
  {% for column in columns %}
    {% set text = 'cast(' ~ column ~ ' as ' ~ dbt.type_string() ~ ')' %}
    {% do parts.append("case when " ~ column ~ " is null then 'N;' else concat('V', cast(length(" ~ text ~ ") as " ~ dbt.type_string() ~ "), ':', " ~ text ~ ", ';') end") %}
  {% endfor %}
  {{ dbt.hash('concat(' ~ parts | join(', ') ~ ')') }}
{% endmacro %}

{% macro incremental_config(key, date_field, clusters) %}
  {{ config(materialized='incremental', unique_key=key,
      incremental_strategy='merge' if target.type == 'bigquery' else 'delete+insert',
      on_schema_change='fail') }}
  {% if target.type == 'bigquery' %}
    {{ config(partition_by={'field': date_field, 'data_type': 'date'}, cluster_by=clusters) }}
  {% endif %}
{% endmacro %}

{% macro date_key(value) %}
  cast(extract(year from {{ value }}) * 10000 + extract(month from {{ value }}) * 100
    + extract(day from {{ value }}) as {{ dbt.type_int() }})
{% endmacro %}
