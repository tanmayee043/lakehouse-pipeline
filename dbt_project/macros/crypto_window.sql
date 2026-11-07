{#-
  Limits crypto models to the days being run.
  - crypto_start_date and crypto_end_date set (a Dagster partition run): only those UTC days.
    Used inside ephemeral models too, so DuckDB skips every other day's Parquet folder.
  - Not set (a plain `dbt build`): every available day.
-#}
{% macro crypto_window(date_column) -%}
    {%- if var('crypto_start_date') and var('crypto_end_date') -%}
        {{ date_column }} between date '{{ var("crypto_start_date") }}' and date '{{ var("crypto_end_date") }}'
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}
