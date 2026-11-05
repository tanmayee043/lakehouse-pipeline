{#- Same key as slugify() in ingestion/fetch_weather.py: "Montréal" -> "montreal" -#}
{% macro city_id(column) -%}
    regexp_replace(lower(strip_accents(trim({{ column }}))), '\s+', '_', 'g')
{%- endmacro %}
