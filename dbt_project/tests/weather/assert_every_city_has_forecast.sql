{{ config(meta={'dagster': {'ref': {'name': 'dim_city'}}}) }}

-- Fails if a city in cities.csv has no rows in the hourly fact,
-- e.g. it failed every ingestion run or its name no longer matches its file.
select c.city_id
from {{ ref('dim_city') }} as c
left join {{ ref('fct_weather_hourly') }} as h using (city_id)
where h.city_id is null
