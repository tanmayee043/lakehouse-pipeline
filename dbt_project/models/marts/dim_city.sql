with latest_fetch as (
    select
        city_id,
        any_value(timezone) as timezone,
        max(fetched_at_utc) as last_fetched_at_utc
    from {{ ref('int_weather_latest') }}
    group by city_id
)

select
    c.city_id,
    c.city_name,
    c.country,
    c.population,
    c.latitude,
    c.longitude,
    f.timezone,
    f.last_fetched_at_utc
from {{ ref('stg_cities') }} as c
left join latest_fetch as f using (city_id)
