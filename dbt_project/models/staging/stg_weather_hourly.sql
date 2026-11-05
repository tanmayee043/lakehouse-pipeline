-- One row per city, per run, per forecast hour.
-- Parallel unnests in the same SELECT zip the hourly arrays together.
with unnested as (
    select
        city,
        cast(fetched_on as date) as fetched_on,
        cast(fetched_at as timestamptz) at time zone 'UTC' as fetched_at_utc,
        weather.timezone as timezone,
        unnest(weather.hourly.time) as local_time_text,
        unnest(weather.hourly.temperature_2m) as temperature_c,
        unnest(weather.hourly.precipitation) as precipitation_mm,
        unnest(weather.hourly.wind_speed_10m) as wind_speed_kmh,
        filename as source_file
    from {{ source('raw', 'weather_forecasts') }}
)

select
    {{ city_id('city') }} as city_id,
    fetched_on,
    fetched_at_utc,
    timezone,
    cast(local_time_text as timestamp) as forecast_hour_local,
    -- Interpret the wall-clock time in the city's zone, then express it in UTC (DST-aware).
    timezone(timezone, cast(local_time_text as timestamp)) at time zone 'UTC' as forecast_hour_utc,
    cast(temperature_c as double) as temperature_c,
    cast(precipitation_mm as double) as precipitation_mm,
    cast(wind_speed_kmh as double) as wind_speed_kmh,
    source_file
from unnested
