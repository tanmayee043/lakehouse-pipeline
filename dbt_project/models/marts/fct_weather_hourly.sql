select
    city_id,
    forecast_hour_utc,
    forecast_hour_local,
    cast(forecast_hour_local as date) as forecast_date_local,
    temperature_c,
    precipitation_mm,
    wind_speed_kmh,
    fetched_at_utc
from {{ ref('int_weather_latest') }}
