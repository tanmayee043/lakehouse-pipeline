from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from dagster_project.crypto.assets import crypto_project_stream, exchange_trades
from dagster_project.crypto.sensors import build_gap_sensor
from dagster_project.dbt_assets import crypto_dbt_assets, dbt_project, weather_dbt_assets
from dagster_project.weather.assets import raw_weather_forecasts

daily_weather_pipeline = define_asset_job(
    name="daily_weather_pipeline",
    # All weather assets, not just those downstream of the fetch: stg_cities reads
    # cities.csv directly, so edits to the city list must be rebuilt and tested too.
    selection=AssetSelection.assets(raw_weather_forecasts, weather_dbt_assets),
    description="Fetch today's forecasts, then rebuild and test every weather model.",
)

daily_crypto_pipeline = define_asset_job(
    name="daily_crypto_pipeline",
    selection=AssetSelection.assets(exchange_trades, crypto_project_stream, crypto_dbt_assets),
    description="For one UTC day (or a backfill range): fetch the official record, copy the Crypto project's capture, "
    "then reconcile and rebuild the crypto models.",
)

daily_weather_schedule = ScheduleDefinition(
    job=daily_weather_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    # Off until switched on in the UI, so nothing runs in the background unexpectedly.
    default_status=DefaultScheduleStatus.STOPPED,
)

# Runs early each morning for yesterday, the newest complete UTC day.
daily_crypto_schedule = build_schedule_from_partitioned_job(
    daily_crypto_pipeline,
    hour_of_day=2,
    minute_of_hour=15,
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=[raw_weather_forecasts, weather_dbt_assets, exchange_trades, crypto_project_stream, crypto_dbt_assets],
    jobs=[daily_weather_pipeline, daily_crypto_pipeline],
    schedules=[daily_weather_schedule, daily_crypto_schedule],
    sensors=[build_gap_sensor(daily_crypto_pipeline)],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
