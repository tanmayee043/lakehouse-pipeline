from dagster import AssetSelection, DefaultScheduleStatus, Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource

from dagster_project.dbt_assets import dbt_project, lakehouse_dbt_assets
from dagster_project.weather.assets import raw_weather_forecasts

daily_weather_pipeline = define_asset_job(
    name="daily_weather_pipeline",
    # All assets, not just those downstream of the fetch: stg_cities reads
    # cities.csv directly, so edits to the city list must be rebuilt and tested too.
    selection=AssetSelection.all(),
    description="Fetch today's forecasts, then rebuild and test every dbt model.",
)

daily_weather_schedule = ScheduleDefinition(
    job=daily_weather_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    # Off until switched on in the UI, so nothing runs in the background unexpectedly.
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=[raw_weather_forecasts, lakehouse_dbt_assets],
    jobs=[daily_weather_pipeline],
    schedules=[daily_weather_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
