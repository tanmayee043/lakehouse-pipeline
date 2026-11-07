"""Ingestion asset for the weather domain: lands one raw JSON file per city per day."""

from datetime import datetime, timezone

from dagster import AssetExecutionContext, AssetKey, Failure, MaterializeResult, RetryPolicy, asset

from ingestion import fetch_weather


# The key matches the dbt source raw.weather_forecasts, so Dagster draws the
# lineage edge from this asset into the dbt models automatically.
@asset(
    key=AssetKey(["raw", "weather_forecasts"]),
    group_name="ingestion",
    kinds={"python", "json"},
    # HTTP errors are already retried per request; this covers a run where some
    # cities still failed (e.g. a longer API outage). Each retry refetches all cities.
    retry_policy=RetryPolicy(max_retries=2, delay=120),
    description="7-day hourly forecast per city from Open-Meteo, landed in data/raw_json/fetched_on=<date>/.",
)
def raw_weather_forecasts(context: AssetExecutionContext) -> MaterializeResult:
    run_date = datetime.now(timezone.utc).date()
    city_count = len(fetch_weather.load_cities())
    failed = fetch_weather.run(run_date)

    if failed:
        # Fail the step so dbt never builds on a partial day.
        raise Failure(
            description=f"{len(failed)} of {city_count} cities failed to fetch",
            metadata={"failed_cities": ", ".join(failed)},
        )

    out_dir = fetch_weather.RAW_JSON_DIR / f"fetched_on={run_date.isoformat()}"
    return MaterializeResult(
        metadata={
            "run_date": run_date.isoformat(),
            "cities_fetched": city_count,
            "folder": str(out_dir),
        }
    )
