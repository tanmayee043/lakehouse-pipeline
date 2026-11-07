"""Crypto assets are partitioned by UTC day. The newest partition is always yesterday, the
last day the exchange's record is complete for."""

from datetime import date, timedelta

from dagster import AssetExecutionContext, DailyPartitionsDefinition

from ingestion.crypto.settings import CryptoSettings

crypto_partitions = DailyPartitionsDefinition(
    start_date=CryptoSettings().crypto_start_date.isoformat(),  # type: ignore[call-arg]
    timezone="UTC",
)


def partition_days(context: AssetExecutionContext) -> tuple[date, date]:
    """First and last UTC day (inclusive) of the run: one day, or a whole backfill range."""
    window = context.partition_time_window
    return window.start.date(), (window.end - timedelta(days=1)).date()


def days_in(first_day: date, last_day: date) -> list[date]:
    return [first_day + timedelta(days=n) for n in range((last_day - first_day).days + 1)]
