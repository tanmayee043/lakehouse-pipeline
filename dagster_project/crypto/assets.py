"""Raw crypto assets: the exchange's official record, and Project 1's live capture (read-only)."""

from collections.abc import Iterator

import psycopg2
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    BackfillPolicy,
    MaterializeResult,
    RetryPolicy,
    asset,
    multi_asset,
)

from dagster_project.crypto.partitions import crypto_partitions, days_in, partition_days
from ingestion.crypto.exchange_history import fetch_and_write
from ingestion.crypto.settings import CryptoSettings
from ingestion.crypto.stream_extract import connect_read_only, extract_day
from ingestion.fetch_weather import make_session

# Keys match the dbt sources in models/staging/crypto/_crypto_sources.yml, so Dagster
# draws the lineage from these assets into the dbt models.
EXCHANGE_TRADES = AssetKey(["crypto_raw", "exchange_trades"])
STREAM_TRADES = AssetKey(["crypto_raw", "stream_trades"])
STREAM_EXTRACTS = AssetKey(["crypto_raw", "stream_extracts"])
TRADE_GAPS = AssetKey(["crypto_raw", "trade_gaps"])


@asset(
    key=EXCHANGE_TRADES,
    group_name="crypto_ingestion",
    kinds={"python", "parquet"},
    partitions_def=crypto_partitions,
    backfill_policy=BackfillPolicy.single_run(),
    retry_policy=RetryPolicy(max_retries=2, delay=120),
    description="Every trade per symbol from Binance.US's history API, one Parquet file per symbol per UTC day.",
)
def exchange_trades(context: AssetExecutionContext) -> MaterializeResult:
    settings = CryptoSettings()  # type: ignore[call-arg]
    session = make_session()
    days = days_in(*partition_days(context))
    total = 0
    for day in days:
        for symbol in settings.symbols:
            total += fetch_and_write(session, symbol, day, settings.crypto_raw_dir)
    return MaterializeResult(
        metadata={"days": len(days), "symbols": ", ".join(settings.symbols), "official_trades": total}
    )


@multi_asset(
    specs=[
        AssetSpec(STREAM_TRADES, description="Trades Project 1 captured live, copied one UTC day at a time."),
        AssetSpec(STREAM_EXTRACTS, description="Which days were copied from Project 1 (one file per day)."),
        AssetSpec(TRADE_GAPS, description="Snapshot of Project 1's trade_gaps table."),
    ],
    group_name="crypto_ingestion",
    partitions_def=crypto_partitions,
    backfill_policy=BackfillPolicy.single_run(),
)
def project1_stream(context: AssetExecutionContext) -> Iterator[MaterializeResult]:
    """Copy Project 1's captured trades and gap list. Read-only: Project 1 is never changed.

    If Project 1's database isn't running, nothing is written and the run carries on, so the
    official record and analytics still build. Reconciliation then shows those days as
    `stream_not_extracted`; re-run the partition once Project 1 is back (it keeps 90 days).
    """
    settings = CryptoSettings()  # type: ignore[call-arg]
    days = days_in(*partition_days(context))
    try:
        conn = connect_read_only()
    except psycopg2.OperationalError as e:
        context.log.warning(f"Project 1's database isn't reachable, so no stream data was copied: {e}")
        for key in (STREAM_TRADES, STREAM_EXTRACTS, TRADE_GAPS):
            yield MaterializeResult(asset_key=key, metadata={"project1_reachable": False, "days": len(days)})
        return

    captured = gaps = 0
    try:
        for day in days:
            counts = extract_day(conn, day, settings.crypto_raw_dir)
            captured += counts["trades"]
            gaps = counts["gaps"]
    finally:
        conn.close()
    yield MaterializeResult(asset_key=STREAM_TRADES, metadata={"project1_reachable": True, "captured_trades": captured})
    yield MaterializeResult(asset_key=STREAM_EXTRACTS, metadata={"days_copied": len(days)})
    yield MaterializeResult(asset_key=TRADE_GAPS, metadata={"gaps_in_project1": gaps})
