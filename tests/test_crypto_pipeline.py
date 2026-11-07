"""Crypto models over the sample data: Project 1's crash-day recording with planted faults.

Planted in tests/data/raw_crypto (see tests/data/README.md):
    BTCUSDT 2025-10-10: 10 trades missing from the stream, listed as an open gap
    ETHUSDT 2025-10-10: 1 trade missing from the stream, listed as an open gap
    SOLUSDT 2025-10-10: a 10-minute outage (178 trades), listed as an open gap
    SOLUSDT 2025-10-11: 1 captured price off by 0.01
"""

import json
import shutil
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tests.conftest import Warehouse, build_warehouse

pytestmark = pytest.mark.integration

CRYPTO_MARTS = {
    "fct_trades": (),
    "fct_reconciliation_daily": (),
    "fct_ohlcv_daily": (),
    "fct_anomalies_daily": (),
    "fct_trade_gaps": (),
}


def partition_build(wh: Warehouse, day: str) -> None:
    """What Dagster runs for one daily partition."""
    window = json.dumps({"crypto_start_date": day, "crypto_end_date": day})
    wh.dbt("build", "--select", "tag:crypto", "--vars", window)


def test_reconciliation_finds_exactly_the_planted_faults(warehouse: Warehouse) -> None:
    rows = warehouse.query(
        "select symbol, cast(trade_date as varchar), missed_while_running, missed_during_downtime, price_mismatches, "
        "stream_only_trades, status from fct_reconciliation_daily order by trade_date, symbol"
    )
    assert rows == [
        ("BTCUSDT", "2025-10-10", 10, 0, 0, 0, "gaps_filled"),
        ("ETHUSDT", "2025-10-10", 1, 0, 0, 0, "gaps_filled"),
        ("SOLUSDT", "2025-10-10", 0, 178, 0, 0, "downtime_filled"),
        ("BTCUSDT", "2025-10-11", 0, 0, 0, 0, "complete"),
        ("ETHUSDT", "2025-10-11", 0, 0, 0, 0, "complete"),
        ("SOLUSDT", "2025-10-11", 0, 0, 1, 0, "disagrees"),
    ]


def test_an_outage_does_not_count_against_completeness_while_running(warehouse: Warehouse) -> None:
    rows = warehouse.query(
        "select completeness_pct, day_coverage_pct < 100 from fct_reconciliation_daily "
        "where symbol = 'SOLUSDT' and trade_date = '2025-10-10'"
    )
    assert rows == [(100.0, True)]


def test_every_official_trade_is_kept_once_and_reported_gaps_are_filled(warehouse: Warehouse) -> None:
    assert warehouse.query("select count(*), count(*) filter (where captured_live) from fct_trades") == [(10507, 10318)]
    assert warehouse.query("select symbol, missing_count, fill_status from fct_trade_gaps order by 1") == [
        ("BTCUSDT", 10, "filled"),
        ("ETHUSDT", 1, "filled"),
        ("SOLUSDT", 178, "filled"),
    ]


def test_daily_candles_match_an_independent_decimal_calculation(warehouse: Warehouse) -> None:
    trades: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for path in (warehouse.data_dir / "raw_crypto" / "exchange_trades").rglob("*.parquet"):
        day = path.parent.name.removeprefix("trade_date=")
        for row in pq.read_table(path).to_pylist():
            trades[(row["symbol"], day)].append(row)

    expected = {}
    for key, rows in trades.items():
        rows.sort(key=lambda r: r["trade_id"])
        volume = sum((r["quantity"] for r in rows), Decimal(0))
        expected[key] = (rows[0]["price"], max(r["price"] for r in rows), min(r["price"] for r in rows),
                         rows[-1]["price"], volume, len(rows))  # fmt: skip

    actual = {
        (symbol, day): rest
        for symbol, day, *rest in warehouse.query(
            "select symbol, cast(trade_date as varchar), open_price, high_price, low_price, close_price, volume, "
            "trade_count from fct_ohlcv_daily"
        )
    }
    assert {k: tuple(v) for k, v in actual.items()} == expected


def test_anomaly_counts_come_from_project1s_flags(warehouse: Warehouse) -> None:
    rows = warehouse.query(
        "select symbol, anomalies, price_anomalies, volume_anomalies, both_anomalies from fct_anomalies_daily "
        "where trade_date = '2025-10-10' order by symbol"
    )
    assert rows == [("BTCUSDT", 1, 1, 0, 0), ("ETHUSDT", 1, 0, 1, 0), ("SOLUSDT", 1, 0, 0, 1)]


def test_rerunning_a_daily_partition_changes_nothing(warehouse: Warehouse, tmp_path: Path) -> None:
    wh = build_warehouse(tmp_path)
    before = {table: wh.checksum(table) for table in CRYPTO_MARTS}

    partition_build(wh, "2025-10-10")
    partition_build(wh, "2025-10-10")

    assert {table: wh.checksum(table) for table in CRYPTO_MARTS} == before


def test_a_partition_run_only_touches_its_own_day(tmp_path: Path) -> None:
    """Incremental proof: with the 11th's raw files gone, rebuilding the 10th keeps the 11th's rows."""
    wh = build_warehouse(tmp_path)
    eleventh_before = wh.query("select count(*) from fct_trades where trade_date = '2025-10-11'")

    for source in ("exchange_trades", "stream_trades"):
        shutil.rmtree(wh.data_dir / "raw_crypto" / source / "trade_date=2025-10-11")
    partition_build(wh, "2025-10-10")

    assert wh.query("select count(*) from fct_trades where trade_date = '2025-10-11'") == eleventh_before
    assert eleventh_before == [(1105,)]
