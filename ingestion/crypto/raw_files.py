"""Raw-layer Parquet schemas and writes. Money stays exact: prices and quantities are DECIMAL(20,8)."""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

MONEY = pa.decimal128(20, 8)  # same as Project 1's NUMERIC(20, 8)
UTC_TIME = pa.timestamp("us", tz="UTC")

# Trades as Project 1 stored them, including its live anomaly scores.
STREAM_TRADES = pa.schema(
    [
        ("symbol", pa.string()),
        ("trade_id", pa.int64()),
        ("trade_time", UTC_TIME),
        ("price", MONEY),
        ("quantity", MONEY),
        ("is_anomaly", pa.bool_()),
        ("anomaly_type", pa.string()),
        ("price_z", pa.float64()),
        ("size_ratio", pa.float64()),
        ("received_at", UTC_TIME),
        ("stored_at", UTC_TIME),
    ]
)

# Project 1's trade_gaps table: missing trade-ID ranges its gap checker found.
TRADE_GAPS = pa.schema(
    [
        ("symbol", pa.string()),
        ("first_missing_id", pa.int64()),
        ("last_missing_id", pa.int64()),
        ("detected_at", UTC_TIME),
        ("resolved_at", UTC_TIME),
    ]
)

# The exchange's official record of every trade.
EXCHANGE_TRADES = pa.schema(
    [
        ("symbol", pa.string()),
        ("trade_id", pa.int64()),
        ("trade_time", UTC_TIME),
        ("price", MONEY),
        ("quantity", MONEY),
        ("is_buyer_maker", pa.bool_()),
    ]
)


def day_dir(raw_dir: Path, source: str, day: date) -> Path:
    """data/raw_crypto/<source>/trade_date=YYYY-MM-DD/"""
    return raw_dir / source / f"trade_date={day.isoformat()}"


def write_parquet(path: Path, rows: list[dict], schema: pa.Schema) -> int:
    """Write rows (possibly none) via a temp file, so a crash never leaves a half-written file.

    An empty file is still written: it records "extracted, nothing there", which is
    different from "never extracted".
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    tmp_path = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp_path)
    tmp_path.replace(path)
    return table.num_rows
