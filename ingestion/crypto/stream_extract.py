"""Copy one UTC day of Project 1's captured trades, plus its gap list, into the raw layer.

Read-only: the session is opened READ ONLY, so this can never change Project 1's data.

    data/raw_crypto/stream_trades/trade_date=2026-11-05/trades.parquet
    data/raw_crypto/trade_gaps/snapshot_date=2026-11-05/trade_gaps.parquet   (the whole list, as of that run)
"""

import argparse
import logging
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2

from ingestion.crypto.raw_files import STREAM_TRADES, TRADE_GAPS, day_dir, write_parquet
from ingestion.crypto.settings import CryptoSettings, Project1PostgresSettings

log = logging.getLogger("stream_extract")

TRADES_SQL = """
    SELECT symbol, trade_id, trade_time, price, quantity,
           is_anomaly, anomaly_type, price_z, size_ratio, received_at, stored_at
    FROM trades
    WHERE trade_time >= %s AND trade_time < %s
    ORDER BY symbol, trade_id
"""

GAPS_SQL = """
    SELECT symbol, first_missing_id, last_missing_id, detected_at, resolved_at
    FROM trade_gaps
    ORDER BY symbol, first_missing_id
"""


# trade_gaps only stores IDs, so a gap's UTC day comes from the captured trades on either
# side of it. A gap that spans midnight belongs to both days.
OPEN_GAP_DAYS_SQL = """
    SELECT g.symbol, g.first_missing_id, d.day
    FROM trade_gaps g
    CROSS JOIN LATERAL (
        SELECT DISTINCT (trade_time AT TIME ZONE 'UTC')::date AS day FROM (
            (SELECT trade_time FROM trades t
             WHERE t.symbol = g.symbol AND t.trade_id < g.first_missing_id
             ORDER BY t.trade_id DESC LIMIT 1)
            UNION
            (SELECT trade_time FROM trades t
             WHERE t.symbol = g.symbol AND t.trade_id > g.last_missing_id
             ORDER BY t.trade_id LIMIT 1)
        ) neighbours
    ) d
    WHERE g.resolved_at IS NULL
    ORDER BY d.day, g.symbol, g.first_missing_id
"""


def open_gap_days(conn: Any) -> dict[date, list[str]]:
    """UTC day -> open gaps on it, as "SYMBOL:first_missing_id" keys."""
    days: dict[date, list[str]] = {}
    for row in fetch_rows(conn, OPEN_GAP_DAYS_SQL):
        days.setdefault(row["day"], []).append(f"{row['symbol'].upper()}:{row['first_missing_id']}")
    return days


def day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time(0), tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def connect_read_only(settings: Project1PostgresSettings | None = None) -> Any:
    conn = psycopg2.connect(**(settings or Project1PostgresSettings()).connect_args())  # type: ignore[call-arg]
    conn.set_session(readonly=True, autocommit=True)
    return conn


def fetch_rows(conn: Any, sql: str, params: tuple = ()) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def extract_day(conn: Any, day: date, raw_dir: Path) -> dict[str, int]:
    """Write the day's captured trades and a snapshot of the gap list. Returns row counts."""
    start, end = day_bounds(day)
    trades = fetch_rows(conn, TRADES_SQL, (start, end))
    gaps = fetch_rows(conn, GAPS_SQL)

    counts = {
        "trades": write_parquet(day_dir(raw_dir, "stream_trades", day) / "trades.parquet", trades, STREAM_TRADES),
        "gaps": write_parquet(
            raw_dir / "trade_gaps" / f"snapshot_date={day.isoformat()}" / "trade_gaps.parquet", gaps, TRADE_GAPS
        ),
    }
    log.info("Extracted %d captured trades and %d gap rows for %s", counts["trades"], counts["gaps"], day)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="UTC day to extract, YYYY-MM-DD")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

    conn = connect_read_only()
    try:
        extract_day(conn, args.date, CryptoSettings().crypto_raw_dir)  # type: ignore[call-arg]
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
