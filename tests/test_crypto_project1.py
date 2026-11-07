"""Reading Project 1's database: a throwaway Postgres (Testcontainers) with Project 1's real schema.

Needs Docker. The data is small and hand-made so every expected value is obvious:
    BTCUSDT 1-5 late on 10 Oct, 8-9 just after midnight -> open gap 6-7 spans both days
    ETHUSDT 10, 12 on 10 Oct                             -> open gap 11
    SOLUSDT gap 20-21, already resolved by Project 1    -> not reported as open
"""

from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.errors
import pyarrow.parquet as pq
import pytest
from testcontainers.community.postgres import PostgresContainer

from ingestion.crypto.settings import Project1PostgresSettings
from ingestion.crypto.stream_extract import connect_read_only, extract_day, open_gap_days

pytestmark = pytest.mark.integration

SCHEMA = Path(__file__).resolve().parent / "data" / "project1" / "schema.sql"
MIDNIGHT = datetime(2025, 10, 11, tzinfo=timezone.utc)

TRADES = [
    *[("BTCUSDT", i, MIDNIGHT - timedelta(seconds=10 - i), "117000.12345678", "0.001") for i in range(1, 6)],
    ("BTCUSDT", 8, MIDNIGHT + timedelta(seconds=1), "117001.00000000", "0.002"),
    ("BTCUSDT", 9, MIDNIGHT + timedelta(seconds=2), "117002.00000000", "0.003"),
    ("ETHUSDT", 10, MIDNIGHT - timedelta(hours=2), "4000.00000000", "0.1"),
    ("ETHUSDT", 12, MIDNIGHT - timedelta(hours=1), "4001.00000000", "0.2"),
    ("SOLUSDT", 19, MIDNIGHT - timedelta(hours=3), "200.00000000", "1"),
    ("SOLUSDT", 22, MIDNIGHT - timedelta(hours=3), "200.50000000", "1"),
]
GAPS = [
    ("BTCUSDT", 6, 7, None),
    ("ETHUSDT", 11, 11, None),
    ("SOLUSDT", 20, 21, MIDNIGHT),
]


@pytest.fixture(scope="module")
def project1() -> Iterator[Project1PostgresSettings]:
    with PostgresContainer("postgres:16.15", username="crypto", password="test", dbname="crypto_db") as pg:
        settings = Project1PostgresSettings(
            host=pg.get_container_host_ip(),
            port=int(pg.get_exposed_port(5432)),
            db="crypto_db",
            user="crypto",
            password="test",
        )
        with psycopg2.connect(**settings.connect_args()) as conn, conn.cursor() as cur:  # type: ignore[call-overload]
            cur.execute(SCHEMA.read_text(encoding="utf-8"))
            cur.executemany(
                "INSERT INTO trades (symbol, trade_id, trade_time, price, quantity, is_anomaly) "
                "VALUES (%s, %s, %s, %s, %s, false)",
                TRADES,
            )
            cur.executemany(
                "INSERT INTO trade_gaps (symbol, first_missing_id, last_missing_id, resolved_at) "
                "VALUES (%s, %s, %s, %s)",
                GAPS,
            )
        yield settings


def test_extract_copies_only_that_utc_day_with_exact_prices(project1: Project1PostgresSettings, tmp_path: Path) -> None:
    conn = connect_read_only(project1)
    try:
        counts = extract_day(conn, date(2025, 10, 10), tmp_path)
    finally:
        conn.close()

    trades = pq.read_table(tmp_path / "stream_trades" / "trade_date=2025-10-10" / "trades.parquet").to_pylist()
    assert counts == {"trades": 9, "gaps": 3}  # 5 BTC + 2 ETH + 2 SOL on the 10th; the whole gap list
    assert {(t["symbol"], t["trade_id"]) for t in trades if t["symbol"] == "BTCUSDT"} == {
        ("BTCUSDT", i) for i in range(1, 6)
    }
    assert trades[0]["price"] == Decimal("117000.12345678")


def test_a_day_with_nothing_captured_still_writes_an_empty_file(
    project1: Project1PostgresSettings, tmp_path: Path
) -> None:
    conn = connect_read_only(project1)
    try:
        extract_day(conn, date(2025, 10, 1), tmp_path)
    finally:
        conn.close()
    assert pq.read_table(tmp_path / "stream_trades" / "trade_date=2025-10-01" / "trades.parquet").num_rows == 0


def test_the_connection_cannot_change_project1(project1: Project1PostgresSettings) -> None:
    conn = connect_read_only(project1)
    try:
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction), conn.cursor() as cur:
            cur.execute("DELETE FROM trade_gaps")
    finally:
        conn.close()


def test_open_gaps_map_to_their_days_including_across_midnight(project1: Project1PostgresSettings) -> None:
    conn = connect_read_only(project1)
    try:
        days = open_gap_days(conn)
    finally:
        conn.close()

    assert days == {
        date(2025, 10, 10): ["BTCUSDT:6", "ETHUSDT:11"],
        date(2025, 10, 11): ["BTCUSDT:6"],
    }
