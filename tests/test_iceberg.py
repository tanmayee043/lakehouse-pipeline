"""Publishing the crypto marts to Iceberg: snapshots, time travel, expiry, schema evolution,
and a cross-engine check (PyIceberg and DuckDB must read identical rows)."""

import shutil
from pathlib import Path

import duckdb
import pytest

from serving import iceberg
from tests.conftest import Warehouse

pytestmark = pytest.mark.integration


@pytest.fixture
def warehouse_copy(warehouse: Warehouse, tmp_path: Path) -> Path:
    """A private copy of the sample warehouse that a test may change."""
    copy = tmp_path / "copy.duckdb"
    shutil.copy(warehouse.db_path, copy)
    return copy


def test_publishing_unchanged_data_adds_no_snapshot(warehouse: Warehouse, tmp_path: Path) -> None:
    first = iceberg.publish(warehouse.db_path, tmp_path)
    second = iceberg.publish(warehouse.db_path, tmp_path)

    assert all(r.changed for r in first)
    assert not any(r.changed for r in second)
    assert [r.snapshot_id for r in first] == [r.snapshot_id for r in second]
    assert all(len(iceberg.snapshots(name, tmp_path)) == 1 for name in iceberg.PUBLISHED_TABLES)


def test_time_travel_returns_the_table_as_it_was(warehouse_copy: Path, tmp_path: Path) -> None:
    iceberg.publish(warehouse_copy, tmp_path)
    with duckdb.connect(str(warehouse_copy)) as con:
        con.execute("delete from fct_reconciliation_daily where symbol = 'SOLUSDT'")
    iceberg.publish(warehouse_copy, tmp_path)

    newest, older = iceberg.snapshots("fct_reconciliation_daily", tmp_path)
    assert (older.rows, newest.rows) == (6, 4)
    then = iceberg.read("fct_reconciliation_daily", older.snapshot_id, tmp_path)
    now = iceberg.read("fct_reconciliation_daily", iceberg_dir=tmp_path)
    assert "SOLUSDT" in then.column("symbol").to_pylist()
    assert "SOLUSDT" not in now.column("symbol").to_pylist()


def test_duckdb_reads_exactly_what_pyiceberg_reads(warehouse: Warehouse, tmp_path: Path) -> None:
    iceberg.publish(warehouse.db_path, tmp_path)
    con = duckdb.connect()
    con.execute("install iceberg; load iceberg")

    for name in iceberg.PUBLISHED_TABLES:
        via_duckdb = con.sql(f"select * from iceberg_scan('{iceberg.metadata_location(name, tmp_path)}')")
        via_pyiceberg = iceberg.read(name, iceberg_dir=tmp_path)
        assert iceberg.content_sha(via_duckdb.to_arrow_table()) == iceberg.content_sha(via_pyiceberg), name

    money = iceberg.read("fct_ohlcv_daily", iceberg_dir=tmp_path).schema.field("close_price").type
    assert str(money) == "decimal128(20, 8)"


def test_old_snapshots_expire_but_the_current_one_stays(warehouse_copy: Path, tmp_path: Path) -> None:
    iceberg.publish(warehouse_copy, tmp_path)
    with duckdb.connect(str(warehouse_copy)) as con:
        con.execute("delete from fct_trade_gaps where symbol = 'ETHUSDT'")
    iceberg.publish(warehouse_copy, tmp_path, retention_days=0)

    kept = iceberg.snapshots("fct_trade_gaps", tmp_path)
    assert [s.rows for s in kept] == [2]
    assert iceberg.read("fct_trade_gaps", iceberg_dir=tmp_path).num_rows == 2


def test_a_new_column_evolves_the_iceberg_schema(warehouse_copy: Path, tmp_path: Path) -> None:
    iceberg.publish(warehouse_copy, tmp_path)
    with duckdb.connect(str(warehouse_copy)) as con:
        con.execute("alter table fct_ohlcv_daily add column note varchar default 'checked'")
    iceberg.publish(warehouse_copy, tmp_path)

    newest, older = iceberg.snapshots("fct_ohlcv_daily", tmp_path)
    assert set(iceberg.read("fct_ohlcv_daily", newest.snapshot_id, tmp_path).column("note").to_pylist()) == {"checked"}
    # Time travel reads a snapshot with the schema it had then, so the older version has no `note`.
    then = iceberg.read("fct_ohlcv_daily", older.snapshot_id, tmp_path)
    assert "note" not in then.column_names
    assert then.num_rows == newest.rows
