"""Publish the crypto marts to Apache Iceberg, and read them back at any earlier snapshot.

The DuckDB warehouse is where dbt builds; Iceberg is what other engines read (DuckDB,
Spark, Athena later) and what keeps history. Each publish that changes a table adds one
snapshot, so "this table as of last Tuesday" is a snapshot lookup.

    data/iceberg/catalog.db                        SQLite catalog (table name -> metadata file)
    data/iceberg/crypto/<table>/metadata, data     Iceberg metadata and Parquet data files
"""

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ICEBERG_DIR = Path(os.environ.get("LAKEHOUSE_ICEBERG_DIR", PROJECT_ROOT / "data" / "iceberg"))
DUCKDB_PATH = Path(os.environ.get("LAKEHOUSE_DUCKDB_PATH", PROJECT_ROOT / "data" / "warehouse" / "lakehouse.duckdb"))

NAMESPACE = "crypto"
# The marts consumers read. fct_trades stays in DuckDB: it's the large per-trade table.
PUBLISHED_TABLES = ("fct_reconciliation_daily", "fct_ohlcv_daily", "fct_anomalies_daily", "fct_trade_gaps")
SNAPSHOT_RETENTION_DAYS = 90
CONTENT_SHA = "lakehouse.content-sha256"  # snapshot property: hash of the rows it holds


@dataclass(frozen=True)
class Published:
    table: str
    rows: int
    changed: bool  # False when the rows were identical to the current snapshot
    snapshot_id: int | None


@dataclass(frozen=True)
class SnapshotInfo:
    snapshot_id: int
    committed_at: datetime
    rows: int


def catalog(iceberg_dir: Path = ICEBERG_DIR) -> SqlCatalog:
    iceberg_dir.mkdir(parents=True, exist_ok=True)
    # Forward slashes: PyIceberg mangles file:// URIs on Windows; plain paths work everywhere.
    return SqlCatalog(
        "lakehouse",
        uri=f"sqlite:///{(iceberg_dir / 'catalog.db').as_posix()}",
        warehouse=iceberg_dir.as_posix(),
    )


def content_sha(rows: pa.Table) -> str:
    """Order-independent hash of a table's rows, used to skip publishes that change nothing."""
    ordered = rows.sort_by([(name, "ascending") for name in rows.column_names])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, ordered.schema) as writer:
        writer.write_table(ordered)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def _open_or_create(cat: SqlCatalog, name: str, schema: pa.Schema) -> Table:
    identifier = f"{NAMESPACE}.{name}"
    cat.create_namespace_if_not_exists(NAMESPACE)
    if not cat.table_exists(identifier):
        table = cat.create_table(identifier, schema=schema)
        # Partition by UTC day so engines reading one day skip the other days' files.
        with table.update_spec() as spec:
            spec.add_identity("trade_date")
        return cat.load_table(identifier)
    table = cat.load_table(identifier)
    # Schema evolution: a column added to the mart is added to the Iceberg table, and older
    # snapshots keep reading with the schema they were written with.
    with table.update_schema() as update:
        update.union_by_name(schema)
    return table


def publish(
    duckdb_path: Path = DUCKDB_PATH,
    iceberg_dir: Path = ICEBERG_DIR,
    retention_days: int = SNAPSHOT_RETENTION_DAYS,
) -> list[Published]:
    """Copy each published mart from DuckDB into its Iceberg table, then expire old snapshots."""
    cat = catalog(iceberg_dir)
    results = []
    with duckdb.connect(str(duckdb_path), read_only=True) as con:
        for name in PUBLISHED_TABLES:
            rows = con.sql(f"select * from {name}").to_arrow_table()
            table = _open_or_create(cat, name, rows.schema)
            sha = content_sha(rows)
            current = table.current_snapshot()
            if (
                current is not None
                and current.summary
                and current.summary.additional_properties.get(CONTENT_SHA) == sha
            ):
                results.append(Published(name, rows.num_rows, False, current.snapshot_id))
                continue
            table.overwrite(rows, snapshot_properties={CONTENT_SHA: sha})
            table.maintenance.expire_snapshots().older_than(
                datetime.now(timezone.utc) - timedelta(days=retention_days)
            ).commit()
            snapshot = cat.load_table(f"{NAMESPACE}.{name}").current_snapshot()
            results.append(Published(name, rows.num_rows, True, snapshot.snapshot_id if snapshot else None))
    return results


def snapshots(name: str, iceberg_dir: Path = ICEBERG_DIR) -> list[SnapshotInfo]:
    """The table's published versions, newest first.

    An overwrite commits two snapshots (remove old files, add new ones); only the "append"
    half holds a complete version of the table, so that's the one listed.
    """
    table = catalog(iceberg_dir).load_table(f"{NAMESPACE}.{name}")
    found = []
    for snap in table.snapshots():
        if snap.summary is None or snap.summary.operation != Operation.APPEND:
            continue
        rows = int(snap.summary.additional_properties.get("total-records", 0))
        committed = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc)
        found.append(SnapshotInfo(snap.snapshot_id, committed, rows))
    return sorted(found, key=lambda s: s.committed_at, reverse=True)


def read(name: str, snapshot_id: int | None = None, iceberg_dir: Path = ICEBERG_DIR) -> pa.Table:
    """The table now, or as it was at an earlier snapshot."""
    table = catalog(iceberg_dir).load_table(f"{NAMESPACE}.{name}")
    return table.scan(snapshot_id=snapshot_id).to_arrow()


def metadata_location(name: str, iceberg_dir: Path = ICEBERG_DIR) -> str:
    """Path of the table's current metadata file: what another engine (e.g. DuckDB) reads."""
    return catalog(iceberg_dir).load_table(f"{NAMESPACE}.{name}").metadata_location
