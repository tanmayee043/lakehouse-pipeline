"""Read-only access to the DuckDB warehouse built by dbt, and to the Iceberg snapshots."""

import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# `streamlit run dashboard/app.py` only puts dashboard/ on the import path; the shared
# Iceberg module lives at the repo root.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from serving import iceberg  # noqa: E402

DUCKDB_PATH = Path(os.environ.get("LAKEHOUSE_DUCKDB_PATH", PROJECT_ROOT / "data" / "warehouse" / "lakehouse.duckdb"))


class WarehouseUnavailable(Exception):
    """The warehouse file is missing, not built yet, or locked by a running pipeline."""


def _query(sql: str, params: list | None = None) -> pd.DataFrame:
    if not DUCKDB_PATH.exists():
        raise WarehouseUnavailable(f"No warehouse found at {DUCKDB_PATH}. Run the pipeline once to build it.")
    try:
        # read_only lets the dashboard run alongside Dagster without taking the write lock.
        with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
            return con.execute(sql, params or []).df()
    except duckdb.CatalogException as e:
        raise WarehouseUnavailable(
            "The warehouse exists but its tables are missing. Run `dbt build` or the Dagster job."
        ) from e
    except duckdb.IOException as e:
        raise WarehouseUnavailable(
            "The warehouse is busy (the pipeline is probably writing to it). Try again in a minute."
        ) from e


def load_cities() -> pd.DataFrame:
    """One row per city, with its current local time for "now" markers and "today"."""
    return _query(
        """
        select
            city_id,
            city_name,
            country,
            population,
            latitude,
            longitude,
            timezone,
            last_fetched_at_utc,
            now() at time zone timezone as local_now,
            cast(now() at time zone timezone as date) as local_today
        from dim_city
        where timezone is not null
        order by city_name
        """
    )


def load_hourly(city_id: str) -> pd.DataFrame:
    return _query(
        """
        select forecast_hour_local, temperature_c, precipitation_mm, wind_speed_kmh
        from fct_weather_hourly
        where city_id = ?
        order by forecast_hour_local
        """,
        [city_id],
    )


def load_daily() -> pd.DataFrame:
    """Daily summaries for every city, with each row's offset from that city's local today."""
    return _query(
        """
        select
            d.*,
            date_diff('day', cast(now() at time zone c.timezone as date), d.forecast_date_local)
                as day_offset
        from fct_weather_daily as d
        join dim_city as c using (city_id)
        order by d.city_id, d.forecast_date_local
        """
    )


def load_health() -> pd.DataFrame:
    """Per-city freshness: last fetch, and how far ahead the stored forecast still reaches."""
    return _query(
        """
        select
            c.city_name,
            c.country,
            c.last_fetched_at_utc,
            max(h.forecast_hour_utc) as forecast_ends_utc,
            count(*) as hourly_rows,
            count(distinct h.forecast_date_local) as days_covered
        from dim_city as c
        left join fct_weather_hourly as h using (city_id)
        group by all
        order by forecast_ends_utc
        """
    )


# ---------- crypto ----------


def load_reconciliation() -> pd.DataFrame:
    return _query("select * from fct_reconciliation_daily order by trade_date desc, symbol")


def load_ohlcv(symbol: str) -> pd.DataFrame:
    return _query(
        """
        select
            o.*,
            coalesce(a.anomalies, 0) as anomalies,
            coalesce(a.price_anomalies, 0) as price_anomalies,
            coalesce(a.volume_anomalies, 0) as volume_anomalies,
            coalesce(a.both_anomalies, 0) as both_anomalies,
            a.captured_trades
        from fct_ohlcv_daily as o
        left join fct_anomalies_daily as a using (symbol, trade_date)
        where o.symbol = ?
        order by o.trade_date
        """,
        [symbol],
    )


def load_symbols() -> list[str]:
    return list(_query("select distinct symbol from fct_ohlcv_daily order by symbol")["symbol"])


def load_gaps() -> pd.DataFrame:
    return _query("select * from fct_trade_gaps order by detected_at_utc desc, symbol")


# ---------- Iceberg time travel ----------


def load_snapshots(name: str) -> list[iceberg.SnapshotInfo]:
    """Published versions of a mart, newest first. Raises WarehouseUnavailable if none exist yet."""
    try:
        found = iceberg.snapshots(name)
    except Exception as e:  # no catalog file, or the table was never published
        raise WarehouseUnavailable("No Iceberg snapshots yet. Run daily_crypto_pipeline once.") from e
    if not found:
        raise WarehouseUnavailable("No Iceberg snapshots yet. Run daily_crypto_pipeline once.")
    return found


def load_snapshot(name: str, snapshot_id: int) -> pd.DataFrame:
    return iceberg.read(name, snapshot_id).to_pandas()
