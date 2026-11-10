"""Smoke tests: the Dagster definitions load, and the dashboard renders without errors."""

import importlib
import sys
from pathlib import Path

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from tests.conftest import PROJECT_ROOT, Warehouse

pytestmark = pytest.mark.integration

DASHBOARD_DIR = PROJECT_ROOT / "dashboard"


def test_dagster_definitions_load_with_full_lineage(warehouse: Warehouse) -> None:
    # Depends on `warehouse` so dbt has written the manifest dagster-dbt reads.
    from dagster_project.definitions import defs

    graph = defs.resolve_asset_graph()
    keys = {key.to_user_string() for key in graph.get_all_asset_keys()}
    assert {"raw/weather_forecasts", "stg_weather_hourly", "int_weather_latest", "fct_weather_daily"} <= keys
    assert graph.get(next(k for k in graph.get_all_asset_keys() if k.path == ["stg_weather_hourly"])).parent_keys
    assert defs.resolve_job_def("daily_weather_pipeline") is not None


def test_crypto_lineage_runs_from_both_sources_into_reconciliation(warehouse: Warehouse) -> None:
    from dagster import AssetKey

    from dagster_project.definitions import defs

    graph = defs.resolve_asset_graph()
    reconciliation = graph.get(AssetKey(["fct_reconciliation_daily"]))
    assert {k.to_user_string() for k in reconciliation.parent_keys} >= {
        "crypto_raw/exchange_trades",
        "crypto_raw/stream_trades",
    }
    assert reconciliation.partitions_def is not None  # daily partitions
    assert graph.get(AssetKey(["raw", "weather_forecasts"])).partitions_def is None  # weather stays unpartitioned
    assert [s.name for s in defs.sensors or ()] == ["crypto_project_gap_sensor"]
    published = graph.get(AssetKey(["iceberg", "fct_reconciliation_daily"]))
    assert [k.to_user_string() for k in published.parent_keys] == ["fct_reconciliation_daily"]


def run_dashboard(db_path: Path, monkeypatch: pytest.MonkeyPatch) -> AppTest:
    monkeypatch.setenv("LAKEHOUSE_DUCKDB_PATH", str(db_path))
    monkeypatch.syspath_prepend(str(DASHBOARD_DIR))
    # queries reads the path at import time, and cached results would hide the new path.
    if "queries" in sys.modules:
        importlib.reload(sys.modules["queries"])
    st.cache_data.clear()
    return AppTest.from_file(str(DASHBOARD_DIR / "app.py"), default_timeout=60).run()


def test_dashboard_renders_all_tabs(warehouse: Warehouse, monkeypatch: pytest.MonkeyPatch) -> None:
    app = run_dashboard(warehouse.db_path, monkeypatch)

    assert not app.exception
    assert [tab.label for tab in app.tabs] == ["City forecast", "Compare cities", "Pipeline health"]
    assert app.selectbox[0].value == "london"
    assert len(app.metric) >= 8  # 4 forecast tiles + 4 health tiles


def test_dashboard_explains_a_missing_warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = run_dashboard(tmp_path / "missing.duckdb", monkeypatch)

    assert not app.exception
    assert "No warehouse found" in app.error[0].value
