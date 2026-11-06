"""Integration tests: real dbt builds over the sample data (3 cities, two overlapping runs).

Sample data: each city has a run fetched on 2026-11-05 and an older run fetched on
2026-11-04 that starts one day earlier and carries +1 °C on every temperature, so any
value that leaks from the older run into the overlap is easy to spot.
"""

import json
from datetime import date
from pathlib import Path

import pytest
import requests

from ingestion import fetch_weather as fw
from tests.conftest import Warehouse, build_warehouse

pytestmark = pytest.mark.integration

CITIES = 3
HOURS_PER_CITY = 8 * 24  # older run's first day + newer run's 7 days


def test_marts_have_one_row_per_city_and_hour(warehouse: Warehouse) -> None:
    assert warehouse.query("select count(*) from dim_city") == [(CITIES,)]
    assert warehouse.query("select count(*) from fct_weather_hourly") == [(CITIES * HOURS_PER_CITY,)]
    assert warehouse.query("select count(*) from fct_weather_daily") == [(CITIES * 8,)]
    assert warehouse.query("select count(*) from fct_weather_daily where not is_complete_day") == [(0,)]


def test_overlapping_hours_come_from_the_newest_run(warehouse: Warehouse) -> None:
    new_run = json.loads(
        (warehouse.data_dir / "raw_json" / "fetched_on=2026-11-05" / "london.json").read_text(encoding="utf-8")
    )
    expected = dict(
        zip(new_run["weather"]["hourly"]["time"], new_run["weather"]["hourly"]["temperature_2m"], strict=True)
    )

    rows = warehouse.query(
        "select strftime(forecast_hour_local, '%Y-%m-%dT%H:%M'), temperature_c "
        "from fct_weather_hourly where city_id = 'london' and forecast_hour_local >= '2026-11-05'"
    )

    assert len(rows) == len(expected)
    assert {hour: temp for hour, temp in rows} == expected


def test_hours_only_the_older_run_covers_are_kept(warehouse: Warehouse) -> None:
    rows = warehouse.query(
        "select count(*), min(cast(fetched_at_utc as date)) from fct_weather_hourly "
        "where city_id = 'london' and forecast_hour_local < '2026-11-05'"
    )
    assert rows == [(24, date(2026, 11, 4))]


def test_local_times_convert_to_utc_with_each_citys_offset(warehouse: Warehouse) -> None:
    offsets = dict(
        warehouse.query(
            "select city_id, any_value(date_diff('hour', forecast_hour_utc, forecast_hour_local)) "
            "from fct_weather_hourly where forecast_hour_local = '2026-11-06 12:00' group by city_id"
        )
    )
    assert offsets == {"london": 0, "montreal": -5, "sydney": 11}


class ReplaySession:
    """Serves each city's API payload from an existing raw run, so re-ingestion is offline."""

    def __init__(self, run_dir: Path) -> None:
        self.payloads = {}
        for path in run_dir.glob("*.json"):
            record = json.loads(path.read_text(encoding="utf-8"))
            self.payloads[record["lat"]] = record["weather"]

    def get(self, url: str, params: dict, timeout: float) -> requests.Response:
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(self.payloads[params["latitude"]]).encode()
        return response


def test_running_the_whole_pipeline_twice_for_a_day_changes_nothing(tmp_path: Path) -> None:
    """Idempotency: ingest + build the same day again -> identical row counts and checksums.

    Fetch timestamps legitimately change on a rerun, so they're left out of the checksums.
    """
    wh = build_warehouse(tmp_path)
    marts = {
        "dim_city": ("last_fetched_at_utc",),
        "fct_weather_hourly": ("fetched_at_utc",),
        "fct_weather_daily": (),
    }
    before = {table: wh.checksum(table, exclude) for table, exclude in marts.items()}
    raw_files_before = sorted(p.relative_to(wh.data_dir) for p in wh.data_dir.rglob("*.json"))

    run_dir = wh.data_dir / "raw_json" / "fetched_on=2026-11-05"
    failed = fw.run(
        date(2026, 11, 5),
        cities_csv=wh.data_dir / "raw_csv" / "cities.csv",
        raw_json_dir=wh.data_dir / "raw_json",
        session=ReplaySession(run_dir),  # type: ignore[arg-type]
        delay_seconds=0,
    )
    wh.dbt("build")

    assert failed == []
    assert sorted(p.relative_to(wh.data_dir) for p in wh.data_dir.rglob("*.json")) == raw_files_before
    assert {table: wh.checksum(table, exclude) for table, exclude in marts.items()} == before
