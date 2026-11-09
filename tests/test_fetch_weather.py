"""Unit tests for the ingestion script. No network: HTTP goes through a fake session."""

import json
import logging
from datetime import date
from pathlib import Path

import pytest
import requests
from requests.adapters import HTTPAdapter

from ingestion import fetch_weather as fw

RUN_DATE = date(2026, 11, 6)
CITIES_CSV = (
    "city,lat,lng,country,population\nMontréal,45.5,-73.5,Canada,3675219\nChiang Mai,18.8,98.9,Thailand,1198000\n"
)


VALID_PAYLOAD = {
    "timezone": "UTC",
    "hourly": {
        "time": ["2026-11-06T00:00", "2026-11-06T01:00"],
        "temperature_2m": [10.5, 10.1],
        "precipitation": [0.0, 0.2],
        "wind_speed_10m": [12.0, 14.4],
    },
}


class FakeSession:
    """Returns a canned API payload, or an HTTP error for the latitudes listed in fail_lats."""

    def __init__(self, fail_lats: tuple[float, ...] = (), payload: dict | None = None) -> None:
        self.fail_lats = fail_lats
        self.payload = payload or VALID_PAYLOAD
        self.calls: list[dict] = []

    def get(self, url: str, params: dict, timeout: float) -> requests.Response:
        self.calls.append(params)
        response = requests.Response()
        response.url = url
        if params["latitude"] in self.fail_lats:
            response.status_code = 503
            response._content = b"Service Unavailable"
        else:
            response.status_code = 200
            response._content = json.dumps(self.payload).encode()
        return response


@pytest.fixture
def cities_csv(tmp_path: Path) -> Path:
    path = tmp_path / "cities.csv"
    path.write_text(CITIES_CSV, encoding="utf-8")
    return path


def run(cities_csv: Path, out: Path, session: FakeSession) -> list[str]:
    return fw.run(RUN_DATE, cities_csv=cities_csv, raw_json_dir=out, session=session, delay_seconds=0)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("name", "slug"),
    [("Montréal", "montreal"), ("Chiang Mai", "chiang_mai"), ("Santa Cruz de la Sierra", "santa_cruz_de_la_sierra")],
)
def test_slugify_is_ascii_and_matches_dbt_city_id(name: str, slug: str) -> None:
    assert fw.slugify(name) == slug


def test_load_cities_rejects_names_that_collide_as_file_names(tmp_path: Path) -> None:
    path = tmp_path / "cities.csv"
    path.write_text("city,lat,lng,country,population\nSan José,1,1,X,1\nSan Jose,2,2,Y,1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="san_jose"):
        fw.load_cities(path)


def test_run_writes_one_file_per_city_with_metadata(cities_csv: Path, tmp_path: Path) -> None:
    failed = run(cities_csv, tmp_path / "raw", FakeSession())

    assert failed == []
    out_dir = tmp_path / "raw" / "fetched_on=2026-11-06"
    assert sorted(p.name for p in out_dir.iterdir()) == ["chiang_mai.json", "montreal.json"]
    record = json.loads((out_dir / "montreal.json").read_text(encoding="utf-8"))
    assert record["city"] == "Montréal"
    assert record["population"] == 3675219
    assert record["fetched_on"] == "2026-11-06"
    assert record["fetched_at"].endswith("+00:00")
    assert record["weather"]["timezone"] == "UTC"


def test_run_asks_for_local_times_and_seven_days(cities_csv: Path, tmp_path: Path) -> None:
    session = FakeSession()
    run(cities_csv, tmp_path / "raw", session)

    params = session.calls[0]
    assert params["timezone"] == "auto"
    assert params["forecast_days"] == 7
    assert params["hourly"] == "temperature_2m,precipitation,wind_speed_10m"


def test_run_reports_failed_cities_and_keeps_going(cities_csv: Path, tmp_path: Path) -> None:
    failed = run(cities_csv, tmp_path / "raw", FakeSession(fail_lats=(45.5,)))

    assert failed == ["Montréal"]
    files = [p.name for p in (tmp_path / "raw" / "fetched_on=2026-11-06").iterdir()]
    assert files == ["chiang_mai.json"]  # the other city still landed, and no .tmp file is left


def test_rerunning_the_same_day_overwrites_instead_of_duplicating(cities_csv: Path, tmp_path: Path) -> None:
    run(cities_csv, tmp_path / "raw", FakeSession())
    run(cities_csv, tmp_path / "raw", FakeSession())

    assert len(list((tmp_path / "raw").rglob("*.json"))) == 2
    assert list((tmp_path / "raw").rglob("*.tmp")) == []


def test_session_retries_rate_limits_and_server_errors_only() -> None:
    adapter = fw.make_session().get_adapter("https://api.open-meteo.com")
    assert isinstance(adapter, HTTPAdapter)
    retry = adapter.max_retries

    assert retry.total == 4
    assert retry.backoff_factor == 1
    assert {429, 500, 502, 503, 504} == set(retry.status_forcelist or ())
    assert 404 not in (retry.status_forcelist or ())


def test_json_log_lines_carry_the_extra_fields() -> None:
    record = logging.LogRecord("fetch_weather", logging.ERROR, __file__, 1, "FAILED %s", ("Montréal",), None)
    record.city = "Montréal"

    line = fw.JsonFormatter().format(record)
    entry = json.loads(line)

    assert line.isascii()  # survives any console or log-file encoding
    assert entry["level"] == "ERROR"
    assert entry["message"] == "FAILED Montréal"
    assert entry["city"] == "Montréal"
    assert "path" not in entry


def malformed(**changes: object) -> dict:
    hourly = {**VALID_PAYLOAD["hourly"], **{k: v for k, v in changes.items() if k != "timezone"}}  # type: ignore[dict-item]
    payload: dict = {**VALID_PAYLOAD, "hourly": hourly}
    if "timezone" in changes:
        payload["timezone"] = changes["timezone"]
    return payload


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(malformed(timezone=""), id="no timezone"),
        pytest.param(malformed(temperature_2m=[10.5]), id="arrays of different lengths"),
        pytest.param(malformed(precipitation=[None, 0.2]), id="null value"),
        pytest.param(malformed(time=["not a time", "2026-11-06T01:00"]), id="bad timestamp"),
    ],
)
def test_a_malformed_response_fails_the_city_and_writes_nothing(
    payload: dict, cities_csv: Path, tmp_path: Path
) -> None:
    failed = run(cities_csv, tmp_path / "raw", FakeSession(payload=payload))

    assert failed == ["Montréal", "Chiang Mai"]
    assert list((tmp_path / "raw").rglob("*.json")) == []
