"""Fetch a 7-day hourly forecast for every city in cities.csv and land it as raw JSON.

Output layout (one folder per run date, one file per city):

    data/raw_json/fetched_on=2026-11-05/montreal.json

Files are written untouched apart from the city metadata wrapper, so the
warehouse can always be rebuilt from this folder.
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from pydantic import BaseModel, Field, ValidationError, model_validator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CITIES_CSV = PROJECT_ROOT / "data" / "raw_csv" / "cities.csv"
RAW_JSON_DIR = PROJECT_ROOT / "data" / "raw_json"

API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = "temperature_2m,precipitation,wind_speed_10m"
FORECAST_DAYS = 7
REQUEST_DELAY_SECONDS = 0.5  # be a polite API citizen, no need to hammer it

log = logging.getLogger("fetch_weather")

# Fields passed via `extra=` that the JSON formatter copies into each log line.
LOG_FIELDS = ("city", "run_date", "path", "succeeded", "failed")


class JsonFormatter(logging.Formatter):
    """One JSON object per line, so log tools can filter by level, logger or city."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        entry.update({k: getattr(record, k) for k in LOG_FIELDS if hasattr(record, k)})
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        # ASCII-only output ("Montréal") stays valid whatever encoding the console or log file uses.
        return json.dumps(entry)


def setup_logging(log_format: str) -> None:
    handler = logging.StreamHandler()
    if log_format == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def slugify(city_name: str) -> str:
    """ASCII-only file name: "Montréal" -> "montreal", "Chiang Mai" -> "chiang_mai"."""
    ascii_name = unicodedata.normalize("NFKD", city_name).encode("ascii", "ignore").decode()
    return "_".join(ascii_name.lower().split())


def make_session() -> requests.Session:
    """Session that retries rate limits and server errors with exponential backoff."""
    retry = Retry(
        total=4,
        backoff_factor=1,  # 1s, 2s, 4s, 8s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def load_cities(cities_csv: Path = CITIES_CSV) -> list[dict]:
    with open(cities_csv, newline="", encoding="utf-8") as f:
        cities = list(csv.DictReader(f))

    slugs = [slugify(row["city"]) for row in cities]
    duplicates = sorted({s for s in slugs if slugs.count(s) > 1})
    if duplicates:
        raise ValueError(f"Duplicate city file names in {cities_csv.name}: {duplicates}")
    return cities


class HourlyForecast(BaseModel):
    """The hourly arrays the models unnest. They must line up one-to-one."""

    time: list[datetime] = Field(min_length=1)
    temperature_2m: list[float]
    precipitation: list[float]
    wind_speed_10m: list[float]

    @model_validator(mode="after")
    def _same_length(self) -> "HourlyForecast":
        lengths = {len(self.time), len(self.temperature_2m), len(self.precipitation), len(self.wind_speed_10m)}
        if len(lengths) != 1:
            raise ValueError(f"hourly arrays differ in length: {sorted(lengths)}")
        return self


class ForecastResponse(BaseModel):
    """What the models rely on in an Open-Meteo response. Other fields pass through untouched."""

    timezone: str = Field(min_length=1)  # IANA name such as "Asia/Tokyo"
    hourly: HourlyForecast


def fetch_weather(session: requests.Session, lat: float, lng: float) -> dict:
    params: dict[str, str | int | float] = {
        "latitude": lat,
        "longitude": lng,
        "hourly": HOURLY_VARIABLES,
        "forecast_days": FORECAST_DAYS,
        # Local wall-clock times plus the IANA zone name (e.g. "Asia/Tokyo"),
        # so the models can build both local days and DST-correct UTC timestamps.
        "timezone": "auto",
    }
    response = session.get(API_URL, params=params, timeout=10)
    response.raise_for_status()
    body = response.json()
    # Check the shape before anything is written; the raw response itself is kept as-is.
    ForecastResponse.model_validate(body)
    return body


def write_json(path: Path, record: dict) -> None:
    """Write via a temp file so a crash never leaves a half-written JSON behind."""
    tmp_path = path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)
    tmp_path.replace(path)


def run(
    run_date: date,
    *,
    cities_csv: Path = CITIES_CSV,
    raw_json_dir: Path = RAW_JSON_DIR,
    session: requests.Session | None = None,
    delay_seconds: float = REQUEST_DELAY_SECONDS,
) -> list[str]:
    """Fetch every city for run_date. Returns the names of cities that failed."""
    out_dir = raw_json_dir / f"fetched_on={run_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    cities = load_cities(cities_csv)
    session = session or make_session()
    failed = []
    log.info(
        "Fetching weather for %d cities into %s",
        len(cities),
        out_dir,
        extra={"run_date": run_date.isoformat(), "path": str(out_dir)},
    )

    for row in cities:
        city = row["city"]
        lat = float(row["lat"])
        lng = float(row["lng"])

        try:
            weather_data = fetch_weather(session, lat, lng)
        except requests.RequestException as e:
            log.error("FAILED %s: %s", city, e, extra={"city": city})
            failed.append(city)
            continue
        except ValidationError as e:
            # A malformed response counts as a failed city, so it never reaches dbt.
            log.error("FAILED %s: unexpected response: %s", city, e.errors()[0]["msg"], extra={"city": city})
            failed.append(city)
            continue

        # Attach the source city metadata alongside the raw API response,
        # so downstream steps don't need to re-join against the CSV just to know
        # which city this file belongs to.
        record = {
            "city": city,
            "country": row["country"],
            "population": int(row["population"]),
            "lat": lat,
            "lng": lng,
            "fetched_on": run_date.isoformat(),
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "weather": weather_data,
        }

        out_path = out_dir / f"{slugify(city)}.json"
        write_json(out_path, record)
        log.info("OK %s -> %s", city, out_path.name, extra={"city": city, "path": str(out_path)})
        time.sleep(delay_seconds)

    succeeded = len(cities) - len(failed)
    log.info(
        "Done: %d succeeded, %d failed",
        succeeded,
        len(failed),
        extra={"run_date": run_date.isoformat(), "succeeded": succeeded, "failed": len(failed)},
    )
    return failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today(),
        help="run date used for the output folder (default: today)",
    )
    parser.add_argument(
        "--log-format",
        choices=("text", "json"),
        default=os.environ.get("LOG_FORMAT", "text"),
        help="text for people, json for log tools (default: $LOG_FORMAT or text)",
    )
    args = parser.parse_args()

    setup_logging(args.log_format)
    failed = run(args.date)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
