"""Download the exchange's official record of every trade for one UTC day.

    data/raw_crypto/exchange_trades/trade_date=2026-11-05/BTCUSDT.parquet

Binance.US trade IDs are sequential per symbol, so a day's record is complete when every
ID from the day's first trade to its last is present. No API key is needed.
"""

import argparse
import logging
import sys
import time as clock
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import requests
from pydantic import BaseModel, Field, TypeAdapter

from ingestion.crypto.raw_files import EXCHANGE_TRADES, day_dir, write_parquet
from ingestion.crypto.settings import CryptoSettings
from ingestion.crypto.stream_extract import day_bounds
from ingestion.fetch_weather import make_session

log = logging.getLogger("exchange_history")

API = "https://api.binance.us/api/v3"
PAGE_SIZE = 1000  # the API's maximum
REQUEST_DELAY_SECONDS = 1.0  # well under the REST weight limit


class ApiTrade(BaseModel):
    """One row of /historicalTrades. A response that doesn't match stops the run."""

    id: int = Field(ge=0)
    price: Decimal = Field(gt=0)
    qty: Decimal = Field(gt=0)
    time: int = Field(gt=0)  # milliseconds since the epoch, UTC
    isBuyerMaker: bool


class ApiAggTrade(BaseModel):
    """The two fields of /aggTrades used to find a day's first trade ID."""

    f: int = Field(ge=0)  # first trade ID in this aggregate
    T: int = Field(gt=0)


TRADES = TypeAdapter(list[ApiTrade])
AGG_TRADES = TypeAdapter(list[ApiAggTrade])


def to_ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def get_json(session: requests.Session, path: str, params: dict[str, str | int]) -> object:
    response = session.get(f"{API}/{path}", params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def first_trade_id(session: requests.Session, symbol: str, start_ms: int, end_ms: int) -> int | None:
    """ID of the first trade at or after start_ms, or None if the day had no trades.

    historicalTrades can only page by ID, but aggTrades can search by time, and each
    aggregate trade records its first trade ID.
    """
    aggs = AGG_TRADES.validate_python(
        get_json(session, "aggTrades", {"symbol": symbol, "startTime": start_ms, "limit": 1})
    )
    if not aggs or aggs[0].T >= end_ms:
        return None
    return aggs[0].f


def fetch_day(
    session: requests.Session, symbol: str, day: date, delay_seconds: float = REQUEST_DELAY_SECONDS
) -> list[dict]:
    """Every trade for symbol during the UTC day, in ID order."""
    start, end = day_bounds(day)
    if end > datetime.now(timezone.utc):
        raise ValueError(f"{day} isn't over yet in UTC; only complete days have a complete record")
    start_ms, end_ms = to_ms(start), to_ms(end)

    from_id = first_trade_id(session, symbol, start_ms, end_ms)
    rows: list[dict] = []
    while from_id is not None:
        clock.sleep(delay_seconds)
        page = TRADES.validate_python(
            get_json(session, "historicalTrades", {"symbol": symbol, "fromId": from_id, "limit": PAGE_SIZE})
        )
        for trade in page:
            if trade.time >= end_ms:
                from_id = None
                break
            rows.append(
                {
                    "symbol": symbol,
                    "trade_id": trade.id,
                    "trade_time": datetime.fromtimestamp(trade.time / 1000, tz=timezone.utc),
                    "price": trade.price,
                    "quantity": trade.qty,
                    "is_buyer_maker": trade.isBuyerMaker,
                }
            )
        else:
            # Whole page was inside the day: continue after its last trade (stop if it was empty).
            from_id = page[-1].id + 1 if page else None

    breaks = sum(1 for a, b in zip(rows, rows[1:], strict=False) if b["trade_id"] != a["trade_id"] + 1)
    if breaks:
        # The completeness check assumes sequential IDs, so say loudly if the exchange ever breaks that.
        log.warning("%s %s: %d breaks in the exchange's own ID sequence", symbol, day, breaks)
    return rows


def fetch_and_write(
    session: requests.Session,
    symbol: str,
    day: date,
    raw_dir: Path,
    delay_seconds: float = REQUEST_DELAY_SECONDS,
) -> int:
    rows = fetch_day(session, symbol, day, delay_seconds)
    count = write_parquet(day_dir(raw_dir, "exchange_trades", day) / f"{symbol}.parquet", rows, EXCHANGE_TRADES)
    log.info("Fetched %d official trades for %s on %s", count, symbol, day)
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="UTC day, YYYY-MM-DD")
    parser.add_argument("--symbols", help="comma-separated; default: $CRYPTO_SYMBOLS")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

    settings = CryptoSettings(**({"crypto_symbols": args.symbols} if args.symbols else {}))  # type: ignore[arg-type]
    session = make_session()
    for symbol in settings.symbols:
        fetch_and_write(session, symbol, args.date, settings.crypto_raw_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
