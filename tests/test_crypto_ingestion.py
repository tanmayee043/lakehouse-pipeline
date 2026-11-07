"""Unit tests for the crypto extractors. No network: the exchange API is faked."""

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pydantic
import pytest
import requests

from ingestion.crypto import exchange_history as xh
from ingestion.crypto.raw_files import EXCHANGE_TRADES, write_parquet
from ingestion.crypto.settings import CryptoSettings

DAY = date(2025, 10, 10)
DAY_START_MS = int(datetime(2025, 10, 10, tzinfo=timezone.utc).timestamp() * 1000)
DAY_END_MS = DAY_START_MS + 86_400_000


def trade(trade_id: int, time_ms: int, price: str = "100.10000000") -> dict:
    return {"id": trade_id, "price": price, "qty": "0.50000000", "time": time_ms, "isBuyerMaker": False}


class FakeExchange:
    """Serves aggTrades and historicalTrades from an in-memory list of trades."""

    def __init__(self, trades: list[dict], page_size: int = 3) -> None:
        self.trades = trades
        self.page_size = page_size
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: float) -> requests.Response:
        self.calls.append((url.rsplit("/", 1)[-1], params))
        if url.endswith("aggTrades"):
            after = [t for t in self.trades if t["time"] >= params["startTime"]][:1]
            body = [{"f": t["id"], "T": t["time"]} for t in after]
        else:
            body = [t for t in self.trades if t["id"] >= params["fromId"]][: self.page_size]
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(body).encode()
        return response


def fetch(exchange: FakeExchange) -> list[dict]:
    return xh.fetch_day(exchange, "BTCUSDT", DAY, delay_seconds=0)  # type: ignore[arg-type]


def test_fetches_exactly_the_utc_day_across_pages() -> None:
    trades = [
        trade(99, DAY_START_MS - 1),  # the evening before: excluded
        *[trade(100 + i, DAY_START_MS + i * 1000) for i in range(7)],  # spans three pages of 3
        trade(107, DAY_END_MS),  # next midnight: excluded
        trade(108, DAY_END_MS + 5),
    ]
    rows = fetch(FakeExchange(trades))

    assert [r["trade_id"] for r in rows] == list(range(100, 107))
    assert rows[0]["trade_time"] == datetime(2025, 10, 10, tzinfo=timezone.utc)
    assert rows[0]["price"] == Decimal("100.10000000")  # exact, never a float


def test_a_day_with_no_trades_gives_no_rows() -> None:
    exchange = FakeExchange([trade(500, DAY_END_MS + 1)])
    assert fetch(exchange) == []
    assert [name for name, _ in exchange.calls] == ["aggTrades"]  # no history pages requested


def test_refuses_a_day_that_is_not_over_yet() -> None:
    today = datetime.now(timezone.utc).date()
    with pytest.raises(ValueError, match="isn't over yet"):
        xh.fetch_day(FakeExchange([]), "BTCUSDT", today, delay_seconds=0)  # type: ignore[arg-type]


def test_rejects_a_response_with_a_non_positive_price() -> None:
    with pytest.raises(pydantic.ValidationError):
        fetch(FakeExchange([trade(100, DAY_START_MS, price="0")]))


def test_raw_parquet_keeps_money_exact_and_writes_empty_days(tmp_path: Path) -> None:
    row = {
        "symbol": "BTCUSDT",
        "trade_id": 1,
        "trade_time": datetime(2025, 10, 10, tzinfo=timezone.utc),
        "price": Decimal("117627.73000001"),
        "quantity": Decimal("0.00012345"),
        "is_buyer_maker": True,
    }
    write_parquet(tmp_path / "day.parquet", [row], EXCHANGE_TRADES)
    write_parquet(tmp_path / "empty.parquet", [], EXCHANGE_TRADES)

    table = pq.read_table(tmp_path / "day.parquet")
    assert str(table.schema.field("price").type) == "decimal128(20, 8)"
    assert table.column("price")[0].as_py() == Decimal("117627.73000001")
    assert pq.read_table(tmp_path / "empty.parquet").num_rows == 0
    assert not list(tmp_path.glob("*.tmp"))


def test_symbols_setting_is_normalised_and_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRYPTO_SYMBOLS", " btcusdt, ethusdt ")
    assert CryptoSettings().symbols == ["BTCUSDT", "ETHUSDT"]  # type: ignore[call-arg]

    monkeypatch.setenv("CRYPTO_SYMBOLS", "BTC/USDT")
    with pytest.raises(pydantic.ValidationError, match="comma-separated symbols"):
        CryptoSettings()  # type: ignore[call-arg]
