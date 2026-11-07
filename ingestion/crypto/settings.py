"""Settings for the crypto sources. A bad value stops the run at startup and names the variable."""

from datetime import date
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"


class CryptoSettings(BaseSettings):
    """Which symbols to track and where raw crypto files land."""

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")

    # Comma-separated, same pairs Project 1 streams by default.
    crypto_symbols: str = Field("BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT", min_length=1)
    crypto_raw_dir: Path = PROJECT_ROOT / "data" / "raw_crypto"
    # First UTC day of the daily partitions. Days before it can't be scheduled or backfilled.
    crypto_start_date: date = date(2026, 9, 1)

    @field_validator("crypto_symbols")
    @classmethod
    def _upper_and_trimmed(cls, value: str) -> str:
        symbols = [s.strip().upper() for s in value.split(",") if s.strip()]
        if not symbols or not all(s.isalnum() for s in symbols):
            raise ValueError("expected comma-separated symbols like BTCUSDT,ETHUSDT")
        return ",".join(symbols)

    @property
    def symbols(self) -> list[str]:
        return self.crypto_symbols.split(",")


class Project1PostgresSettings(BaseSettings):
    """PROJECT1_POSTGRES_* variables: Project 1's database, which the lakehouse only reads.

    Read when connecting, so importing a module never needs the database.
    """

    model_config = SettingsConfigDict(env_prefix="PROJECT1_POSTGRES_", env_file=ENV_FILE, extra="ignore")

    host: str = "localhost"
    port: int = Field(5432, ge=1, le=65535)
    db: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password: str = Field(min_length=1)

    def connect_args(self) -> dict[str, str | int]:
        return {"host": self.host, "port": self.port, "dbname": self.db, "user": self.user, "password": self.password}
