"""Builds throwaway warehouses from the sample data in tests/data, so tests never touch data/."""

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
SAMPLE_DATA_DIR = Path(__file__).resolve().parent / "data"
# The dbt installed alongside this Python (the venv locally, the runner's Python in CI).
DBT = shutil.which("dbt", path=str(Path(sys.executable).parent)) or "dbt"


@dataclass
class Warehouse:
    data_dir: Path  # a copy of tests/data: raw_csv/ and raw_json/
    db_path: Path

    def dbt(self, *args: str) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "LAKEHOUSE_DATA_DIR": str(self.data_dir),
            "LAKEHOUSE_DUCKDB_PATH": str(self.db_path),
            "DBT_PROFILES_DIR": str(DBT_PROJECT_DIR),
            "PYTHONIOENCODING": "utf-8",
        }
        result = subprocess.run(
            [DBT, *args], cwd=DBT_PROJECT_DIR, env=env, capture_output=True, text=True, encoding="utf-8"
        )
        assert result.returncode == 0, f"dbt {' '.join(args)} failed:\n{result.stdout[-4000:]}"
        return result

    def query(self, sql: str) -> list[tuple]:
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            return con.execute(sql).fetchall()

    def checksum(self, table: str, exclude: tuple[str, ...] = ()) -> tuple[int, str]:
        """Row count plus an order-independent hash of every row (minus the excluded columns)."""
        columns = f"* exclude ({', '.join(exclude)})" if exclude else "*"
        sql = (
            "select count(*), md5(coalesce(string_agg(cast(t as varchar), '|' order by cast(t as varchar)), '')) "
            f"from (select {columns} from {table}) as t"
        )
        count, digest = self.query(sql)[0]
        return count, digest


def build_warehouse(root: Path) -> Warehouse:
    data_dir = root / "data"
    shutil.copytree(SAMPLE_DATA_DIR, data_dir)
    warehouse = Warehouse(data_dir=data_dir, db_path=root / "lakehouse.duckdb")
    if not (DBT_PROJECT_DIR / "dbt_packages" / "dbt_utils").exists():
        warehouse.dbt("deps")
    warehouse.dbt("build")
    return warehouse


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Warehouse:
    """One warehouse built from the sample data, shared by read-only tests."""
    return build_warehouse(tmp_path_factory.mktemp("warehouse"))
