"""Every dbt model becomes a Dagster asset, and every dbt test becomes an asset check."""

import json
import os
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, BackfillPolicy
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from dagster_project.crypto.partitions import crypto_partitions, partition_days

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# profiles.yml and _sources.yml default to paths relative to dbt_project/.
# Absolute paths make the dbt runs independent of the folder Dagster starts in.
os.environ.setdefault("LAKEHOUSE_DATA_DIR", str(PROJECT_ROOT / "data"))
os.environ.setdefault("LAKEHOUSE_DUCKDB_PATH", str(PROJECT_ROOT / "data" / "warehouse" / "lakehouse.duckdb"))
# dagster-dbt calls `dbt` by name. Put the venv's scripts folder first on PATH so it
# finds the dbt installed alongside this Python even when the venv isn't activated.
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", "")

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
# Under `dagster dev`, regenerate the manifest so model edits show up on reload.
dbt_project.prepare_if_dev()


# DuckDB allows one writer at a time, so every dbt run takes the single slot in this pool
# (limit set in dagster_home/dagster.yaml) and weather and crypto builds never collide.
WAREHOUSE_POOL = "duckdb_warehouse"


class DomainLayerTranslator(DagsterDbtTranslator):
    """Group dbt assets by domain and layer from their folder, e.g. crypto_marts."""

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        fqn = dbt_resource_props.get("fqn", [])
        # fqn looks like ["lakehouse", "marts", "crypto", "fct_trades"]
        return f"{fqn[2]}_{fqn[1]}" if len(fqn) > 3 else None


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="tag:weather",
    name="weather_dbt_assets",
    dagster_dbt_translator=DomainLayerTranslator(),
    pool=WAREHOUSE_POOL,
)
def weather_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    # `build` runs models and their tests in dependency order; Dagster passes
    # the selected subset when only some assets are materialized.
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="tag:crypto",
    name="crypto_dbt_assets",
    partitions_def=crypto_partitions,
    backfill_policy=BackfillPolicy.single_run(),
    dagster_dbt_translator=DomainLayerTranslator(),
    pool=WAREHOUSE_POOL,
)
def crypto_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    # The partition window (one day, or a whole backfill range in one run) becomes the
    # incremental models' date window; aggregates over them are rebuilt in full.
    first_day, last_day = partition_days(context)
    window = {"crypto_start_date": first_day.isoformat(), "crypto_end_date": last_day.isoformat()}
    yield from dbt.cli(["build", "--vars", json.dumps(window)], context=context).stream()
