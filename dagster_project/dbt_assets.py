"""Every dbt model becomes a Dagster asset, and every dbt test becomes an asset check."""

import os
import sys
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

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


class LayerGroupTranslator(DagsterDbtTranslator):
    """Group dbt assets by layer (staging / intermediate / marts) from their folder."""

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        fqn = dbt_resource_props.get("fqn", [])
        # fqn looks like ["lakehouse", "marts", "weather", "dim_city"]
        return fqn[1] if len(fqn) > 2 else None


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=LayerGroupTranslator(),
)
def lakehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    # `build` runs models and their tests in dependency order; Dagster passes
    # the selected subset when only some assets are materialized.
    yield from dbt.cli(["build"], context=context).stream()
