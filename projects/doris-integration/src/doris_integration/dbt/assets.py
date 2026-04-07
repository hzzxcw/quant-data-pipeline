from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parent / "dbt_project"


@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def doris_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
