from pathlib import Path

from dagster_dbt import DbtCliResource

DBT_PROJECT_DIR = Path(__file__).parent / "dbt_project"


def get_dbt_resource() -> DbtCliResource:
    return DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    )
