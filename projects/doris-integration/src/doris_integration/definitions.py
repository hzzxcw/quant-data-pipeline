from pathlib import Path

import dagster as dg

from doris_integration.defs.approach1_mysql.resources import (
    DorisIOManager,
    DorisResource,
)
from doris_integration.dbt.assets import doris_dbt_assets
from doris_integration.dbt.resources import get_dbt_resource


@dg.definitions
def defs() -> dg.Definitions:
    base = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)

    resource_defs = dg.Definitions(
        assets=[doris_dbt_assets],
        resources={
            "doris_resource": DorisResource(
                host=dg.EnvVar("DORIS_HOST"),
                user=dg.EnvVar("DORIS_USER"),
                password=dg.EnvVar("DORIS_PASSWORD"),
                database=dg.EnvVar("DORIS_DATABASE"),
            ),
            "doris": DorisIOManager(
                host=dg.EnvVar("DORIS_HOST"),
                user=dg.EnvVar("DORIS_USER"),
                password=dg.EnvVar("DORIS_PASSWORD"),
                database=dg.EnvVar("DORIS_DATABASE"),
            ),
            "dbt": get_dbt_resource(),
        },
    )

    return dg.Definitions.merge(base, resource_defs)
