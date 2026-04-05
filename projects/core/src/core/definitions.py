from pathlib import Path

from dagster import Definitions, load_from_defs_folder, definitions

from core.defs.resources import GreatExpectationsResource
from core.defs.check_report import MockNotificationResource


@definitions
def defs() -> Definitions:
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    resource_defs = Definitions(
        resources={
            "ge_resource": GreatExpectationsResource(),
            "notifier": MockNotificationResource(),
        },
    )
    return Definitions.merge(base, resource_defs)
