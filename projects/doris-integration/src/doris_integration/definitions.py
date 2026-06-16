from pathlib import Path

import dagster as dg

from doris_integration.defs.approach1_mysql.resources import (
    DorisIOManager,
    DorisResource,
)
from doris_integration.defs.volume_monitoring.resources import (
    DingTalkAlertResource,
    VolumeRecorderResource,
    WebhookAlertResource,
)
from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)
from doris_integration.dbt.assets import doris_dbt_assets
from doris_integration.dbt.resources import get_dbt_resource

_DORIS_HOST = dg.EnvVar("DORIS_HOST")
_DORIS_USER = dg.EnvVar("DORIS_USER")
_DORIS_PASSWORD = dg.EnvVar("DORIS_PASSWORD")
_DORIS_DATABASE = dg.EnvVar("DORIS_DATABASE")

_STOCK_2026_HOLIDAYS = (
    "2026-01-01,2026-01-28,2026-01-29,2026-01-30,2026-01-31,2026-02-01,"
    "2026-02-02,2026-02-03,2026-04-04,2026-04-05,2026-04-06,"
    "2026-05-01,2026-05-02,2026-05-03,2026-05-04,2026-05-05,"
    "2026-06-19,2026-06-20,2026-09-25,"
    "2026-10-01,2026-10-02,2026-10-03,2026-10-04,2026-10-05,2026-10-06,2026-10-07"
)

_STOCK_2026_MAKEUP_DAYS = "2026-02-08,2026-02-14,2026-09-27,2026-10-10"


@dg.definitions
def defs() -> dg.Definitions:
    base = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)

    doris_resource = DorisResource(
        host=_DORIS_HOST,
        user=_DORIS_USER,
        password=_DORIS_PASSWORD,
        database=_DORIS_DATABASE,
    )

    resource_defs = dg.Definitions(
        assets=[doris_dbt_assets],
        resources={
            "doris_resource": doris_resource,
            "doris": DorisIOManager(
                host=_DORIS_HOST,
                user=_DORIS_USER,
                password=_DORIS_PASSWORD,
                database=_DORIS_DATABASE,
            ),
            "dbt": get_dbt_resource(),
            "volume_recorder": VolumeRecorderResource(doris=doris_resource),
            "trading_calendar": TradingCalendarResource(
                holiday_dates=_STOCK_2026_HOLIDAYS,
                makeup_dates=_STOCK_2026_MAKEUP_DAYS,
            ),
            "dingtalk_alert": DingTalkAlertResource(),
            "webhook_alert": WebhookAlertResource(),
        },
    )

    return dg.Definitions.merge(base, resource_defs)
