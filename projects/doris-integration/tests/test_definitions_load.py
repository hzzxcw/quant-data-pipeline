from datetime import date

import dagster as dg

from doris_integration.defs.approach1_mysql.resources import DorisResource
from doris_integration.defs.volume_monitoring.resources import (
    DingTalkAlertResource,
    VolumeRecorderResource,
    WebhookAlertResource,
    volume_recorder_sensor,
)
from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)


class TestResourcesRegistered:
    def test_volume_recorder_resource_can_be_created(self) -> None:
        doris = DorisResource(
            host="test", port=9030, user="test", password="test", database="test"
        )
        resource = VolumeRecorderResource(doris=doris)
        assert resource is not None
        assert resource.doris is doris

    def test_trading_calendar_resource_can_be_created(self) -> None:
        resource = TradingCalendarResource(
            holiday_dates="2026-01-01,2026-10-01",
            makeup_dates="2026-02-08",
        )
        assert resource is not None
        assert date(2026, 1, 1) in resource.holidays
        assert date(2026, 2, 8) in resource.makeup_trading_days

    def test_volume_recorder_sensor_has_correct_name(self) -> None:
        assert volume_recorder_sensor.name == "volume_recorder_sensor"
        assert volume_recorder_sensor.minimum_interval_seconds == 300

    def test_trading_calendar_2026_holidays_complete(self) -> None:
        resource = TradingCalendarResource(
            holiday_dates=(
                "2026-01-01,2026-01-28,2026-01-29,2026-01-30,2026-01-31,"
                "2026-02-01,2026-02-02,2026-02-03,2026-04-04,2026-04-05,"
                "2026-04-06,2026-05-01,2026-05-02,2026-05-03,2026-05-04,"
                "2026-05-05,2026-06-19,2026-06-20,2026-09-25,"
                "2026-10-01,2026-10-02,2026-10-03,2026-10-04,2026-10-05,"
                "2026-10-06,2026-10-07"
            ),
            makeup_dates="2026-02-08,2026-02-14,2026-09-27,2026-10-10",
        )
        assert not resource.is_trading_day(date(2026, 1, 1))
        assert not resource.is_trading_day(date(2026, 2, 2))
        assert resource.is_trading_day(date(2026, 2, 8))
        assert not resource.is_trading_day(date(2026, 10, 1))
        assert resource.is_trading_day(date(2026, 10, 10))

    def test_dingtalk_alert_resource_defaults_to_mock(self) -> None:
        resource = DingTalkAlertResource()
        assert resource.webhook_url == ""
        assert resource.mention_all is False

    def test_webhook_alert_resource_defaults_to_mock(self) -> None:
        resource = WebhookAlertResource()
        assert resource.webhook_url == ""
        assert resource.headers == {}
