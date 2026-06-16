from __future__ import annotations

from unittest.mock import MagicMock, patch

import dagster as dg
import numpy as np
import pandas as pd

from doris_integration.defs.approach1_mysql.resources import DorisResource
from doris_integration.defs.volume_monitoring.resources import (
    DingTalkAlertResource,
    VolumeRecorderResource,
    WebhookAlertResource,
)
from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)
from doris_integration.defs.volume_monitoring.volume_anomaly_detection import (
    volume_anomaly_detection,
)

_DORIS_TEST_KWARGS = dict(
    host="test-host",
    port=9030,
    user="test-user",
    password="test-pass",
    database="test-db",
)

_TRADING_CALENDAR = TradingCalendarResource(
    holiday_dates="2026-01-01",
    makeup_dates="",
)


def _make_history_df(rows: int, base_count: int = 5000) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    counts = rng.normal(base_count, 100, rows).astype(int)
    counts = np.maximum(counts, 0)
    dates = pd.date_range("2026-01-15", periods=rows, freq="D")
    return pd.DataFrame(
        {
            "asset_key": ["test_asset"] * rows,
            "ts": dates,
            "row_count": counts,
        }
    )


class TestVolumeAnomalyDetectionAsset:
    def test_asset_is_registered(self) -> None:
        assert volume_anomaly_detection is not None
        assert hasattr(volume_anomaly_detection, "key")
        assert volume_anomaly_detection.key.path == ["volume_anomaly_detection"]

    @patch.object(DorisResource, "fetch_dataframe")
    def test_asset_with_normal_data_no_anomalies(self, mock_fetch: MagicMock) -> None:
        history_df = _make_history_df(30)
        mock_fetch.return_value = history_df

        doris = DorisResource(**_DORIS_TEST_KWARGS)
        volume_recorder = VolumeRecorderResource(doris=doris)

        with dg.build_op_context(
            resources={
                "volume_recorder": volume_recorder,
                "trading_calendar": _TRADING_CALENDAR,
                "dingtalk_alert": DingTalkAlertResource(),
                "webhook_alert": WebhookAlertResource(),
            }
        ) as context:
            result = volume_anomaly_detection(context)

        assert isinstance(result, dg.MaterializeResult)
        assert "total_anomalies" in result.metadata
        assert "suppressed_count" in result.metadata
        assert "warning_count" in result.metadata
        assert "error_count" in result.metadata
        assert "critical_count" in result.metadata
        assert "report" in result.metadata
