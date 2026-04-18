from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from doris_integration.defs.approach1_mysql.resources import DorisResource
from doris_integration.defs.volume_monitoring.models import VolumeRecord
from doris_integration.defs.volume_monitoring.resources import (
    VolumeRecorderResource,
    VolumeRecorderSensorConfig,
    extract_volume_events,
)

_DORIS_TEST_KWARGS = dict(
    host="test-host",
    port=9030,
    user="test-user",
    password="test-pass",
    database="test-db",
)


class TestVolumeRecorderResource:
    @patch.object(DorisResource, "insert_dataframe")
    def test_record_volume_inserts_dataframe(self, mock_insert: MagicMock) -> None:
        doris = DorisResource(**_DORIS_TEST_KWARGS)
        resource = VolumeRecorderResource(doris=doris)

        record = VolumeRecord(
            asset_key="daily_stock_quotes",
            timestamp="2026-04-18T10:00:00",
            row_count=5000,
            partition_key="2026-04-18",
            run_id="run-123",
        )
        resource.record_volume(record)

        mock_insert.assert_called_once()
        call_args = mock_insert.call_args
        assert call_args[0][0] == "volume_monitoring.asset_volume_history"
        df = call_args[0][1]
        assert len(df) == 1
        assert df.iloc[0]["asset_key"] == "daily_stock_quotes"
        assert df.iloc[0]["row_count"] == 5000

    @patch.object(DorisResource, "insert_dataframe")
    def test_record_volume_with_null_optional_fields(
        self, mock_insert: MagicMock
    ) -> None:
        doris = DorisResource(**_DORIS_TEST_KWARGS)
        resource = VolumeRecorderResource(doris=doris)

        record = VolumeRecord(
            asset_key="quant_macro_data",
            timestamp="2026-04-18T10:00:00",
            row_count=100,
        )
        resource.record_volume(record)

        mock_insert.assert_called_once()
        df = mock_insert.call_args[0][1]
        assert df.iloc[0]["row_count"] == 100

    @patch.object(DorisResource, "fetch_dataframe")
    def test_get_volume_history_queries_doris(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = pd.DataFrame(
            {"asset_key": ["test"], "row_count": [100]}
        )

        doris = DorisResource(**_DORIS_TEST_KWARGS)
        resource = VolumeRecorderResource(doris=doris)
        resource.get_volume_history("test", days=30)

        mock_fetch.assert_called_once()
        call_args = mock_fetch.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        assert "asset_key = %(asset_key)s" in query
        assert "INTERVAL 30 DAY" in query
        assert params["asset_key"] == "test"

    @patch.object(DorisResource, "fetch_dataframe")
    def test_get_volume_history_with_partition_key(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = pd.DataFrame()

        doris = DorisResource(**_DORIS_TEST_KWARGS)
        resource = VolumeRecorderResource(doris=doris)
        resource.get_volume_history("test", partition_key="2026-01-01")

        call_args = mock_fetch.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        assert "partition_key = %(partition_key)s" in query
        assert params["partition_key"] == "2026-01-01"


class TestExtractVolumeEvents:
    def test_extracts_row_count_from_metadata(self) -> None:
        metadata = {"row_count": dg.MetadataValue.int(5000)}
        result = extract_volume_events(
            asset_key_str="daily_stock_quotes",
            metadata=metadata,
            timestamp=datetime(2026, 4, 18, 10, 0, 0),
            run_id="run-abc-123",
        )

        assert result is not None
        assert result.asset_key == "daily_stock_quotes"
        assert result.row_count == 5000
        assert result.run_id == "run-abc-123"
        assert result.timestamp == "2026-04-18T10:00:00"

    def test_returns_none_when_no_row_count(self) -> None:
        metadata = {"other_key": dg.MetadataValue.text("value")}
        result = extract_volume_events(
            asset_key_str="daily_stock_quotes",
            metadata=metadata,
            timestamp=datetime(2026, 4, 18, 10, 0, 0),
            run_id="run-abc-123",
        )

        assert result is None

    def test_returns_volume_record_with_all_fields(self) -> None:
        metadata = {"row_count": dg.MetadataValue.int(3000)}
        result = extract_volume_events(
            asset_key_str="stock_quotes_asset",
            metadata=metadata,
            timestamp=datetime(2026, 3, 15, 8, 30, 0),
            run_id="run-xyz-456",
        )

        assert result is not None
        assert result.asset_key == "stock_quotes_asset"
        assert result.row_count == 3000


class TestVolumeRecorderSensorConfig:
    def test_default_monitored_assets(self) -> None:
        config = VolumeRecorderSensorConfig()
        assert "daily_stock_quotes" in config.monitored_assets
        assert "quant_macro_data" in config.monitored_assets
        assert "stock_quotes_asset" in config.monitored_assets

    def test_custom_monitored_assets(self) -> None:
        config = VolumeRecorderSensorConfig(monitored_assets=["my_asset"])
        assert config.monitored_assets == ["my_asset"]
