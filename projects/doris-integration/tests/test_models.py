from doris_integration.defs.volume_monitoring.models import (
    AnomalyResult,
    AnomalyType,
    Severity,
    VolumeRecord,
)


class TestVolumeRecord:
    def test_to_insert_dict_with_all_fields(self):
        record = VolumeRecord(
            asset_key="daily_stock_quotes",
            timestamp="2026-04-18T00:00:00",
            row_count=5000,
            partition_key="2026-04-18",
            run_id="run-abc-123",
        )
        result = record.to_insert_dict()

        assert result == {
            "asset_key": "daily_stock_quotes",
            "timestamp": "2026-04-18T00:00:00",
            "row_count": 5000,
            "partition_key": "2026-04-18",
            "run_id": "run-abc-123",
        }

    def test_to_insert_dict_with_optional_fields_none(self):
        record = VolumeRecord(
            asset_key="quant_macro_data",
            timestamp="2026-04-18T00:00:00",
            row_count=100,
        )
        result = record.to_insert_dict()

        assert result["partition_key"] is None
        assert result["run_id"] is None
        assert result["row_count"] == 100

    def test_frozen_dataclass_immutable(self):
        record = VolumeRecord(
            asset_key="test",
            timestamp="2026-01-01T00:00:00",
            row_count=10,
        )
        import pytest

        with pytest.raises(AttributeError):
            record.row_count = 20


class TestAnomalyResult:
    def test_passed_when_suppressed(self):
        result = AnomalyResult(
            asset_key="test",
            anomaly_type=AnomalyType.VOLUME_DROP,
            severity=Severity.ERROR,
            observed_count=100,
            expected_count=1000.0,
            deviation_pct=90.0,
            is_trading_day=True,
            consecutive_confirmations=2,
            suppressed=True,
            reason="Non-trading day suppression",
        )
        assert result.passed is True

    def test_passed_when_warning_severity(self):
        result = AnomalyResult(
            asset_key="test",
            anomaly_type=AnomalyType.VOLUME_DROP,
            severity=Severity.WARNING,
            observed_count=750,
            expected_count=1000.0,
            deviation_pct=25.0,
            is_trading_day=True,
            consecutive_confirmations=1,
            suppressed=False,
            reason="Minor drop",
        )
        assert result.passed is True

    def test_failed_when_error_severity_not_suppressed(self):
        result = AnomalyResult(
            asset_key="test",
            anomaly_type=AnomalyType.VOLUME_DROP,
            severity=Severity.ERROR,
            observed_count=500,
            expected_count=1000.0,
            deviation_pct=50.0,
            is_trading_day=True,
            consecutive_confirmations=2,
            suppressed=False,
            reason="Significant drop",
        )
        assert result.passed is False

    def test_failed_when_critical_severity_not_suppressed(self):
        result = AnomalyResult(
            asset_key="test",
            anomaly_type=AnomalyType.VOLUME_DROP,
            severity=Severity.CRITICAL,
            observed_count=200,
            expected_count=1000.0,
            deviation_pct=80.0,
            is_trading_day=True,
            consecutive_confirmations=3,
            suppressed=False,
            reason="Severe drop",
        )
        assert result.passed is False


class TestSeverityEnum:
    def test_enum_values(self):
        assert Severity.WARNING.value == "WARNING"
        assert Severity.ERROR.value == "ERROR"
        assert Severity.CRITICAL.value == "CRITICAL"

    def test_enum_members(self):
        members = list(Severity)
        assert len(members) == 3
        assert Severity.WARNING in members
        assert Severity.ERROR in members
        assert Severity.CRITICAL in members


class TestAnomalyTypeEnum:
    def test_volume_drop_value(self):
        assert AnomalyType.VOLUME_DROP.value == "VOLUME_DROP"
