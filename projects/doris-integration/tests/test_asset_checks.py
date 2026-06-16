import dagster as dg
import numpy as np
import pandas as pd
import pytest

from doris_integration.defs.volume_monitoring.asset_checks import make_volume_check
from doris_integration.defs.volume_monitoring.detector import detect_volume_anomalies
from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)


_TRADING_CALENDAR = TradingCalendarResource(
    holiday_dates="2026-01-01",
    makeup_dates="",
)


class TestMakeVolumeCheck:
    def test_factory_creates_check_definition(self) -> None:
        check = make_volume_check(
            asset=dg.AssetKey(["test_asset"]),
            asset_key_str="test_asset",
        )
        assert check is not None
        assert isinstance(check, dg.AssetChecksDefinition)

    def test_factory_with_blocking_flag(self) -> None:
        check = make_volume_check(
            asset=dg.AssetKey(["test_asset"]),
            asset_key_str="test_asset",
            blocking=True,
        )
        assert check is not None

    def test_factory_with_custom_parameters(self) -> None:
        check = make_volume_check(
            asset=dg.AssetKey(["my_asset"]),
            asset_key_str="my_asset",
            min_rows=100,
            ema_span=7,
            k_sigma=3.0,
        )
        assert check is not None

    def test_factory_creates_unique_check_per_asset(self) -> None:
        check1 = make_volume_check(
            asset=dg.AssetKey(["asset_a"]),
            asset_key_str="asset_a",
        )
        check2 = make_volume_check(
            asset=dg.AssetKey(["asset_b"]),
            asset_key_str="asset_b",
        )
        assert check1 is not check2


class TestVolumeCheckLogic:
    def test_empty_history_returns_pass(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        df = pd.DataFrame({"asset_key": [], "ts": [], "row_count": []})
        results = detect_volume_anomalies(df, calendar.is_trading_day)
        assert len(results) == 0

    def test_stable_history_no_anomalies(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        rng = np.random.default_rng(42)
        counts = rng.normal(5000, 50, 30).astype(int)
        counts = np.maximum(counts, 0)
        dates = pd.date_range("2026-01-15", periods=30, freq="D")
        df = pd.DataFrame(
            {
                "asset_key": ["test_asset"] * 30,
                "ts": dates,
                "row_count": counts,
            }
        )
        results = detect_volume_anomalies(df, calendar.is_trading_day)
        unsuppressed = [r for r in results if not r.suppressed]
        assert len(unsuppressed) == 0

    def test_severe_drop_detected(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        rng = np.random.default_rng(42)
        counts = rng.normal(5000, 50, 29).astype(int).tolist()
        counts.append(100)
        dates = pd.date_range("2026-01-15", periods=30, freq="D")
        df = pd.DataFrame(
            {
                "asset_key": ["test_asset"] * 30,
                "ts": dates,
                "row_count": counts,
            }
        )
        results = detect_volume_anomalies(
            df, calendar.is_trading_day, consecutive_confirmations=1
        )
        unsuppressed = [
            r for r in results if not r.suppressed and r.observed_count == 100
        ]
        assert len(unsuppressed) >= 1
