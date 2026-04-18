from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from doris_integration.defs.volume_monitoring.detector import (
    AnomalyResult,
    AnomalyType,
    Severity,
    check_absolute_significance,
    check_relative_significance,
    classify_severity,
    compute_ema,
    compute_ema_std,
    detect_volume_anomalies,
    is_volume_drop,
)
from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)


def _always_trading_day(d: date) -> bool:
    return True


def _never_trading_day(d: date) -> bool:
    return False


def _make_history_df(
    rows: int,
    base_count: int = 5000,
    noise_std: float = 100.0,
    start_date: str = "2026-01-15",
    drop_at: int | None = None,
    drop_pct: float = 0.5,
) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    counts = rng.normal(base_count, noise_std, rows).astype(int)
    counts = np.maximum(counts, 0)
    if drop_at is not None and drop_at < rows:
        counts[drop_at] = int(base_count * (1 - drop_pct))
    dates = pd.date_range(start=start_date, periods=rows, freq="D")
    return pd.DataFrame(
        {
            "asset_key": ["test_asset"] * rows,
            "ts": dates,
            "row_count": counts,
        }
    )


class TestComputeEma:
    def test_returns_correct_length(self) -> None:
        series = pd.Series(range(100))
        result = compute_ema(series, span=14)
        assert len(result) == 100

    def test_ema_smoothes_data(self) -> None:
        series = pd.Series([100] * 10 + [200] * 10)
        ema = compute_ema(series, span=5)
        assert ema.iloc[-1] > series.mean()
        assert ema.iloc[-1] < series.max()

    def test_ema_first_value_near_first_data_point(self) -> None:
        series = pd.Series(range(20, 30))
        ema = compute_ema(series, span=14)
        assert abs(ema.iloc[0] - 20) < 2.0

    def test_ema_responds_to_recent_data(self) -> None:
        stable = pd.Series([1000] * 30)
        spike = pd.Series([1000] * 20 + [2000] * 10)
        ema_stable = compute_ema(stable, span=14)
        ema_spike = compute_ema(spike, span=14)
        assert ema_spike.iloc[-1] > ema_stable.iloc[-1]


class TestComputeEmaStd:
    def test_returns_correct_length(self) -> None:
        series = pd.Series(range(100))
        result = compute_ema_std(series, span=14)
        assert len(result) == 100

    def test_std_positive_for_varying_data(self) -> None:
        rng = np.random.default_rng(42)
        series = pd.Series(rng.normal(100, 10, 50))
        std = compute_ema_std(series, span=14)
        std_valid = std.dropna()
        assert (std_valid > 0).all()

    def test_std_near_zero_for_constant_data(self) -> None:
        series = pd.Series([500] * 30)
        std = compute_ema_std(series, span=14)
        assert std.iloc[-1] < 1.0


class TestClassifySeverity:
    def test_warning_range_25_to_40(self) -> None:
        assert classify_severity(30.0, 700, 1000) == Severity.WARNING
        assert classify_severity(25.0, 750, 1000) == Severity.WARNING
        assert classify_severity(39.9, 601, 1000) == Severity.WARNING

    def test_error_range_40_to_60(self) -> None:
        assert classify_severity(50.0, 500, 1000) == Severity.ERROR
        assert classify_severity(40.0, 600, 1000) == Severity.ERROR
        assert classify_severity(59.9, 401, 1000) == Severity.ERROR

    def test_critical_above_60(self) -> None:
        assert classify_severity(70.0, 300, 1000) == Severity.CRITICAL
        assert classify_severity(60.0, 400, 1000) == Severity.CRITICAL
        assert classify_severity(99.9, 1, 1000) == Severity.CRITICAL

    def test_below_warning_threshold_returns_warning(self) -> None:
        assert classify_severity(10.0, 900, 1000) == Severity.WARNING


class TestIsVolumeDrop:
    def test_true_when_below_threshold(self) -> None:
        assert is_volume_drop(500, 1000, 100) is True

    def test_false_when_above_threshold(self) -> None:
        assert is_volume_drop(850, 1000, 100) is False

    def test_never_flags_spikes(self) -> None:
        assert is_volume_drop(1500, 1000, 100) is False

    def test_with_zero_std(self) -> None:
        assert is_volume_drop(999, 1000, 0) is False

    def test_with_custom_k(self) -> None:
        assert is_volume_drop(940, 1000, 50, k=1) is True
        assert is_volume_drop(940, 1000, 50, k=3) is False


class TestAbsoluteSignificance:
    def test_filter_noise_below_threshold(self) -> None:
        assert check_absolute_significance(49980, 50000, threshold_pct=0.1) is False

    def test_pass_above_threshold(self) -> None:
        assert check_absolute_significance(100, 50000, threshold_pct=0.1) is True

    def test_edge_case_at_threshold(self) -> None:
        assert check_absolute_significance(50, 50000, threshold_pct=0.1) is True


class TestRelativeSignificance:
    def test_filter_minor_drop_below_threshold(self) -> None:
        assert check_relative_significance(950, 1000, threshold_pct=25.0) is False

    def test_pass_above_threshold(self) -> None:
        assert check_relative_significance(600, 1000, threshold_pct=25.0) is True

    def test_edge_case_at_threshold(self) -> None:
        assert check_relative_significance(750, 1000, threshold_pct=25.0) is True

    def test_with_zero_expected(self) -> None:
        assert check_relative_significance(0, 0, threshold_pct=25.0) is False


class TestDetectVolumeAnomalies:
    def test_no_anomalies_on_normal_data(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        df = _make_history_df(90, base_count=5000, noise_std=100)
        results = detect_volume_anomalies(df, calendar.is_trading_day)
        unsuppressed = [r for r in results if not r.suppressed]
        assert len(unsuppressed) == 0

    def test_only_drops_not_spikes(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        rng = np.random.default_rng(42)
        counts = list(rng.normal(5000, 100, 50).astype(int))
        counts.append(100000)  # spike
        counts.extend(list(rng.normal(5000, 100, 39).astype(int)))
        dates = pd.date_range("2026-01-15", periods=len(counts), freq="D")
        df = pd.DataFrame(
            {
                "asset_key": ["test_asset"] * len(counts),
                "ts": dates,
                "row_count": counts,
            }
        )
        results = detect_volume_anomalies(df, calendar.is_trading_day)
        for r in results:
            assert r.anomaly_type == AnomalyType.VOLUME_DROP

    def test_trading_calendar_suppression(self) -> None:
        non_trading = date(2026, 4, 5)
        calendar = TradingCalendarResource(
            holiday_dates="2026-04-05",
            makeup_dates="",
        )
        df = pd.DataFrame(
            {
                "asset_key": ["test_asset"] * 30,
                "ts": pd.date_range("2026-03-01", periods=30, freq="D"),
                "row_count": [5000] * 30,
            }
        )
        df.loc[df["ts"] == pd.Timestamp(non_trading), "row_count"] = 50
        results = detect_volume_anomalies(df, calendar.is_trading_day)
        for r in results:
            if r.observed_count == 50:
                assert r.suppressed is True

    def test_consecutive_confirmation_required(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        df = _make_history_df(30, base_count=5000, noise_std=50)
        df.iloc[-1, df.columns.get_loc("row_count")] = 100
        results = detect_volume_anomalies(df, calendar.is_trading_day)
        single_drop_results = [
            r for r in results if not r.suppressed and r.observed_count == 100
        ]
        assert len(single_drop_results) == 0

    def test_insufficient_history_suppressed(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        df = pd.DataFrame(
            {
                "asset_key": ["test_asset"] * 5,
                "ts": pd.date_range("2026-01-15", periods=5, freq="D"),
                "row_count": [10, 5000, 5000, 5000, 5000],
            }
        )
        results = detect_volume_anomalies(
            df, calendar.is_trading_day, min_history_days=14
        )
        assert all(r.suppressed for r in results)

    def test_low_cv_suppressed(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        df = pd.DataFrame(
            {
                "asset_key": ["test_asset"] * 30,
                "ts": pd.date_range("2026-01-15", periods=30, freq="D"),
                "row_count": [5000] * 30,
            }
        )
        df.iloc[-1, df.columns.get_loc("row_count")] = 4000
        results = detect_volume_anomalies(
            df, calendar.is_trading_day, min_coefficient_of_variation=0.01
        )
        low_cv_results = [
            r for r in results if r.observed_count == 4000 and not r.suppressed
        ]
        assert len(low_cv_results) == 0

    def test_full_pipeline_detects_severe_drop(self) -> None:
        calendar = TradingCalendarResource(holiday_dates="", makeup_dates="")
        rng = np.random.default_rng(42)
        base = list(rng.normal(5000, 100, 30).astype(int))
        base[-1] = 200
        dates = pd.date_range("2026-01-15", periods=len(base), freq="D")
        df = pd.DataFrame(
            {
                "asset_key": ["test_asset"] * len(base),
                "ts": dates,
                "row_count": base,
            }
        )
        results = detect_volume_anomalies(
            df, calendar.is_trading_day, consecutive_confirmations=1
        )
        unsuppressed = [r for r in results if not r.suppressed]
        assert len(unsuppressed) >= 1
        severe = [r for r in unsuppressed if r.observed_count == 200]
        assert len(severe) >= 1
        assert severe[0].severity in (Severity.ERROR, Severity.CRITICAL)
