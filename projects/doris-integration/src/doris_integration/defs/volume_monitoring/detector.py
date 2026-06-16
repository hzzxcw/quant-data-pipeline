from __future__ import annotations

from datetime import date
from typing import Callable

import numpy as np
import pandas as pd

from doris_integration.defs.volume_monitoring.models import (
    AnomalyResult,
    AnomalyType,
    Severity,
)


def compute_ema(series: pd.Series, span: int = 14) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def compute_ema_std(series: pd.Series, span: int = 14) -> pd.Series:
    return series.ewm(span=span, adjust=False).std()


def classify_severity(deviation_pct: float, observed: int, expected: float) -> Severity:
    if deviation_pct >= 60:
        return Severity.CRITICAL
    if deviation_pct >= 40:
        return Severity.ERROR
    if deviation_pct >= 25:
        return Severity.WARNING
    return Severity.WARNING


def is_volume_drop(observed: int, expected: float, std: float, k: float = 2.0) -> bool:
    if std <= 0:
        return False
    return observed < expected - k * std


def check_absolute_significance(
    observed: int, peak_volume: int, threshold_pct: float = 0.1
) -> bool:
    if peak_volume <= 0:
        return False
    drop_amount = peak_volume - observed
    return (drop_amount / peak_volume * 100) >= threshold_pct


def check_relative_significance(
    observed: int, expected: float, threshold_pct: float = 25.0
) -> bool:
    if expected <= 0:
        return False
    deviation_pct = (expected - observed) / expected * 100
    return deviation_pct >= threshold_pct


def detect_volume_anomalies(
    history: pd.DataFrame,
    is_trading_day_fn: Callable[[date], bool],
    ema_span: int = 14,
    k_sigma: float = 2.0,
    relative_threshold_pct: float = 25.0,
    absolute_threshold_pct: float = 0.1,
    consecutive_confirmations: int = 2,
    min_history_days: int = 14,
    min_coefficient_of_variation: float = 0.05,
) -> list[AnomalyResult]:
    results: list[AnomalyResult] = []

    for asset_key, group in history.groupby("asset_key"):
        group = group.sort_values("ts").reset_index(drop=True)

        if len(group) < min_history_days:
            continue

        ema = compute_ema(group["row_count"], span=ema_span)
        ema_std = compute_ema_std(group["row_count"], span=ema_span)
        peak_volume = group["row_count"].max()
        cv = (
            group["row_count"].std() / group["row_count"].mean()
            if group["row_count"].mean() > 0
            else 0
        )

        for i in range(min_history_days, len(group)):
            row = group.iloc[i]
            observed = int(row["row_count"])
            expected = ema.iloc[i]
            std = ema_std.iloc[i]
            ts_date = pd.Timestamp(row["ts"]).date()
            trading_day = is_trading_day_fn(ts_date)

            if not is_volume_drop(observed, expected, std, k_sigma):
                continue

            drop_amount = expected - observed
            deviation_pct = (drop_amount / expected * 100) if expected > 0 else 0

            suppressed = False
            reason_parts: list[str] = []

            if not trading_day:
                suppressed = True
                reason_parts.append("non_trading_day")

            if not check_absolute_significance(
                observed, peak_volume, absolute_threshold_pct
            ):
                suppressed = True
                reason_parts.append("below_absolute_threshold")

            if not check_relative_significance(
                observed, expected, relative_threshold_pct
            ):
                suppressed = True
                reason_parts.append("below_relative_threshold")

            if cv < min_coefficient_of_variation:
                suppressed = True
                reason_parts.append("low_coefficient_of_variation")

            if not suppressed and consecutive_confirmations > 1:
                consecutive_drops = 0
                for j in range(
                    max(min_history_days, i - consecutive_confirmations + 1), i + 1
                ):
                    row_j = group.iloc[j]
                    ema_j = ema.iloc[j]
                    std_j = ema_std.iloc[j]
                    if is_volume_drop(int(row_j["row_count"]), ema_j, std_j, k_sigma):
                        consecutive_drops += 1
                if consecutive_drops < consecutive_confirmations:
                    suppressed = True
                    reason_parts.append("insufficient_consecutive_confirmations")

            severity = classify_severity(deviation_pct, observed, expected)

            results.append(
                AnomalyResult(
                    asset_key=str(asset_key),
                    anomaly_type=AnomalyType.VOLUME_DROP,
                    severity=severity,
                    observed_count=observed,
                    expected_count=expected,
                    deviation_pct=round(deviation_pct, 2),
                    is_trading_day=trading_day,
                    consecutive_confirmations=consecutive_confirmations,
                    suppressed=suppressed,
                    reason=", ".join(reason_parts)
                    if reason_parts
                    else "volume_drop_detected",
                )
            )

    return results
