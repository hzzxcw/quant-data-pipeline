import dagster as dg

from doris_integration.defs.volume_monitoring.detector import detect_volume_anomalies
from doris_integration.defs.volume_monitoring.models import Severity
from doris_integration.defs.volume_monitoring.resources import VolumeRecorderResource
from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)


def make_volume_check(
    asset,
    asset_key_str: str,
    min_rows: int = 0,
    ema_span: int = 14,
    k_sigma: float = 2.0,
    blocking: bool = False,
    description: str = "",
) -> dg.AssetChecksDefinition:
    """Factory that creates a volume anomaly asset check for a given asset.

    Args:
        asset: The Dagster asset to attach the check to.
        asset_key_str: String key of the monitored asset (e.g. "daily_stock_quotes").
        min_rows: Minimum row count threshold; fails check if latest volume is below this.
        ema_span: Span for exponential moving average calculation.
        k_sigma: Number of standard deviations for drop detection threshold.
        blocking: Whether the check blocks downstream execution on failure.
        description: Custom description; defaults to auto-generated message.

    Returns:
        An AssetChecksDefinition that can be registered in definitions.py.
    """
    check_name = f"volume_anomaly_{asset_key_str.replace('/', '_')}"

    @dg.asset_check(
        asset=asset,
        name=check_name,
        blocking=blocking,
        description=description or f"Volume anomaly check for {asset_key_str}",
    )
    def _check(
        context: dg.AssetCheckExecutionContext,
        volume_recorder: VolumeRecorderResource,
        trading_calendar: TradingCalendarResource,
    ) -> dg.AssetCheckResult:
        try:
            history = volume_recorder.get_volume_history(asset_key_str, days=90)
        except Exception as e:
            return dg.AssetCheckResult(
                passed=True,
                severity=dg.AssetCheckSeverity.WARN,
                description=f"Could not fetch volume history for {asset_key_str}: {e}",
            )

        if history.empty or len(history) < 14:
            return dg.AssetCheckResult(
                passed=True,
                severity=dg.AssetCheckSeverity.WARN,
                description=f"Insufficient history for {asset_key_str} ({len(history)} rows)",
            )

        if (
            min_rows > 0
            and len(history) > 0
            and history["row_count"].iloc[-1] < min_rows
        ):
            return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                description=f"Row count {history['row_count'].iloc[-1]} below minimum {min_rows}",
                metadata={
                    "observed_count": dg.MetadataValue.int(
                        int(history["row_count"].iloc[-1])
                    ),
                    "min_rows": dg.MetadataValue.int(min_rows),
                },
            )

        results = detect_volume_anomalies(
            history,
            is_trading_day_fn=trading_calendar.is_trading_day,
            ema_span=ema_span,
            k_sigma=k_sigma,
        )

        unsuppressed = [
            r for r in results if not r.suppressed and r.asset_key == asset_key_str
        ]

        if not unsuppressed:
            return dg.AssetCheckResult(
                passed=True,
                description=f"No volume anomalies detected for {asset_key_str}",
            )

        worst = max(unsuppressed, key=lambda r: r.severity.value)
        severity_map = {
            Severity.WARNING: dg.AssetCheckSeverity.WARN,
            Severity.ERROR: dg.AssetCheckSeverity.ERROR,
            Severity.CRITICAL: dg.AssetCheckSeverity.ERROR,
        }

        metadata = {
            "observed_count": dg.MetadataValue.int(worst.observed_count),
            "expected_count": dg.MetadataValue.float(worst.expected_count),
            "deviation_pct": dg.MetadataValue.float(worst.deviation_pct),
            "anomaly_count": dg.MetadataValue.int(len(unsuppressed)),
            "worst_severity": dg.MetadataValue.text(worst.severity.value),
        }

        return dg.AssetCheckResult(
            passed=worst.passed,
            severity=severity_map.get(worst.severity, dg.AssetCheckSeverity.WARN),
            description=f"Volume anomaly: {worst.reason} (deviation: {worst.deviation_pct:.1f}%)",
            metadata=metadata,
        )

    return _check
