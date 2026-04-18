import json
from datetime import date

import dagster as dg
import pandas as pd

from doris_integration.defs.volume_monitoring.detector import detect_volume_anomalies
from doris_integration.defs.volume_monitoring.models import Severity
from doris_integration.defs.volume_monitoring.resources import (
    VolumeRecorderResource,
    DEFAULT_MONITORED_ASSETS,
    DingTalkAlertResource,
    WebhookAlertResource,
)
from doris_integration.defs.volume_monitoring.trading_calendar import (
    TradingCalendarResource,
)


@dg.asset(
    group_name="volume_monitoring",
    description="Detect volume anomalies across monitored assets. Only detects DROPS, aggressively suppresses false positives.",
)
def volume_anomaly_detection(
    context: dg.AssetExecutionContext,
    volume_recorder: VolumeRecorderResource,
    trading_calendar: TradingCalendarResource,
    # Default no-op instances allow running without alert config;
    # Dagster runtime injects configured resources from definitions.py
    dingtalk_alert: DingTalkAlertResource = DingTalkAlertResource(),
    webhook_alert: WebhookAlertResource = WebhookAlertResource(),
) -> dg.MaterializeResult:
    all_results = []
    total_anomalies = 0
    suppressed_count = 0
    severity_counts: dict[str, int] = {"WARNING": 0, "ERROR": 0, "CRITICAL": 0}

    for asset_key in DEFAULT_MONITORED_ASSETS:
        context.log.info(f"Checking volume for {asset_key}")
        try:
            history = volume_recorder.get_volume_history(asset_key, days=90)
        except Exception:
            context.log.exception(f"Could not fetch history for {asset_key}, skipping")
            continue

        if history.empty or len(history) < 14:
            context.log.info(
                f"Insufficient history for {asset_key} ({len(history)} rows), skipping"
            )
            continue

        results = detect_volume_anomalies(
            history,
            is_trading_day_fn=trading_calendar.is_trading_day,
        )

        for r in results:
            if not r.suppressed:
                total_anomalies += 1
                severity_counts[r.severity.value] += 1
            else:
                suppressed_count += 1

        all_results.extend(results)

    unsuppressed_results = [r for r in all_results if not r.suppressed]

    report_lines = [
        "# Volume Anomaly Detection Report",
        f"**Total anomalies (unsuppressed)**: {total_anomalies}",
        f"**Suppressed (false positives)**: {suppressed_count}",
        f"- WARNING: {severity_counts['WARNING']}",
        f"- ERROR: {severity_counts['ERROR']}",
        f"- CRITICAL: {severity_counts['CRITICAL']}",
        "",
    ]
    if unsuppressed_results:
        report_lines.append(
            "| Asset | Severity | Observed | Expected | Deviation% | Reason |"
        )
        report_lines.append("|---|---|---|---|---|---|")
        for r in unsuppressed_results:
            report_lines.append(
                f"| {r.asset_key} | {r.severity.value} | {r.observed_count} | "
                f"{r.expected_count:.0f} | {r.deviation_pct:.1f}% | {r.reason} |"
            )

    report_content = "\n".join(report_lines)

    anomaly_details = [
        {
            "asset_key": r.asset_key,
            "severity": r.severity.value,
            "observed": r.observed_count,
            "expected": r.expected_count,
            "deviation_pct": r.deviation_pct,
            "suppressed": r.suppressed,
            "reason": r.reason,
        }
        for r in all_results
    ]

    for r in unsuppressed_results:
        if r.severity in (Severity.ERROR, Severity.CRITICAL):
            alert_title = f"Volume Anomaly: {r.asset_key}"
            alert_content = (
                f"**Severity**: {r.severity.value}\n"
                f"**Asset**: {r.asset_key}\n"
                f"**Observed**: {r.observed_count} rows\n"
                f"**Expected**: {r.expected_count:.0f} rows\n"
                f"**Deviation**: {r.deviation_pct:.1f}%\n"
                f"**Reason**: {r.reason}"
            )
            dingtalk_alert.send(alert_title, alert_content, r.severity)
            webhook_alert.send(alert_title, alert_content, r.severity)

    return dg.MaterializeResult(
        metadata={
            "total_anomalies": dg.MetadataValue.int(total_anomalies),
            "suppressed_count": dg.MetadataValue.int(suppressed_count),
            "warning_count": dg.MetadataValue.int(severity_counts["WARNING"]),
            "error_count": dg.MetadataValue.int(severity_counts["ERROR"]),
            "critical_count": dg.MetadataValue.int(severity_counts["CRITICAL"]),
            "report": dg.MetadataValue.md(report_content),
            "anomaly_details": dg.MetadataValue.json(
                json.dumps(anomaly_details, default=str)
            ),
        }
    )
