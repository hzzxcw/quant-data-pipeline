import logging
from datetime import datetime

import dagster as dg
import pandas as pd

from doris_integration.defs.approach1_mysql.resources import DorisResource
from doris_integration.defs.volume_monitoring.models import (
    AnomalyResult,
    Severity,
    VolumeRecord,
)

logger = logging.getLogger(__name__)

VOLUME_HISTORY_TABLE = "volume_monitoring.asset_volume_history"


class VolumeRecorderResource(dg.ConfigurableResource):
    """Records and retrieves asset volume history from Doris for anomaly detection.

    Wraps DorisResource to provide volume-specific query and insert operations
    against the asset_volume_history table.
    """

    doris: DorisResource

    def record_volume(self, record: VolumeRecord) -> None:
        df = pd.DataFrame([record.to_insert_dict()])
        self.doris.insert_dataframe(VOLUME_HISTORY_TABLE, df)

    def get_volume_history(
        self,
        asset_key: str,
        days: int = 90,
        partition_key: str | None = None,
    ) -> pd.DataFrame:
        base_query = f"""
            SELECT asset_key, ts, row_count, partition_key, run_id
            FROM {VOLUME_HISTORY_TABLE}
            WHERE asset_key = %(asset_key)s
            AND ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        """
        params: dict[str, str | int] = {"asset_key": asset_key}
        if partition_key:
            base_query += " AND partition_key = %(partition_key)s"
            params["partition_key"] = partition_key
        base_query += " ORDER BY ts ASC"
        return self.doris.fetch_dataframe(base_query, params)


DEFAULT_MONITORED_ASSETS: list[str] = [
    "daily_stock_quotes",
    "quant_macro_data",
    "industrial_value_added",
    "retail_sales",
    "stock_quotes_asset",
    "stock_daily_returns",
    "stock_summary",
]


class VolumeRecorderSensorConfig(dg.Config):
    monitored_assets: list[str] = DEFAULT_MONITORED_ASSETS


def extract_volume_events(
    asset_key_str: str,
    metadata: dict[str, dg.MetadataValue],
    timestamp: datetime,
    run_id: str,
) -> VolumeRecord | None:
    row_count_md = metadata.get("row_count")
    if row_count_md is None:
        return None

    try:
        row_count = int(row_count_md.value)
    except (ValueError, TypeError):
        logger.warning(
            f"Non-integer row_count metadata for {asset_key_str}: {row_count_md.value!r}"
        )
        return None

    return VolumeRecord(
        asset_key=asset_key_str,
        timestamp=timestamp.isoformat(),
        row_count=row_count,
        run_id=run_id,
    )


@dg.sensor(
    minimum_interval_seconds=300,
    description="Records asset materialization row_counts to Doris for volume anomaly detection.",
)
def volume_recorder_sensor(
    context: dg.SensorEvaluationContext,
    config: VolumeRecorderSensorConfig,
    volume_recorder: VolumeRecorderResource,
) -> dg.SensorResult:
    after = int(context.cursor) if context.cursor else 0
    recorded_count = 0
    new_cursor = after

    event_records = context.instance.get_event_records(
        after_cursor=after,
        limit=200,
    )

    for record in event_records:
        event_log_entry = record.event_log_entry
        new_cursor = record.storage_id

        if event_log_entry.event_type_value != "ASSET_MATERIALIZATION":
            continue

        materialization = event_log_entry.dagster_event.asset_materialization_data
        if materialization is None:
            continue

        asset_key_str = "/".join(event_log_entry.dagster_event.asset_key.path)

        if asset_key_str not in config.monitored_assets:
            continue

        volume_record = extract_volume_events(
            asset_key_str=asset_key_str,
            metadata=materialization.metadata,
            timestamp=event_log_entry.timestamp,
            run_id=str(event_log_entry.run_id),
        )
        if volume_record is None:
            continue

        volume_recorder.record_volume(volume_record)
        recorded_count += 1
        context.log.info(
            f"Recorded volume for {asset_key_str}: {volume_record.row_count} rows"
        )

    context.log.info(
        f"Volume recorder sensor: recorded {recorded_count} entries, new cursor={new_cursor}"
    )
    return dg.SensorResult(cursor=str(new_cursor))


class DingTalkAlertResource(dg.ConfigurableResource):
    """Sends volume anomaly alerts via DingTalk webhook.

    When webhook_url is empty, operates in mock mode (logs only).
    """

    webhook_url: str = ""
    mention_all: bool = False

    def send(self, title: str, content: str, severity: Severity) -> None:
        if not self.webhook_url:
            logger.info(f"[MOCK] DingTalk alert: [{severity.value}] {title}")
            logger.info(content)
            return

        import json
        import urllib.request

        at_mobiles = []
        if self.mention_all:
            at_mobiles = ["all"]

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"[{severity.value}] {title}",
                "text": content,
            },
            "at": {
                "atMobiles": at_mobiles,
                "isAtAll": self.mention_all,
            },
        }

        req = urllib.request.Request(
            self.webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as response:
                logger.info(f"DingTalk alert sent: {response.status}")
        except Exception:
            logger.exception("Failed to send DingTalk alert")
            raise


class WebhookAlertResource(dg.ConfigurableResource):
    """Sends volume anomaly alerts to a generic webhook endpoint.

    When webhook_url is empty, operates in mock mode (logs only).
    Supports custom headers for authentication (e.g. Bearer tokens).
    """

    webhook_url: str = ""
    headers: dict[str, str] = {}

    def send(self, title: str, content: str, severity: Severity) -> None:
        if not self.webhook_url:
            logger.info(f"[MOCK] Webhook alert: [{severity.value}] {title}")
            logger.info(content)
            return

        import json
        import urllib.request

        payload = {
            "title": title,
            "content": content,
            "severity": severity.value,
        }
        all_headers = {"Content-Type": "application/json", **self.headers}

        req = urllib.request.Request(
            self.webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers=all_headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as response:
                logger.info(f"Webhook alert sent: {response.status}")
        except Exception:
            logger.exception("Failed to send webhook alert")
            raise
