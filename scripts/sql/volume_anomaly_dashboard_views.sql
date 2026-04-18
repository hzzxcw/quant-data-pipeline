CREATE DATABASE IF NOT EXISTS volume_monitoring;

CREATE VIEW IF NOT EXISTS volume_monitoring.v_volume_trend AS
SELECT
    asset_key,
    ts,
    row_count,
    partition_key,
    run_id
FROM volume_monitoring.asset_volume_history
WHERE ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
ORDER BY asset_key, ts;

CREATE VIEW IF NOT EXISTS volume_monitoring.v_anomaly_summary AS
SELECT
    asset_key,
    ts,
    row_count,
    run_id,
    CASE
        WHEN row_count < expected_ema * 0.4 THEN 'CRITICAL'
        WHEN row_count < expected_ema * 0.6 THEN 'ERROR'
        WHEN row_count < expected_ema * 0.75 THEN 'WARNING'
        ELSE 'OK'
    END AS volume_status
FROM (
    SELECT
        h.asset_key,
        h.ts,
        h.row_count,
        h.run_id,
        AVG(h.row_count) OVER (
            PARTITION BY h.asset_key
            ORDER BY h.ts
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS expected_ema
    FROM volume_monitoring.asset_volume_history h
    WHERE h.ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
) sub
ORDER BY asset_key, ts;

CREATE VIEW IF NOT EXISTS volume_monitoring.v_volume_comparison AS
SELECT
    asset_key,
    ts,
    row_count AS observed,
    ema_row_count AS expected,
    row_count - ema_row_count AS diff,
    CASE
        WHEN ema_row_count > 0 THEN ROUND((row_count - ema_row_count) / ema_row_count * 100, 2)
        ELSE NULL
    END AS deviation_pct
FROM (
    SELECT
        asset_key,
        ts,
        row_count,
        AVG(row_count) OVER (
            PARTITION BY asset_key
            ORDER BY ts
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS ema_row_count
    FROM volume_monitoring.asset_volume_history
    WHERE ts >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
) sub
ORDER BY asset_key, ts;