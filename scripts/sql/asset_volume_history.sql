CREATE DATABASE IF NOT EXISTS volume_monitoring;

CREATE TABLE IF NOT EXISTS volume_monitoring.asset_volume_history (
    asset_key      VARCHAR(255)  NOT NULL COMMENT 'Dagster asset key path',
    ts             DATETIME      NOT NULL COMMENT 'Materialization timestamp',
    row_count      BIGINT        NOT NULL COMMENT 'Number of rows materialized',
    partition_key  VARCHAR(128)  NULL     COMMENT 'Partition key',
    run_id         VARCHAR(128)  NULL     COMMENT 'Dagster run ID',
    insert_ts      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Record insert timestamp'
)
UNIQUE KEY (asset_key, ts, partition_key)
DISTRIBUTED BY HASH(asset_key) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true"
);