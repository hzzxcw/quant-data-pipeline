CREATE DATABASE IF NOT EXISTS volume_monitoring;

CREATE TABLE IF NOT EXISTS volume_monitoring.trading_calendar (
    trade_date     DATE           NOT NULL COMMENT 'Trading date (YYYY-MM-DD)',
    is_trading_day  TINYINT       NOT NULL COMMENT '1=trading day, 0=non-trading day',
    reason         VARCHAR(64)    NULL     COMMENT 'Reason if non-trading (holiday name, weekend, etc.)',
    source         VARCHAR(32)    NOT NULL DEFAULT 'manual' COMMENT 'Data source',
    update_ts      DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Last update timestamp'
)
UNIQUE KEY (trade_date)
DISTRIBUTED BY HASH(trade_date) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);