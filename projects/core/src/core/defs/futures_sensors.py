"""期货数据采集 Sensor。

支持两种触发模式：
1. 文件到达：数据文件落地到指定目录时触发
2. 数据就位：检查多个数据源状态，满足条件时触发
"""

import json
from datetime import datetime
from pathlib import Path

import dagster as dg

from core.defs.futures_daily_quotes import (
    futures_daily_quotes,
    futures_daily_summary,
)

# ============================================================
# Job 定义
# ============================================================

futures_job = dg.define_asset_job(
    name="futures_daily",
    selection=dg.AssetSelection.assets(
        futures_daily_quotes,
        futures_daily_summary,
    ),
)


# ============================================================
# 配置：数据源定义
# ============================================================

# 数据就位标记文件目录
DATA_READY_DIR = Path("/tmp/futures_data_ready")

# 日盘数据源
DAY_SOURCES = ["sh_exchange", "dce_exchange", "zce_exchange"]

# 夜盘数据源
NIGHT_SOURCES = ["night_session"]


def _ensure_ready_dir() -> Path:
    DATA_READY_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_READY_DIR


def _check_source_ready(source: str, date: str) -> bool:
    flag = _ensure_ready_dir() / f"{source}_{date}.ready"
    return flag.exists()


# ============================================================
# Sensor 1：数据就位检测
# ============================================================

@dg.sensor(
    job=futures_job,
    minimum_interval_seconds=30,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def futures_data_readiness_sensor(context: dg.SensorEvaluationContext):
    """检查数据源就位状态，满足条件时触发期货处理。

    触发条件：
    - 日盘：所有交易所数据就位
    - 夜盘：夜盘数据源就位
    """
    processed: dict[str, str] = json.loads(context.cursor or "{}")
    today = datetime.now().strftime("%Y-%m-%d")

    runs: list[dg.RunRequest] = []

    day_key = f"{today}|day"
    if day_key not in processed:
        day_ready = all(_check_source_ready(s, today) for s in DAY_SOURCES)
        if day_ready:
            context.log.info(f"日盘数据就位: {DAY_SOURCES}")
            runs.append(dg.RunRequest(
                run_key=f"day_{today}",
                partition_key=day_key,
            ))
            processed[day_key] = datetime.now().isoformat()

    night_key = f"{today}|night"
    if night_key not in processed:
        night_ready = all(_check_source_ready(s, today) for s in NIGHT_SOURCES)
        if night_ready:
            context.log.info(f"夜盘数据就位: {NIGHT_SOURCES}")
            runs.append(dg.RunRequest(
                run_key=f"night_{today}",
                partition_key=night_key,
            ))
            processed[night_key] = datetime.now().isoformat()

    return dg.SensorResult(
        run_requests=runs,
        cursor=json.dumps(processed),
    )


# ============================================================
# Sensor 2：文件到达检测
# ============================================================

@dg.sensor(
    job=futures_job,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def futures_file_arrival_sensor(context: dg.SensorEvaluationContext):
    """监控数据文件到达，自动触发处理。

    文件命名格式：{source}_{date}.ready
    例如：sh_exchange_2026-04-26.ready
    """
    processed_files: list[str] = json.loads(context.cursor or "[]")
    ready_dir = _ensure_ready_dir()

    new_files = [
        f.name for f in ready_dir.glob("*.ready")
        if f.name not in processed_files
    ]

    if not new_files:
        return dg.SkipReason("No new files")

    runs: list[dg.RunRequest] = []
    today = datetime.now().strftime("%Y-%m-%d")

    for fname in sorted(new_files):
        context.log.info(f"检测到新文件: {fname}")

        parts = fname.replace(".ready", "").rsplit("_", 1)
        if len(parts) == 2:
            source, date = parts
            session = "night" if source in NIGHT_SOURCES else "day"
            partition_key = f"{date}|{session}"
        else:
            partition_key = f"{today}|day"

        runs.append(dg.RunRequest(
            run_key=f"file_{fname}",
            partition_key=partition_key,
        ))

    all_processed = list(set(processed_files + new_files))

    return dg.SensorResult(
        run_requests=runs,
        cursor=json.dumps(all_processed),
    )
