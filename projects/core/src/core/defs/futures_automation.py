"""期货调度方案对比。

方案 1：build_schedule_from_partitioned_job（传统 Schedule）
方案 2：AutomationCondition.on_cron()（声明式自动化，推荐）

两种方案对比：
- Schedule：显式定义，手动管理，适合简单场景
- AutomationCondition：声明式，自动处理依赖，适合复杂场景
"""

import dagster as dg
from datetime import datetime

from core.defs.futures_daily_quotes import (
    futures_daily_quotes,
    futures_daily_summary,
    futures_partitions,
)


# ============================================================
# 方案 1：build_schedule_from_partitioned_job
# ============================================================

futures_job = dg.define_asset_job(
    name="futures_daily_job",
    selection=dg.AssetSelection.assets(
        futures_daily_quotes,
        futures_daily_summary,
    ),
)


# 方式 1a：简单场景，自动从分区推导 cron
# 注意：MultiPartitionsDefinition 需要手动指定 cron
futures_schedule_simple = dg.ScheduleDefinition(
    job=futures_job,
    cron_schedule="0 15 * * 1-5",  # 交易日15:00收盘后
    execution_timezone="Asia/Shanghai",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)


# 方式 1b：手动指定分区键
@dg.schedule(
    job=futures_job,
    cron_schedule="0 15 * * 1-5",
    execution_timezone="Asia/Shanghai",
)
def futures_day_session_schedule(context: dg.ScheduleEvaluationContext):
    """日盘：每个交易日15:00收盘后执行。"""
    today = datetime.now().strftime("%Y-%m-%d")
    return dg.RunRequest(
        run_key=f"day_{today}",
        partition_key=f"{today}|day",
    )


@dg.schedule(
    job=futures_job,
    cron_schedule="30 2 * * 1-6",  # 周六02:30，覆盖周五夜盘
    execution_timezone="Asia/Shanghai",
)
def futures_night_session_schedule(context: dg.ScheduleEvaluationContext):
    """夜盘：凌晨02:30执行。"""
    from core.defs.futures_daily_quotes import get_prev_trading_day, FUTURES_HOLIDAYS

    now = datetime.now()
    today = datetime(now.year, now.month, now.day)
    prev_trading = get_prev_trading_day(today, FUTURES_HOLIDAYS)
    date_str = prev_trading.strftime("%Y-%m-%d")

    return dg.RunRequest(
        run_key=f"night_{date_str}",
        partition_key=f"{date_str}|night",
    )


# ============================================================
# 方案 2：AutomationCondition.on_cron()（推荐）
# ============================================================

# 方式 2a：简单场景，单个 cron 条件
@dg.asset(
    group_name="futures_auto",
    partitions_def=futures_partitions,
    automation_condition=dg.AutomationCondition.on_cron(
        "0 15 * * 1-5",  # 交易日15:00
        cron_timezone="Asia/Shanghai",
    ),
    description="期货日盘行情，声明式自动化。",
)
def futures_auto_quotes(
    context: dg.AssetExecutionContext,
) -> dict:
    """每个交易日15:00自动执行。"""
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    session = partition_key.keys_by_dimension["session"]

    context.log.info(f"自动执行: {date_str} {session}")

    return {
        "date": date_str,
        "session": session,
        "rows": 100,
    }


# 方式 2b：上游就绪后自动执行（更智能）
@dg.asset(
    group_name="futures_auto",
    partitions_def=futures_partitions,
    automation_condition=dg.AutomationCondition.eager(),
    deps=["market_data_feed"],
    description="外部行情数据更新后立即执行。",
)
def futures_smart_quotes(context: dg.AssetExecutionContext) -> dict:
    """外部行情更新后立即执行。"""
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    session = partition_key.keys_by_dimension["session"]

    return {"date": date_str, "session": session}


# 方式 2c：延迟执行（处理延迟到货的数据）
@dg.asset(
    group_name="futures_auto",
    partitions_def=futures_partitions,
    automation_condition=dg.AutomationCondition.on_cron(
        "0 20 * * 1-5",  # 20:00执行
        cron_timezone="Asia/Shanghai",
    ),
    description="期货行情，延迟执行。",
)
def futures_delayed_quotes(context: dg.AssetExecutionContext) -> dict:
    """20:00执行，处理当日数据。"""
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    session = partition_key.keys_by_dimension["session"]

    return {"date": date_str, "session": session, "delayed": True}


# ============================================================
# 方案对比总结
# ============================================================

"""
## 方案对比

| 特性 | Schedule | AutomationCondition |
|---|---|---|
| 定义方式 | 显式 Schedule 对象 | 声明式条件 |
| 依赖处理 | 手动管理 | 自动等待上游 |
| 延迟执行 | 手动逻辑 | .with_lag() |
| 多条件组合 | 需要 Sensor | .allow() / .without() |
| 错过执行 | 需手动补跑 | 自动补跑 |
| 复杂度 | 低 | 中 |

## 推荐使用场景

- **Schedule**：简单固定时间执行，无复杂依赖
- **AutomationCondition**：有上游依赖，需要智能调度
"""

