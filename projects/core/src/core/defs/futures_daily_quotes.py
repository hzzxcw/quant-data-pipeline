"""期货日盘/夜盘行情资产（双维度分区）。

分区逻辑：
- 日期维度：交易日（周一至周五，排除节假日）
- 盘次维度：day（日盘）、night（夜盘）
- 夜盘归属下一个交易日：周一前一夜盘 → 周一|night

分区键格式：2026-01-05|night 表示"周一前一夜盘（周日21:00~周一02:30）"
"""

import dagster as dg
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass


FUTURES_HOLIDAYS = [
    datetime(2026, 1, 1), datetime(2026, 1, 2), datetime(2026, 1, 3),
    datetime(2026, 1, 28), datetime(2026, 1, 29), datetime(2026, 1, 30),
    datetime(2026, 1, 31), datetime(2026, 2, 1), datetime(2026, 2, 2),
    datetime(2026, 2, 3),
    datetime(2026, 4, 4), datetime(2026, 4, 5), datetime(2026, 4, 6),
    datetime(2026, 5, 1), datetime(2026, 5, 2), datetime(2026, 5, 3),
    datetime(2026, 5, 4), datetime(2026, 5, 5),
    datetime(2026, 6, 19), datetime(2026, 6, 20),
    datetime(2026, 9, 25), datetime(2026, 9, 26), datetime(2026, 9, 27),
    datetime(2026, 10, 1), datetime(2026, 10, 2), datetime(2026, 10, 3),
    datetime(2026, 10, 4), datetime(2026, 10, 5), datetime(2026, 10, 6),
    datetime(2026, 10, 7),
]

# 仅交易日（周五夜盘归属下一个交易日）
futures_trading_days = dg.TimeWindowPartitionsDefinition(
    start=datetime(2026, 1, 1),
    cron_schedule="0 0 * * 1-5",
    fmt="%Y-%m-%d",
    exclusions=FUTURES_HOLIDAYS,
)

futures_session = dg.StaticPartitionsDefinition(["day", "night"])

futures_partitions = dg.MultiPartitionsDefinition({
    "date": futures_trading_days,
    "session": futures_session,
})

@dataclass
class SessionTimeRange:
    start: datetime
    end: datetime
    label: str


def get_session_time_range(partition_key: dg.MultiPartitionKey) -> SessionTimeRange:
    date_str = partition_key.keys_by_dimension["date"]
    session = partition_key.keys_by_dimension["session"]
    trade_date = datetime.strptime(date_str, "%Y-%m-%d")

    if session == "day":
        return SessionTimeRange(
            start=trade_date.replace(hour=9, minute=0),
            end=trade_date.replace(hour=15, minute=0),
            label=f"{date_str} 日盘",
        )
    else:
        night_start, night_end = get_night_session_for_trading_day(
            trade_date, FUTURES_HOLIDAYS
        )
        return SessionTimeRange(
            start=night_start,
            end=night_end,
            label=f"{date_str} 夜盘（前一交易日21:00 ~ 次日02:30）",
        )


def get_prev_trading_day(current: datetime, holidays: list[datetime]) -> datetime:
    """获取前一个交易日。"""
    d = current - timedelta(days=1)
    while d.weekday() >= 5 or d.replace(hour=0, minute=0, second=0, microsecond=0) in holidays:
        d -= timedelta(days=1)
    return d


def get_night_session_for_trading_day(
    trade_date: datetime,
    holidays: list[datetime],
) -> tuple[datetime, datetime]:
    """获取交易日夜盘的实际时间范围。

    夜盘属于该交易日，但实际是前一个交易日晚上开始的。
    周一夜盘 = 周五21:00 ~ 周六02:30
    """
    prev_trading_day = get_prev_trading_day(trade_date, holidays)
    return (
        prev_trading_day.replace(hour=21, minute=0),
        (prev_trading_day + timedelta(days=1)).replace(hour=2, minute=30),
    )


FUTURES_SYMBOLS = [
    "AU2512.SHF", "AG2512.SHF", "CU2507.SHF", "AL2507.SHF", "ZN2507.SHF",
    "I2509.DCE", "RB2510.SHF", "HC2510.SHF", "J2509.DCE", "JM2509.DCE",
    "M2509.DCE", "P2509.DCE", "CF509.ZCE", "SR509.ZCE", "C2509.DCE",
    "SC2507.INE", "TA509.ZCE", "MA509.ZCE", "PP2509.DCE", "V2509.DCE",
]


@dg.asset(
    group_name="futures_data",
    partitions_def=futures_partitions,
    description="期货日度行情数据，按(日期, 盘次)双维度分区。",
)
def futures_daily_quotes(context: dg.AssetExecutionContext) -> pd.DataFrame:
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    session = partition_key.keys_by_dimension["session"]
    time_range = get_session_time_range(partition_key)

    context.log.info(f"处理分区: {time_range.label}")
    context.log.info(f"实际时段: {time_range.start} ~ {time_range.end}")

    seed = hash(f"{date_str}_{session}") % (2**31)
    np.random.seed(seed)

    records = []
    for symbol in FUTURES_SYMBOLS:
        base_price = np.random.uniform(1000, 5000)
        open_price = round(base_price * (1 + np.random.normal(0, 0.005)), 2)
        high = round(
            max(open_price, base_price) * (1 + abs(np.random.normal(0, 0.01))), 2
        )
        low = round(
            min(open_price, base_price) * (1 - abs(np.random.normal(0, 0.01))), 2
        )
        close = round(base_price * (1 + np.random.normal(0, 0.02)), 2)

        records.append({
            "trade_date": date_str,
            "session": session,
            "symbol": symbol,
            "exchange": symbol.split(".")[1],
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": int(np.random.lognormal(10, 1)),
            "turnover": round(np.random.uniform(1e6, 1e9), 2),
        })

    df = pd.DataFrame(records)
    context.log.info(f"[{time_range.label}] 生成 {len(df)} 条行情记录")

    return df


@dg.asset(
    group_name="futures_data",
    partitions_def=futures_partitions,
    description="期货行情统计摘要，按(日期, 盘次)分区。",
)
def futures_daily_summary(
    context: dg.AssetExecutionContext,
    futures_daily_quotes: pd.DataFrame,
) -> dict:
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    session = partition_key.keys_by_dimension["session"]
    time_range = get_session_time_range(partition_key)

    summary = {
        "trade_date": date_str,
        "session": session,
        "time_range": {"start": str(time_range.start), "end": str(time_range.end)},
        "row_count": len(futures_daily_quotes),
        "volume_total": int(futures_daily_quotes["volume"].sum()),
        "turnover_total": round(futures_daily_quotes["turnover"].sum(), 2),
        "symbols": futures_daily_quotes["symbol"].tolist(),
    }

    context.log.info(f"[{time_range.label}] 汇总: {summary['row_count']} 条记录")
    return summary
