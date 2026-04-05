"""股票日度行情资产（带日期分区）+ GE 数据质量校验。

使用 TimeWindowPartitionsDefinition 按交易日分区，
每个分区包含当日 A 股主要股票行情数据。
"""

import dagster as dg
import pandas as pd
import numpy as np
from datetime import datetime

from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnPairValuesAToBeGreaterThanB,
)

from core.defs.resources import make_ge_asset_check_for_partitioned

market_holidays = [
    datetime(2026, 1, 1),
    datetime(2026, 1, 28),
    datetime(2026, 1, 29),
    datetime(2026, 1, 30),
    datetime(2026, 1, 31),
    datetime(2026, 2, 1),
    datetime(2026, 2, 2),
    datetime(2026, 2, 3),
    datetime(2026, 4, 4),
    datetime(2026, 4, 5),
    datetime(2026, 4, 6),
    datetime(2026, 5, 1),
    datetime(2026, 5, 2),
    datetime(2026, 5, 3),
    datetime(2026, 5, 4),
    datetime(2026, 5, 5),
    datetime(2026, 6, 19),
    datetime(2026, 6, 20),
    datetime(2026, 9, 25),
    datetime(2026, 10, 1),
    datetime(2026, 10, 2),
    datetime(2026, 10, 3),
    datetime(2026, 10, 4),
    datetime(2026, 10, 5),
    datetime(2026, 10, 6),
    datetime(2026, 10, 7),
]

trading_calendar = dg.TimeWindowPartitionsDefinition(
    start=datetime(2026, 1, 1),
    cron_schedule="0 0 * * 1-5",
    fmt="%Y-%m-%d",
    exclusions=market_holidays,
)


@dg.asset(
    group_name="stock_data",
    partitions_def=trading_calendar,
    description="A股主要股票日度行情数据，按交易日分区。",
)
def daily_stock_quotes(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """生成模拟 A 股日度行情数据。

    每个分区（交易日）包含约 50 只股票的 OHLCV 数据。
    """
    np.random.seed(hash(context.partition_key) % (2**31))

    symbols = [
        "600519.SH",
        "000858.SZ",
        "601318.SH",
        "000333.SZ",
        "600036.SH",
        "601166.SH",
        "000001.SZ",
        "600276.SH",
        "002594.SZ",
        "300750.SZ",
        "600900.SH",
        "601888.SH",
        "000568.SZ",
        "601012.SH",
        "002352.SZ",
        "603259.SH",
        "000651.SZ",
        "600809.SH",
        "002714.SZ",
        "601899.SH",
        "600030.SH",
        "002415.SZ",
        "601668.SH",
        "000725.SZ",
        "600585.SH",
        "601398.SH",
        "601288.SH",
        "600028.SH",
        "601988.SH",
        "601601.SH",
        "000002.SZ",
        "601669.SH",
        "002304.SZ",
        "600887.SH",
        "601336.SH",
        "002230.SZ",
        "600050.SH",
        "000100.SZ",
        "601088.SH",
        "002142.SZ",
        "600016.SH",
        "601985.SH",
        "000776.SZ",
        "600031.SH",
        "002027.SZ",
        "600196.SH",
        "000661.SZ",
        "300015.SZ",
        "600436.SH",
        "601066.SH",
    ]

    records = []
    for symbol in symbols:
        is_sh = symbol.endswith("SH")
        base_price = np.random.uniform(10, 200)
        prev_close = round(base_price, 2)

        pct_change = np.random.normal(0, 0.02)
        pct_change = np.clip(pct_change, -0.099, 0.099)
        open_price = round(prev_close * (1 + np.random.normal(0, 0.005)), 2)
        high = round(
            max(open_price, prev_close) * (1 + abs(np.random.normal(0, 0.01))), 2
        )
        low = round(
            min(open_price, prev_close) * (1 - abs(np.random.normal(0, 0.01))), 2
        )
        close = round(prev_close * (1 + pct_change), 2)

        volume = int(np.random.lognormal(16, 1))
        amount = round(volume * close * np.random.uniform(0.8, 1.2), 2)
        turnover = round(np.random.uniform(0.1, 8.0), 2)

        records.append(
            {
                "trade_date": context.partition_key,
                "symbol": symbol,
                "exchange": "SSE" if is_sh else "SZSE",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
                "pct_change": round(pct_change * 100, 2),
                "turnover_rate": turnover,
                "prev_close": prev_close,
            }
        )

    df = pd.DataFrame(records)
    context.log.info(f"[{context.partition_key}] 生成 {len(df)} 条行情记录")

    return df


def _load_stock_quotes_data(context: dg.AssetCheckExecutionContext) -> pd.DataFrame:
    """根据 partition_key 重新生成行情数据，绕过 Dagster 分区加载 bug。"""
    np.random.seed(hash(context.partition_key) % (2**31))

    symbols = [
        "600519.SH",
        "000858.SZ",
        "601318.SH",
        "000333.SZ",
        "600036.SH",
        "601166.SH",
        "000001.SZ",
        "600276.SH",
        "002594.SZ",
        "300750.SZ",
        "600900.SH",
        "601888.SH",
        "000568.SZ",
        "601012.SH",
        "002352.SZ",
        "603259.SH",
        "000651.SZ",
        "600809.SH",
        "002714.SZ",
        "601899.SH",
        "600030.SH",
        "002415.SZ",
        "601668.SH",
        "000725.SZ",
        "600585.SH",
        "601398.SH",
        "601288.SH",
        "600028.SH",
        "601988.SH",
        "601601.SH",
        "000002.SZ",
        "601669.SH",
        "002304.SZ",
        "600887.SH",
        "601336.SH",
        "002230.SZ",
        "600050.SH",
        "000100.SZ",
        "601088.SH",
        "002142.SZ",
        "600016.SH",
        "601985.SH",
        "000776.SZ",
        "600031.SH",
        "002027.SZ",
        "600196.SH",
        "000661.SZ",
        "300015.SZ",
        "600436.SH",
        "601066.SH",
    ]

    records = []
    for symbol in symbols:
        is_sh = symbol.endswith("SH")
        base_price = np.random.uniform(10, 200)
        prev_close = round(base_price, 2)
        pct_change = np.clip(np.random.normal(0, 0.02), -0.099, 0.099)
        open_price = round(prev_close * (1 + np.random.normal(0, 0.005)), 2)
        high = round(
            max(open_price, prev_close) * (1 + abs(np.random.normal(0, 0.01))), 2
        )
        low = round(
            min(open_price, prev_close) * (1 - abs(np.random.normal(0, 0.01))), 2
        )
        close = round(prev_close * (1 + pct_change), 2)
        volume = int(np.random.lognormal(16, 1))
        amount = round(volume * close * np.random.uniform(0.8, 1.2), 2)
        turnover = round(np.random.uniform(0.1, 8.0), 2)

        records.append(
            {
                "trade_date": context.partition_key,
                "symbol": symbol,
                "exchange": "SSE" if is_sh else "SZSE",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
                "pct_change": round(pct_change * 100, 2),
                "turnover_rate": turnover,
                "prev_close": prev_close,
            }
        )

    return pd.DataFrame(records)


stock_completeness = [
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__no_null_symbol",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(column="symbol"),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="股票代码不能为空",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__no_null_close",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(column="close"),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="收盘价不能为空",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__no_null_volume",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(column="volume"),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="成交量不能为空",
    ),
]

stock_accuracy = [
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__close_positive",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="close", min_value=0.01
        ),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="收盘价必须为正数",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__high_gte_low",
        expectation_factory=lambda: ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high", column_B="low"
        ),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="最高价必须 >= 最低价",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__pct_change_within_limit",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="pct_change", min_value=-10.0, max_value=10.0
        ),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="涨跌幅必须在 [-10%, +10%] 范围内（涨跌停限制）",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__volume_non_negative",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="volume", min_value=0
        ),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="成交量必须为非负数",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__turnover_rate_reasonable",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="turnover_rate", min_value=0.0, max_value=50.0
        ),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=False,
        description="换手率需在 [0%, 50%] 合理范围",
    ),
]

stock_consistency = [
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__symbol_unique",
        expectation_factory=lambda: ExpectColumnValuesToBeUnique(column="symbol"),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="同一交易日内股票代码不能重复",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__exchange_valid",
        expectation_factory=lambda: ExpectColumnValuesToBeInSet(
            column="exchange", value_set={"SSE", "SZSE"}
        ),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="交易所代码必须为 SSE 或 SZSE",
    ),
    make_ge_asset_check_for_partitioned(
        expectation_name="stock__symbol_format_valid",
        expectation_factory=lambda: ExpectColumnValuesToMatchRegex(
            column="symbol", regex=r"^\d{6}\.(SH|SZ)$"
        ),
        asset=daily_stock_quotes,
        data_loader=_load_stock_quotes_data,
        blocking=True,
        description="股票代码格式必须为 6位数字.SH 或 6位数字.SZ",
    ),
]

daily_stock_quotes_checks = [
    *stock_completeness,
    *stock_accuracy,
    *stock_consistency,
]
