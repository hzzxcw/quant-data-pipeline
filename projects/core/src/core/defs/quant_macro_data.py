"""量化宏观数据资产 + Great Expectations 数据质量校验。

包含一个生成模拟量化数据的 asset，以及四类基于 GE 的 asset checks：
- 完整性校验（非空、必需列）
- 准确性校验（值域范围、数据类型）
- 时序一致性校验（时间序列连续性）
- 业务规则校验（量化领域约束）
"""

import dagster as dg
import pandas as pd
import numpy as np
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeIncreasing,
)

from core.defs.resources import make_ge_asset_check


@dg.asset(
    group_name="quant_data",
    description="生成模拟的量化宏观时间序列数据，包含多种经济指标。",
)
def quant_macro_data(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """生成模拟量化宏观数据，包含以下字段：

    - trade_date: 交易日期（月度）
    - indicator_code: 指标编码
    - indicator_name: 指标名称
    - value: 指标数值
    - yoy_change: 同比变化百分比
    - mom_change: 环比变化百分比
    - confidence_level: 置信度 (0-1)
    - data_source: 数据来源
    - revision_count: 修订次数
    """
    np.random.seed(42)

    dates = pd.date_range(start="2025-01-31", end="2025-12-31", freq="ME")

    indicators = [
        {"code": "CPI", "name": "消费者物价指数", "base": 100.0, "vol": 0.5},
        {"code": "PMI", "name": "采购经理指数", "base": 50.0, "vol": 1.5},
        {"code": "GDP_GROWTH", "name": "GDP 同比增长率", "base": 5.0, "vol": 0.3},
        {"code": "M2", "name": "广义货币供应量增速", "base": 8.0, "vol": 0.4},
        {"code": "UNEMPLOYMENT", "name": "城镇调查失业率", "base": 5.2, "vol": 0.2},
    ]

    records = []
    for date in dates:
        for ind in indicators:
            if np.random.random() < 0.02:
                records.append(
                    {
                        "trade_date": date,
                        "indicator_code": ind["code"],
                        "indicator_name": ind["name"],
                        "value": None,
                        "yoy_change": None,
                        "mom_change": None,
                        "confidence_level": None,
                        "data_source": "国家统计局",
                        "revision_count": 0,
                    }
                )
                continue

            value = ind["base"] + np.random.normal(0, ind["vol"])
            yoy = np.random.normal(5.0, 1.5)
            mom = np.random.normal(0.2, 0.5)
            confidence = np.clip(np.random.normal(0.85, 0.1), 0.0, 1.0)

            records.append(
                {
                    "trade_date": date,
                    "indicator_code": ind["code"],
                    "indicator_name": ind["name"],
                    "value": round(value, 2),
                    "yoy_change": round(yoy, 2),
                    "mom_change": round(mom, 2),
                    "confidence_level": round(confidence, 4),
                    "data_source": "国家统计局",
                    "revision_count": int(
                        np.random.choice(
                            [0, 0, 0, 1, 1, 2], p=[0.5, 0.15, 0.15, 0.1, 0.05, 0.05]
                        )
                    ),
                }
            )

    df = pd.DataFrame(records)
    context.log.info(
        f"生成 {len(df)} 条量化宏观数据记录，覆盖 {len(dates)} 个月、{len(indicators)} 个指标"
    )

    return df


completeness_checks = [
    make_ge_asset_check(
        expectation_name="completeness__no_null_trade_date",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(column="trade_date"),
        asset=quant_macro_data,
        blocking=True,
        description="交易日期不能为空",
    ),
    make_ge_asset_check(
        expectation_name="completeness__no_null_indicator_code",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(
            column="indicator_code"
        ),
        asset=quant_macro_data,
        blocking=True,
        description="指标编码不能为空",
    ),
    make_ge_asset_check(
        expectation_name="completeness__no_null_indicator_name",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(
            column="indicator_name"
        ),
        asset=quant_macro_data,
        blocking=True,
        description="指标名称不能为空",
    ),
    make_ge_asset_check(
        expectation_name="completeness__no_null_data_source",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(column="data_source"),
        asset=quant_macro_data,
        blocking=True,
        description="数据来源不能为空",
    ),
    make_ge_asset_check(
        expectation_name="completeness__value_null_rate_below_5pct",
        expectation_factory=lambda: ExpectColumnValuesToNotBeNull(
            column="value", mostly=0.95
        ),
        asset=quant_macro_data,
        blocking=False,
        description="指标值空值率需低于 5%",
    ),
]

# 2. 准确性校验 (Accuracy)

accuracy_checks = [
    make_ge_asset_check(
        expectation_name="accuracy__confidence_in_range_0_to_1",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="confidence_level", min_value=0.0, max_value=1.0
        ),
        asset=quant_macro_data,
        blocking=True,
        description="置信度必须在 [0, 1] 范围内",
    ),
    make_ge_asset_check(
        expectation_name="accuracy__revision_count_non_negative",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="revision_count", min_value=0
        ),
        asset=quant_macro_data,
        blocking=True,
        description="修订次数必须为非负整数",
    ),
    make_ge_asset_check(
        expectation_name="accuracy__pmi_in_reasonable_range",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="value",
            min_value=20.0,
            max_value=80.0,
            mostly=0.99,
            row_condition='indicator_code=="PMI"',
            condition_parser="pandas",
        ),
        asset=quant_macro_data,
        blocking=True,
        description="PMI 指标值需在合理范围 [20, 80]",
    ),
    make_ge_asset_check(
        expectation_name="accuracy__cpi_positive",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="value",
            min_value=80.0,
            max_value=120.0,
            mostly=0.99,
            row_condition='indicator_code=="CPI"',
            condition_parser="pandas",
        ),
        asset=quant_macro_data,
        blocking=True,
        description="CPI 指标值需在合理范围 [80, 120]",
    ),
]

# 3. 时序一致性校验 (Temporal Consistency)

temporal_checks = [
    make_ge_asset_check(
        expectation_name="temporal__trade_date_sorted",
        expectation_factory=lambda: ExpectColumnValuesToBeIncreasing(
            column="trade_date", strictly=False
        ),
        asset=quant_macro_data,
        blocking=True,
        description="交易日期需按升序排列",
    ),
    make_ge_asset_check(
        expectation_name="temporal__indicator_code_in_valid_set",
        expectation_factory=lambda: ExpectColumnValuesToBeInSet(
            column="indicator_code",
            value_set={"CPI", "PMI", "GDP_GROWTH", "M2", "UNEMPLOYMENT"},
        ),
        asset=quant_macro_data,
        blocking=True,
        description="指标编码必须在预定义的合法集合内",
    ),
    make_ge_asset_check(
        expectation_name="temporal__data_source_consistent",
        expectation_factory=lambda: ExpectColumnValuesToBeInSet(
            column="data_source",
            value_set={"国家统计局", "中国人民银行", "海关总署", "财政部"},
        ),
        asset=quant_macro_data,
        blocking=True,
        description="数据来源必须在合法来源集合内",
    ),
]

# 4. 业务规则校验 (Business Rules)

business_rule_checks = [
    make_ge_asset_check(
        expectation_name="business__pmi_above_50_indicates_expansion",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="value",
            min_value=30.0,
            max_value=70.0,
            mostly=0.99,
            row_condition='indicator_code=="PMI"',
            condition_parser="pandas",
        ),
        asset=quant_macro_data,
        blocking=True,
        description="PMI 应在 30-70 的合理区间（50 为荣枯线）",
    ),
    make_ge_asset_check(
        expectation_name="business__unemployment_rate_reasonable",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="value",
            min_value=3.0,
            max_value=8.0,
            mostly=0.99,
            row_condition='indicator_code=="UNEMPLOYMENT"',
            condition_parser="pandas",
        ),
        asset=quant_macro_data,
        blocking=True,
        description="城镇调查失业率应在 3%-8% 合理区间",
    ),
    make_ge_asset_check(
        expectation_name="business__gdp_growth_positive",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="value",
            min_value=2.0,
            max_value=10.0,
            mostly=0.99,
            row_condition='indicator_code=="GDP_GROWTH"',
            condition_parser="pandas",
        ),
        asset=quant_macro_data,
        blocking=True,
        description="GDP 同比增长率应在 2%-10% 合理区间",
    ),
    make_ge_asset_check(
        expectation_name="business__m2_growth_reasonable",
        expectation_factory=lambda: ExpectColumnValuesToBeBetween(
            column="value",
            min_value=5.0,
            max_value=15.0,
            mostly=0.99,
            row_condition='indicator_code=="M2"',
            condition_parser="pandas",
        ),
        asset=quant_macro_data,
        blocking=True,
        description="M2 增速应在 5%-15% 合理区间",
    ),
]

quant_macro_data_checks = [
    *completeness_checks,
    *accuracy_checks,
    *temporal_checks,
    *business_rule_checks,
]
