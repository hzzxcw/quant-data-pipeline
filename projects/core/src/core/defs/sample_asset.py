import dagster as dg
import pandas as pd
from datetime import datetime


@dg.asset(
    group_name="sample",
    description="示例资产：生成模拟的股票行情数据",
)
def sample_market_data() -> dg.MaterializeResult:
    """生成模拟的股票行情数据，用于测试 Dagster 工作流。

    这是一个测试资产，用于验证项目结构和配置。
    """
    # 生成模拟数据
    data = {
        "symbol": ["AAPL", "GOOGL", "MSFT", "AMZN"],
        "price": [150.25, 2800.50, 310.75, 3400.00],
        "volume": [1000000, 500000, 750000, 300000],
        "timestamp": [datetime.now()] * 4,
    }
    df = pd.DataFrame(data)

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "symbols": dg.MetadataValue.text(", ".join(df["symbol"].tolist())),
            "sample_data": dg.MetadataValue.json(df.to_dict(orient="records")),
        }
    )


@dg.asset(
    group_name="sample",
    description="示例资产：计算股票平均价格",
    deps=["sample_market_data"],
)
def sample_avg_price(sample_market_data: dg.MaterializeResult) -> dg.MaterializeResult:
    """计算示例数据中的平均价格。

    依赖于 sample_market_data 资产。
    """
    # 在实际场景中，这里会从上游资产获取数据
    # 这里为了演示，我们直接计算
    avg_price = 2415.375  # 示例平均值

    return dg.MaterializeResult(
        metadata={
            "avg_price": dg.MetadataValue.float(avg_price),
            "calculation_time": dg.MetadataValue.text(str(datetime.now())),
        }
    )
