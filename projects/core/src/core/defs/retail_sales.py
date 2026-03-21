import dagster as dg
import pandas as pd


@dg.asset(
    group_name="macro_data",
    description="获取最新月份的社会消费品零售总额数据",
)
def retail_sales() -> dg.MaterializeResult:
    """获取国家统计局发布的社会消费品零售总额数据。

    社会消费品零售总额是反映消费品市场活跃程度的重要指标，
    涵盖餐饮和商品零售，是衡量内需的重要参考。

    数据来源：国家统计局 (stats.gov.cn)

    Returns:
        MaterializeResult: 包含社会消费品零售总额数据和元数据
    """
    context = dg.AssetExecutionContext.get()

    try:
        context.log.info("开始获取社会消费品零售总额数据...")

        url = "http://data.stats.gov.cn/easyquery.htm"

        params = {
            "m": "QueryData",
            "dbcode": "hgyd",
            "rowcode": "zb",
            "colcode": "sj",
            "wds": "[]",
            "dfwds": '[{"wdcode":"zb","valuecode":"A0N0E"},{"wdcode":"sj","valuecode":"LAST"}]',
            "h": 1,
        }

        import requests

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("returncode") == 0:
            datalist = data.get("data", {}).get("datanodes", [])
            if datalist:
                records = []
                for node in datalist:
                    records.append(
                        {
                            "indicator": node["wds"][0]["valuecode"],
                            "period": node["wds"][1]["valuecode"],
                            "value": node["data"].get("data", None),
                            "has_value": node["data"].get("hasdata", False),
                        }
                    )
                df = pd.DataFrame(records)

                context.log.info(f"成功获取 {len(df)} 条记录")

                return dg.MaterializeResult(
                    metadata={
                        "row_count": dg.MetadataValue.int(len(df)),
                        "latest_period": dg.MetadataValue.text(
                            df[df["has_value"]]["period"].max()
                            if df["has_value"].any()
                            else "N/A"
                        ),
                        "data_source": dg.MetadataValue.text("国家统计局"),
                        "sample": dg.MetadataValue.json(
                            df[df["has_value"]].head(5).to_dict(orient="records")
                        ),
                    }
                )

        context.log.warning("API 不可用，返回模拟数据用于测试")
        return _generate_mock_data(context)

    except Exception as e:
        context.log.warning(f"请求失败: {e}，返回模拟数据")
        return _generate_mock_data(context)


def _generate_mock_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """生成模拟数据用于测试和演示。"""
    mock_data = {
        "indicator": ["零售总额当月同比", "零售总额累计同比", "餐饮收入当月同比"],
        "period": ["202602", "202601", "202602"],
        "value": [5.2, 5.5, 5.8],
        "unit": ["%", "%", "%"],
    }
    df = pd.DataFrame(mock_data)

    context.log.info(f"生成 {len(df)} 条模拟数据记录")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "data_type": dg.MetadataValue.text("mock_data"),
            "latest_period": dg.MetadataValue.text("202602"),
            "note": dg.MetadataValue.text("使用模拟数据，实际数据请访问国家统计局"),
            "sample": dg.MetadataValue.json(df.to_dict(orient="records")),
        }
    )
