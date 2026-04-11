import dagster as dg
import pandas as pd


@dg.asset(
    group_name="macro_data",
    description="Fetch latest month retail sales data from national statistics bureau",
)
def retail_sales(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch social consumer goods retail sales data from national statistics bureau.

    Retail sales is an important indicator of consumer market activity,
    covering catering and commodity retail, and serves as a key reference for measuring domestic demand.

    Data source: National Bureau of Statistics (stats.gov.cn)

    Raises:
        requests.RequestException: If data fetch fails, stop and report error.
    """
    context.log.info("Fetching retail sales data...")

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
    from requests.exceptions import RequestException

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except RequestException as e:
        context.log.error(f"API request failed: {e}")
        raise

    if data.get("returncode") != 0:
        raise RuntimeError(f"API returned error code: {data.get('returncode')}")

    datalist = data.get("data", {}).get("datanodes", [])
    if not datalist:
        raise RuntimeError("No data nodes returned from API")

    records = []
    for node in datalist:
        records.append(
            {
                "indicator": node["wds"][0]["valuecode"],
                "period": node["wds"][1]["valuecode"],
                "value": node["data"].get("data"),
                "has_value": node["data"].get("hasdata", False),
            }
        )
    df = pd.DataFrame(records)

    valid_df = df[df["has_value"] == True]
    if valid_df.empty:
        raise RuntimeError("No valid data available in the response")

    context.log.info(f"Successfully fetched {len(valid_df)} records")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(valid_df)),
            "latest_period": dg.MetadataValue.text(str(valid_df["period"].max())),
            "data_source": dg.MetadataValue.text("National Bureau of Statistics"),
            "sample": dg.MetadataValue.json(valid_df.head(5).to_dict(orient="records")),
        }
    )
