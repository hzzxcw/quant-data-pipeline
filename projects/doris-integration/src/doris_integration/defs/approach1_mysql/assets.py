from datetime import date, timedelta

import dagster as dg
import numpy as np
import pandas as pd


def _generate_stock_quotes(days: int = 30) -> pd.DataFrame:
    codes = ["000001.SZ", "000002.SZ", "600000.SH", "600036.SH"]
    rows = []
    base_date = date.today() - timedelta(days=days)

    for code in codes:
        base_price = np.random.uniform(10, 100)
        for i in range(days):
            trade_date = base_date + timedelta(days=i)
            if trade_date.weekday() >= 5:
                continue
            change = np.random.normal(0, 0.02)
            open_price = round(base_price * (1 + np.random.uniform(-0.01, 0.01)), 2)
            close_price = round(base_price * (1 + change), 2)
            high_price = round(
                max(open_price, close_price) * (1 + abs(np.random.normal(0, 0.005))), 2
            )
            low_price = round(
                min(open_price, close_price) * (1 - abs(np.random.normal(0, 0.005))), 2
            )
            volume = int(np.random.uniform(100000, 5000000))
            amount = round(volume * (open_price + close_price) / 2, 2)

            rows.append(
                {
                    "code": code,
                    "date": trade_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "amount": amount,
                }
            )
            base_price = close_price

    return pd.DataFrame(rows)


@dg.asset(key_prefix=["doris"])
def stock_quotes_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
    """Generate simulated stock quotes and persist via DorisIOManager."""
    df = _generate_stock_quotes()
    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "codes": dg.MetadataValue.text(", ".join(df["code"].unique())),
        }
    )
    return df


@dg.asset(key_prefix=["doris"])
def stock_daily_returns(
    context: dg.AssetExecutionContext, stock_quotes_asset: pd.DataFrame
) -> pd.DataFrame:
    """Compute daily returns from stock quotes and persist via DorisIOManager."""
    df = stock_quotes_asset.sort_values(["code", "date"])
    df["prev_close"] = df.groupby("code")["close"].shift(1)
    df["daily_return_pct"] = (
        (df["close"] - df["prev_close"]) / df["prev_close"] * 100
    ).round(4)
    df = df.dropna(subset=["prev_close"])

    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
        }
    )
    return df


@dg.asset(key_prefix=["doris"])
def stock_summary(stock_quotes_asset: pd.DataFrame) -> pd.DataFrame:
    """Aggregate stock quotes by code and persist via DorisIOManager."""
    return (
        stock_quotes_asset.groupby("code")
        .agg(
            avg_close=("close", "mean"),
            max_close=("close", "max"),
            min_close=("close", "min"),
            total_volume=("volume", "sum"),
        )
        .reset_index()
    )
