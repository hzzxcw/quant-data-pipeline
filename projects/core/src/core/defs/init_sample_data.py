"""初始化示例数据 - 将 CSV 数据加载到 DuckDB 数据库中。

该脚本读取 CSV 文件并将其写入 DuckDB 数据库，
为 dbt 模型提供原始数据源。
"""

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = (
    Path(__file__).parent.parent.parent.parent.parent
    / "dbt_project"
    / "data"
    / "warehouse.duckdb"
)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def init_sample_data():
    """初始化示例数据。"""
    conn = duckdb.connect(str(DB_PATH))

    orders_data = {
        "order_id": list(range(1, 101)),
        "customer_id": [i % 10 + 1 for i in range(100)],
        "amount": [round(50 + i * 1.5, 2) for i in range(100)],
        "status": ["completed"] * 80 + ["pending"] * 15 + ["cancelled"] * 5,
        "created_at": pd.date_range("2024-01-01", periods=100, freq="D")
        .strftime("%Y-%m-%d")
        .tolist(),
    }

    customers_data = {
        "customer_id": list(range(1, 11)),
        "name": [f"Customer_{i}" for i in range(1, 11)],
        "email": [f"customer{i}@example.com" for i in range(1, 11)],
        "region": ["East", "West", "North", "South"] * 2 + ["East", "West"],
    }

    orders_df = pd.DataFrame(orders_data)
    customers_df = pd.DataFrame(customers_data)

    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("DROP TABLE IF EXISTS raw.orders")
    conn.execute("DROP TABLE IF EXISTS raw.customers")

    conn.execute("""
        CREATE TABLE raw.orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            status VARCHAR NOT NULL,
            created_at DATE NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE raw.customers (
            customer_id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            region VARCHAR
        )
    """)

    conn.execute("INSERT INTO raw.orders SELECT * FROM orders_df")
    conn.execute("INSERT INTO raw.customers SELECT * FROM customers_df")

    print(f"✅ 示例数据已加载到 {DB_PATH}")
    print(f"   - 订单: {len(orders_df)} 条")
    print(f"   - 客户: {len(customers_df)} 条")

    conn.close()


if __name__ == "__main__":
    init_sample_data()
