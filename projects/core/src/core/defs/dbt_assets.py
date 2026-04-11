"""dbt 项目集成 - 将 dbt 模型转换为 Dagster 资产。

该模块使用 @dbt_assets 装饰器自动将 dbt 模型映射为 Dagster 资产，
实现细粒度的编排和监控。
"""

from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = (
    Path(__file__).parent.parent.parent.parent.parent.parent / "dbt_project"
)

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    packaged_project_dir=DBT_PROJECT_DIR,
    profiles_dir=Path.home() / ".dbt",
)

dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(
    project_dir=dbt_project.project_dir,
    profiles_dir=Path.home() / ".dbt",
)


@dbt_assets(manifest=dbt_project.manifest_path)
def quant_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """将 dbt 模型转换为 Dagster 资产。

    dbt 模型包括:
    - stg_orders: 清洗后的订单数据
    - stg_customers: 客户基础数据
    - daily_revenue: 每日收入汇总
    - customer_summary: 客户汇总分析
    """
    yield from (
        dbt.cli(["build"], context=context)
        .stream()
        .fetch_row_counts()
        .fetch_column_metadata()
    )


daily_dbt_job = dg.define_asset_job(
    name="daily_dbt_models",
    selection=dg.AssetSelection.all(),
)

daily_schedule = dg.ScheduleDefinition(
    job=daily_dbt_job,
    cron_schedule="0 2 * * *",
)

defs = dg.Definitions(
    assets=[quant_dbt_assets],
    resources={"dbt": dbt_resource},
    schedules=[daily_schedule],
)
