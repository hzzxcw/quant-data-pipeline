import dagster as dg
import pandas as pd
import great_expectations as gx


class GreatExpectationsResource(dg.ConfigurableResource):
    """GE v1.15 校验资源。

    使用 gx.get_context() + data_sources.add_pandas + batch.validate() 模式。
    """

    def validate(self, df: pd.DataFrame, expectation):
        context = gx.get_context()
        data_source = context.data_sources.add_pandas("quant_source")
        data_asset = data_source.add_dataframe_asset(name="quant_asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_def")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
        return batch.validate(expectation)


_CHECK_TEMPLATE = """
def {func_name}(
    context: dg.AssetCheckExecutionContext,
    ge_resource: GreatExpectationsResource,
    {asset_name}: pd.DataFrame,
) -> dg.AssetCheckResult:
    target_data = {asset_name}
    return _run_ge_check(context, ge_resource, target_data, expectation_factory, sample_size)
"""


def _run_ge_check(context, ge_resource, target_data, expectation_factory, sample_size):
    expectation = expectation_factory()
    validation_result = ge_resource.validate(target_data, expectation)

    result_data = validation_result.get("result", {})
    unexpected_indices = result_data.get("partial_unexpected_index_list", [])
    sample_records = None
    if unexpected_indices:
        df_slice = target_data.iloc[unexpected_indices[:sample_size]].copy()
        for col in df_slice.select_dtypes(include=["datetime"]).columns:
            df_slice[col] = df_slice[col].astype(str)
        sample_records = df_slice.to_dict(orient="records")

    metadata = {
        "element_count": dg.MetadataValue.int(result_data.get("element_count", 0)),
        "unexpected_count": dg.MetadataValue.int(
            result_data.get("unexpected_count", 0)
        ),
        "unexpected_percent": dg.MetadataValue.float(
            result_data.get("unexpected_percent", 0.0)
        ),
    }
    if sample_records:
        metadata["sample_unexpected_records"] = dg.MetadataValue.json(sample_records)

    return dg.AssetCheckResult(
        passed=validation_result.get("success", False),
        severity=dg.AssetCheckSeverity.ERROR,
        metadata=metadata,
    )


def make_ge_asset_check(
    expectation_name: str,
    expectation_factory,
    asset,
    blocking: bool = False,
    description: str = "",
    sample_size: int = 5,
):
    asset_name = asset.key.path[-1] if hasattr(asset, "key") else asset.__name__
    func_name = f"_check_{asset_name}_{expectation_name}"

    code = _CHECK_TEMPLATE.format(
        func_name=func_name,
        asset_name=asset_name,
    )

    local_ns = {
        "dg": dg,
        "pd": pd,
        "GreatExpectationsResource": GreatExpectationsResource,
        "expectation_factory": expectation_factory,
        "sample_size": sample_size,
        "_run_ge_check": _run_ge_check,
    }
    exec(code, local_ns, local_ns)
    check_fn = local_ns[func_name]

    return dg.asset_check(
        asset=asset,
        name=expectation_name,
        blocking=blocking,
        description=description,
    )(check_fn)


def make_ge_asset_check_for_partitioned(
    expectation_name: str,
    expectation_factory,
    asset,
    data_loader,
    blocking: bool = False,
    description: str = "",
    sample_size: int = 5,
):
    """工厂函数：为分区资产生成 asset check，绕过 Dagster 分区加载 bug。

    不依赖 Dagster 的自动输入加载，改为通过 data_loader 函数手动获取数据。

    Args:
        expectation_name: check 名称
        expectation_factory: 返回 GE Expectation 实例的 callable
        asset: 目标资产
        data_loader: callable(context) -> pd.DataFrame，根据 context.partition_key 加载数据
        blocking: 是否阻塞下游资产
        description: check 描述
        sample_size: 失败样本展示条数
    """
    func_name = f"_check_{expectation_name}"

    code = f"""
def {func_name}(
    context: dg.AssetCheckExecutionContext,
    ge_resource: GreatExpectationsResource,
) -> dg.AssetCheckResult:
    target_data = data_loader(context)
    return _run_ge_check(context, ge_resource, target_data, expectation_factory, sample_size)
"""

    local_ns = {
        "dg": dg,
        "pd": pd,
        "GreatExpectationsResource": GreatExpectationsResource,
        "expectation_factory": expectation_factory,
        "sample_size": sample_size,
        "data_loader": data_loader,
        "_run_ge_check": _run_ge_check,
    }
    exec(code, local_ns, local_ns)
    check_fn = local_ns[func_name]

    return dg.asset_check(
        asset=asset,
        name=expectation_name,
        blocking=blocking,
        description=description,
    )(check_fn)
