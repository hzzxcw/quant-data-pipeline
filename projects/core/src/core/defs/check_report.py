"""资产检查状态报告生成。

查询 Dagster 实例中所有 Asset Checks 的最新执行状态，
生成 Markdown 格式报告，并通过 Mock 资源发送通知。
"""

import dagster as dg
from datetime import datetime
from collections import defaultdict


class MockNotificationResource(dg.ConfigurableResource):
    """Mock 通知资源，仅打印日志。"""

    def send(self, title: str, content: str, context: dg.AssetExecutionContext):
        context.log.info(f"📧 [MOCK] 发送通知: {title}")
        context.log.info("-" * 50)
        context.log.info(content)
        context.log.info("-" * 50)


@dg.asset(
    group_name="reporting",
    description="生成所有 Asset Checks 的状态报告并发送通知。",
)
def check_status_report(
    context: dg.AssetExecutionContext,
    notifier: MockNotificationResource,
) -> dg.MaterializeResult:
    """查询最新 Check 状态并生成报告。

    依赖: 无 (查询实例全局状态)
    """
    instance = context.instance

    evaluations = instance.get_asset_check_evaluations()

    latest_checks = {}
    for ev in evaluations:
        key = ev.asset_check_key
        if key not in latest_checks or ev.timestamp > latest_checks[key].timestamp:
            latest_checks[key] = ev

    sorted_checks = sorted(
        latest_checks.values(),
        key=lambda x: (x.asset_check_key.asset_key.path[-1], x.asset_check_key.name),
    )

    report_lines = [
        f"# 📊 Asset Check Status Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"",
        f"| Asset | Check | Status | Time |",
        f"|---|---|---|---|",
    ]

    total = len(sorted_checks)
    passed = 0
    failed = 0
    skipped = 0

    for ev in sorted_checks:
        asset_name = ev.asset_check_key.asset_key.path[-1]
        check_name = ev.asset_check_key.name
        status = ev.status.value
        time_str = datetime.fromtimestamp(ev.timestamp / 1000).strftime("%H:%M:%S")

        if status == "SUCCESS":
            passed += 1
            icon = "✅"
        elif status == "FAILURE":
            failed += 1
            icon = "❌"
        else:
            skipped += 1
            icon = "⏳"

        report_lines.append(
            f"| {asset_name} | {check_name} | {icon} {status} | {time_str} |"
        )

    report_lines.append(f"")
    report_lines.append(
        f"**Summary**: {total} checks, {passed} passed, {failed} failed, {skipped} skipped."
    )

    report_content = "\n".join(report_lines)

    notifier.send(
        title="Daily Asset Check Report",
        content=report_content,
        context=context,
    )

    return dg.MaterializeResult(
        metadata={
            "total_checks": dg.MetadataValue.int(total),
            "passed": dg.MetadataValue.int(passed),
            "failed": dg.MetadataValue.int(failed),
            "report": dg.MetadataValue.md(report_content),
        }
    )
