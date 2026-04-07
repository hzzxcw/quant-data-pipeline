# PROJECT KNOWLEDGE BASE

**Generated:** 2026-04-07
**Branch:** develop
**Model:** Python 3.11/3.13 multi-code-location Dagster workspace

## OVERVIEW

量化投资数据管线，基于 Dagster 多 code locations 架构。支持股票/期货/期权数据采集 → 清洗 → 存储 → 分析。多数据源集成（Doris, PostgreSQL）。

## STRUCTURE

```
quant-data-pipeline/
├── AGENTS.md                    # 项目级开发规范（本文件）
├── dg.toml                      # Dagster workspace 配置
├── deployments/local/           # 本地部署配置
├── docs/                        # 参考文档
├── scripts/                     # 辅助脚本
└── projects/
    ├── core/                    # 核心数据处理 code location (Python 3.11)
    │   ├── src/core/
    │   │   ├── definitions.py   # Dagster 定义入口
    │   │   └── defs/            # 资产定义（load_from_defs_folder 自动扫描）
    │   └── tests/
    └── doris-integration/       # Doris 集成 code location (Python 3.13)
        ├── src/doris_integration/
        │   ├── definitions.py   # Dagster 定义入口
        │   ├── defs/            # MySQL 协议资产（自动扫描）
        │   └── dbt/             # dbt-doris 集成（手动注册）
        └── pyproject.toml
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| 添加新资产 | `projects/<location>/src/<pkg>/defs/` | 新建 .py 文件即生效 |
| 添加新资源 | `projects/<location>/src/<pkg>/defs/resources.py` | ConfigurableResource |
| 添加新 code location | `projects/` 下新建 + 更新 `dg.toml` | 参考 doris-integration |
| 部署配置 | `deployments/local/` | Docker Compose |
| 开发规范 | `AGENTS.md` (本文件) + `~/.config/opencode/AGENTS.md` (全局) | |

## CONVENTIONS

- `import dagster as dg` 统一别名
- `load_from_defs_folder` 自动扫描资产，不手动 import
- 资源通过 `dg.EnvVar` 注入，不硬编码
- Python 3.12+ 类型语法（`list[str]` 非 `List[str]`）
- LBYL 异常处理，具体异常不裸 except
- pathlib 操作路径

## ANTI-PATTERNS

- 不手动 import asset 到 definitions.py（用 load_from_defs_folder）
- 不硬编码数据库凭证（用 dg.EnvVar）
- 不使用 `as any` / `type: ignore`
- 不提交 `.venv/` / `__pycache__/` / `target/`
- 不在 defs/ 下放 dbt 项目（会重复注册）

## UNIQUE STYLES

- 多 code locations 通过 `dg.toml` workspace 注册
- 每个 location 独立 pyproject.toml + 独立虚拟环境
- 资产依赖通过参数名自动解析（`stock_quotes_asset: pd.DataFrame`）
- IOManager 表名从 asset key path 推导

## COMMANDS

```bash
# 开发
uv run dg dev                          # 启动开发服务器
uv run dg list defs                    # 列出所有定义
uv run dg check defs                   # 验证定义
uv run dg launch --assets asset_name   # 材料化资产

# 测试
uv run pytest                          # 运行测试

# 依赖
uv sync                                # 同步依赖
```

## NOTES

- core 用 Python 3.11，doris-integration 用 Python 3.13
- dagster-daemon 只能单实例
- dbt manifest.json 需通过 `dbt parse` 生成
- Doris 通过 MySQL 协议连接（端口 9030），默认 root/空密码
