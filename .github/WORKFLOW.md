# 开发工作流

## 分支策略

```
main (生产分支)
  └── develop (开发分支)
        └── feature/issue-{编号}-{描述}
```

- `main`: 生产就绪代码，只接收来自 develop 的 PR 合并
- `develop`: 集成分支，所有 feature 开发完成后合并至此
- `feature/*`: 从 develop 创建，开发完成后提 PR 合并回 develop

## 开发流程

### 1. 查看待办 Issues

```bash
./.github/scripts/worktree.sh issues
```

### 2. 开始开发 Issue

```bash
# 创建 worktree 并切换
./.github/scripts/worktree.sh start <issue_number>

# 例如：开始处理 issue #42
./.github/scripts/worktree.sh start 42
```

脚本会自动：
- 获取 issue 信息
- 创建分支 `feature/issue-42-{title}`
- 在 `../quant-data-pipeline-feature/issue-42-xxx/` 创建 worktree

### 3. 切换 Worktree

```bash
# 查看所有 worktree
./.github/scripts/worktree.sh list

# 切换到指定 worktree
./.github/scripts/worktree.sh switch issue-42-xxx
```

### 4. 完成开发

```bash
# 确保已经：
# 1. 实现功能
# 2. 添加/更新测试
# 3. 运行测试：uv run pytest
# 4. 提交代码

# 在 worktree 目录执行
../quant-data-pipeline/.github/scripts/worktree.sh done
```

脚本会自动：
- 推送代码到 origin
- 创建 PR 到 develop 分支

### 5. Code Review

1. 在 GitHub 上查看 PR
2. 进行代码审查
3. 合并 PR 到 develop

### 6. 部署到生产

```bash
# 在 develop 分支
git checkout develop
git pull upstream develop

# 创建 PR 到 main
gh pr create --base main --head develop

# 合并后，main 即生产版本
```

## Worktree 管理

### 当前结构

```
/Users/chenwei/
├── quant-data-pipeline/           # 主仓库 (develop)
│   ├── .github/scripts/worktree.sh
│   ├── projects/
│   └── ...
└── quant-data-pipeline-feature/   # Feature worktrees
    ├── issue-42-add-doris-support/
    ├── issue-43-fix-auth-bug/
    └── ...
```

### 清理已合并的 Worktree

```bash
# 删除已合并的 worktree
git worktree prune

# 完全删除分支
git push origin --delete feature/issue-42-xxx
```

## 测试

```bash
# 运行所有测试
uv run pytest

# 运行带覆盖率
uv run pytest --cov=src --cov-report=html
```

## 常用命令参考

| 操作 | 命令 |
|------|------|
| 查看 issues | `./.github/scripts/worktree.sh issues` |
| 开始开发 | `./.github/scripts/worktree.sh start 42` |
| 列出 worktrees | `./.github/scripts/worktree.sh list` |
| 切换 worktree | `./.github/scripts/worktree.sh switch issue-42-xxx` |
| 完成开发 | `./.github/scripts/worktree.sh done` |
