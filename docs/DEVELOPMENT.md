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
./scripts/worktree.sh issues
```

### 2. 开始开发 Issue

```bash
./scripts/worktree.sh start <issue_number>
```

脚本会自动：
- 获取 issue 信息
- 创建分支 `feature/issue-{编号}-{title}`
- 在 `../quant-data-pipeline-{branch}/` 创建 worktree

### 3. 切换 Worktree

```bash
./scripts/worktree.sh list
./scripts/worktree.sh switch issue-42-xxx
```

### 4. 完成开发

确保完成：
1. 实现功能
2. 添加/更新测试
3. 运行测试：`uv run pytest`
4. 提交代码

```bash
../quant-data-pipeline/scripts/worktree.sh done
```

脚本会自动推送并创建 PR 到 develop。

### 5. Code Review

1. 在 GitHub 上查看 PR
2. 进行代码审查
3. 合并 PR 到 develop

### 6. 部署到生产

```bash
git checkout develop && git pull upstream develop
gh pr create --base main --head develop
```

## Worktree 结构

```
/Users/chenwei/
├── quant-data-pipeline/           # 主仓库 (develop)
│   ├── scripts/worktree.sh
│   └── ...
└── quant-data-pipeline-feature/  # Feature worktrees
    ├── issue-42-xxx/
    └── ...
```

### 清理已合并的 Worktree

```bash
git worktree prune
git push origin --delete feature/issue-42-xxx
```

## 常用命令

| 操作 | 命令 |
|------|------|
| 查看 issues | `./scripts/worktree.sh issues` |
| 开始开发 | `./scripts/worktree.sh start 42` |
| 列出 worktrees | `./scripts/worktree.sh list` |
| 切换 worktree | `./scripts/worktree.sh switch issue-42-xxx` |
| 完成开发 | `./scripts/worktree.sh done` |
