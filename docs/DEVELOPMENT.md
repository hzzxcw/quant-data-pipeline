# 开发工作流

## 分支策略

```
main (生产分支)
  └── develop (开发分支)
        └── feature/issue-{编号}-{描述}
```

## 开发流程

### 1. 查看待办 Issues

```bash
./scripts/worktree.sh issues
```

### 2. 开始开发

```bash
./scripts/worktree.sh start <issue_number>
```

### 3. 切换 Worktree

```bash
./scripts/worktree.sh list
./scripts/worktree.sh switch <name>
```

### 4. 完成开发

```bash
# 确保已提交代码
./scripts/worktree.sh done
```

## Worktree 结构

```
/parent-directory/
├── {project}/                 # 主仓库
│   ├── scripts/worktree.sh
│   └── ...
└── {project}-feature/        # Feature worktrees
    ├── issue-42-xxx/
    └── ...
```

## 常用命令

| 操作 | 命令 |
|------|------|
| 查看 issues | `./scripts/worktree.sh issues` |
| 开始开发 | `./scripts/worktree.sh start 42` |
| 列出 worktrees | `./scripts/worktree.sh list` |
| 切换 worktree | `./scripts/worktree.sh switch issue-42-xxx` |
| 完成开发 | `./scripts/worktree.sh done` |
