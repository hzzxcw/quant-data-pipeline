# Oh My OpenCode 安装指南

> 仓库地址：https://github.com/code-yeongyu/oh-my-openagent
>
> 最新版本文档：https://raw.githubusercontent.com/code-yeongyu/oh-my-openagent/refs/heads/dev/docs/guide/installation.md

---

## 前置条件

1. 安装 OpenCode（如未安装）：
   ```bash
   # 检查是否已安装
   opencode --version
   
   # 未安装？参考：https://opencode.ai/docs
   ```

2. 确认你的 AI 订阅（至少需要一个）：

|| 订阅 | 用途 |
||-----|------|
|| **Claude Pro/Max** | Sisyphus 主代理（推荐） |
|| **ChatGPT Plus** | Oracle/深度推理（推荐） |
|| **Gemini** | 视觉/前端 |
|| **GitHub Copilot** | 备用模型 |
|| **Kimi for Coding** | Claude 替代 |
|| **Z.ai Coding Plan** | GLM 模型 |
|| **OpenCode Go** | $10/月套餐 |

---

## 安装步骤

### 第一步：确认订阅

根据你的订阅情况，组合 CLI 参数：

```bash
# 全部订阅
bunx oh-my-opencode install --no-tui \
  --claude=max20 --openai=yes --gemini=yes --copilot=no

# 只有 Claude
bunx oh-my-opencode install --no-tui \
  --claude=yes --openai=no --gemini=no --copilot=no

# 只有 Kimi for Coding（Claude 替代）
bunx oh-my-opencode install --no-tui \
  --claude=no --openai=no --gemini=no --copilot=no \
  --kimi-for-coding=yes

# Claude + OpenAI（推荐组合）
bunx oh-my-opencode install --no-tui \
  --claude=max20 --openai=yes --gemini=no --copilot=no

# OpenCode Go（$10/月套餐）
bunx oh-my-opencode install --no-tui \
  --claude=no --openai=no --gemini=no --copilot=no \
  --opencode-go=yes
```

**CLI 参数说明**：

| 参数 | 可选值 | 说明 |
|-----|-------|------|
| `--claude=` | `yes`, `no`, `max20` | max20 指 20x 模式 |
| `--openai=` | `yes`, `no` | GPT-5.4 用于 Oracle |
| `--gemini=` | `yes`, `no` | 视觉/前端任务 |
| `--copilot=` | `yes`, `no` | GitHub Copilot |
| `--kimi-for-coding=` | `yes`, `no` | Kimi K2.5 |
| `--zai-coding-plan=` | `yes`, `no` | GLM 模型 |
| `--opencode-go=` | `yes`, `no` | $10/月套餐 |
| `--opencode-zen=` | `yes`, `no` | opencode/ 模型 |
| `--vercel-ai-gateway=` | `yes`, `no` | Vercel 网关 |

### 第二步：执行安装

```bash
bunx oh-my-opencode install --no-tui <你的参数>
```

### 第三步：验证安装

```bash
# 检查版本
opencode --version

# 检查插件注册
cat ~/.config/opencode/opencode.json | grep oh-my-openagent

# 运行诊断
bunx oh-my-opencode doctor
```

### 第四步：配置认证

```bash
# Claude 认证
opencode auth login
# → 选择 Anthropic → Claude Pro/Max → 浏览器 OAuth

# Gemini 认证（如需要）
opencode auth login
# → 选择 Google → OAuth with Google

# ChatGPT 认证（如需要）
# 设置环境变量：OPENAI_API_KEY
```

### 第五步：开始使用

```bash
# 启动
opencode

# 输入 ultrawork 或 ulw 进入超工作模式
```

---

## 离线安装（无网络环境）

npm 包包含预编译的各平台 binary（Linux/macOS/Windows），单文件约 50MB。

### 第一步：本地有网机器下载

```bash
# 下载 npm 包（包含所有平台 binary）
npm pack oh-my-opencode
# 输出：oh-my-opencode-3.17.5.tgz（版本号随发布变化）
```

### 第二步：传输到服务器

```bash
scp oh-my-opencode-*.tgz user@server:~/
```

### 第三步：服务器上安装

```bash
# 全局安装（本地 tarball）
npm install -g ~/oh-my-opencode-*.tgz

# 运行安装
oh-my-opencode install --no-tui --claude=max20 --openai=yes
```

### 离线安装 OpenCode 本体

```bash
# 有网机器上下载
npm pack opencode-ai

# 传输后离线安装
npm install -g ~/opencode-ai-*.tgz
```

---

## 代理模型配置

每个代理有独立的模型链路。安装后可在配置文件中自定义：

```json
// ~/.config/opencode/oh-my-openagent.json
{
  "agents": {
    "sisyphus": { "model": "anthropic/claude-opus-4-7" },
    "prometheus": { "model": "openai/gpt-5.4" },
    "oracle": { "model": "openai/gpt-5.4" }
  }
}
```

### 安全替换矩阵

| 代理 | 推荐模型 | 可替换为 |
|-----|---------|---------|
| **Sisyphus** | Claude Opus 4.7 | Sonnet 4.6, Kimi K2.5, GLM 5 |
| **Prometheus** | Claude Opus 4.7 | GPT-5.4（自动切换 prompt） |
| **Oracle** | GPT-5.4 | Claude Opus 4.7 |
| **Atlas** | Sonnet 4.6 | Kimi K2.5, GPT-5.4 |
| **Hephaestus** | GPT-5.4 | **不能替换** |
| **Explore** | Grok Code Fast | MiniMax M2.7, Haiku 4.5 |
| **Librarian** | MiniMax M2.7 | Haiku 4.5, GPT-5-Nano |

---

## 卸载

```bash
# 1. 从配置中移除插件
jq '.plugin = [.plugin[] | select(. != "oh-my-openagent")]' \
    ~/.config/opencode/opencode.json > /tmp/oc.json && \
    mv /tmp/oc.json ~/.config/opencode/opencode.json

# 2. 删除配置文件
rm -f ~/.config/opencode/oh-my-openagent.jsonc \
      ~/.config/opencode/oh-my-openagent.json \
      ~/.config/opencode/oh-my-opencode.jsonc \
      ~/.config/opencode/oh-my-opencode.json

# 3. 验证
opencode --version
```

---

## 快速参考

```bash
# 安装
bunx oh-my-opencode install --no-tui --claude=max20 --openai=yes

# 诊断
bunx oh-my-opencode doctor

# 认证
opencode auth login

# 启动
opencode

# 超工作模式
ulw
```

---

> 完整文档：https://github.com/code-yeongyu/oh-my-openagent
>
> Discord 社区：https://discord.gg/PUwSMR9XNk
