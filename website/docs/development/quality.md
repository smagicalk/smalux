---
title: 测试与质量检查
description: 本地测试、Clippy、Rustdoc 和文档站构建流程。
---

# 测试与质量检查

## Rust 格式

检查格式而不修改文件：

```powershell
cargo fmt --all -- --check
```

需要格式化时：

```powershell
cargo fmt --all
```

## 编译检查

```powershell
cargo check --workspace --all-targets
```

`--all-targets` 会包含 library、binary、tests 和 examples，能发现普通 `cargo check` 遗漏的示例错误。

## 测试

```powershell
cargo test --workspace --all-targets
```

Protocol 的关键测试至少应覆盖：

- XXpsk3 正确/错误 PSK；
- IK 双方静态身份认证；
- 注册 pending/commit/committed 和断线恢复；
- 半握手关闭、超时和安全错误；
- Driver 强类型收发、心跳和 rekey；
- Job/Task Proto round-trip；
- 静态密钥轮换 snapshot 与 promotion。

Agent 的关键测试至少应覆盖：

- 配置校验和 include/exclude 优先级；
- Scheduler 时序、队列、重试、取消和 panic 隔离；
- Job 命令幂等、revision 冲突和远程所有权；
- Collector 首次采样、增量、截断和平台不可用状态；
- Probe 并发、超时和部分失败。

### 分层测试策略

| 层 | 首选测试 | 重点 |
| --- | --- | --- |
| Collector | 单元测试或平台条件测试 | 原始读取、权限和不可用状态。 |
| Task | 使用固定输入/假 Collector | 配置、筛选、首次采样和结果转换。 |
| Scheduler | Tokio 时间暂停测试 | 时序、misfire、重试、取消和容量。 |
| RemoteJobController | 内存 Scheduler/Factory | revision、幂等、ReplaceAll 原子性。 |
| Noise | 内存帧往返 | 握手阶段、nonce、rekey 和错误帧。 |
| Tonic | 本机临时 listener | HTTP/2 流、超时、半关闭与 Driver。 |
| Example | 进程级 smoke test | REST/gRPC 共端口和持久化恢复。 |

测试名称应描述“条件 + 行为”，例如 `rejects_stale_catalog_revision`，而不是笼统的 `test_job`。平台相关
测试要明确跳过原因；不能把所有错误吞掉后让测试永远通过。

## Clippy

```powershell
cargo clippy --workspace --all-targets -- -D warnings
```

`-D warnings` 让新增警告直接失败。Windows MSVC 链接器输出本地化的“正在创建库”信息时，Rust 可能报告
`linker_messages` warning；应区分工具链提示和源码 lint，但 CI 环境仍应保持无源码警告。

## Rustdoc

```powershell
cargo rustdoc -p smalux-protocol --lib -- -D warnings
```

Protocol 是跨模块公共边界，公开类型和方法需要比内部实现更完整的文档，并确保 intra-doc link 有效。

建议的提交前顺序是：格式检查 → workspace check → workspace test → Clippy → Protocol rustdoc → 网站
typecheck/build。前一项失败就先修复再继续，这样能快速区分格式、类型、行为、lint 和文档链接问题。

## 文档站

```powershell
Set-Location website
pnpm install --frozen-lockfile
pnpm typecheck
pnpm build
```

Docusaurus 配置 `onBrokenLinks: 'throw'`，内部页面链接错误会让构建失败。Markdown 链接 warning 也应在提交前
处理，不能依赖线上点击后才发现。

## 修改范围检查

```powershell
git status --short
git diff --check
git diff --stat
```

提交前确认没有误提交 `target/`、`website/node_modules/`、站点 `build/`、IDE 配置或私钥文件。

## 三轮验证

一次较大修改至少从三个角度验证：第一轮运行目标模块测试，第二轮运行 workspace 全目标，第三轮执行
Clippy/Rustdoc/文档构建和 diff 检查。三轮不是机械重复同一命令，而是逐步扩大覆盖面并检查不同风险。
