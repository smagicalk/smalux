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
