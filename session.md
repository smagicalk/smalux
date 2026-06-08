# Smalux Session Handoff

## 恢复目标

用于在新电脑或新会话中快速恢复当前开发上下文。项目路径：`F:/code/rust/smalux`。当前分支：`dev`。

## 当前提交

最新已提交并推送：

```text
a51229d refactor agent protocol and server structure
```

恢复时先执行：

```powershell
git pull
git status --short
cargo fmt --all --check
cargo check -p smalux-agent
cargo check -p smalux-server
```

预期 `git status --short` 为空。若不为空，先确认是否是其他机器或用户的新改动，不要直接回滚。

## 当前项目状态

- `smalux-agent`：监控 agent，已有采集、动态配置、导出、Komari 兼容、remote task/probe/shell。
- `smalux-core`：共享模型、日志、脱敏工具和公共工具。
- `smalux-protocol`：共享 `ClientFrame`、`ServerFrame`、wire、secure_psk、remote payload。
- `smalux-server`：当前还是骨架，但目录、依赖、README 和详细实现计划已经建立。

## 关键决策

### 1. server 删除 query 模块

`crates/smalux-server/src/query.rs` 和 `query/` 已删除。

前端读模型不再单独放 `query/`：

- agent 列表、latest、在线状态：`service/agent.rs`
- dashboard 聚合：`service/dashboard.rs`
- REST handler：`http/rest.rs`
- 数据库存取：`storage/repository.rs`

### 2. server HTTP 入口重新设计

server 当前预留结构：

```text
crates/smalux-server/src/
  main.rs
  cli.rs
  cli/args.rs
  config.rs
  config/model.rs
  auth.rs
  auth/agent.rs
  auth/session.rs
  http.rs
  http/router.rs
  http/rest.rs
  http/agent.rs
  http/realtime.rs
  http/frontend.rs
  http/middleware.rs
  ingest.rs
  ingest/frame.rs
  ingest/report.rs
  service.rs
  service/agent.rs
  service/dashboard.rs
  service/command.rs
  storage.rs
  storage/entity.rs
  storage/migration.rs
  storage/repository.rs
```

路径规划：

```text
/agent/v1/connect    # Smalux agent 主 WebSocket
/api/v1/*            # 前端和管理端 REST API
/live/v1/dashboard   # 前端实时通道，后续可用 WebSocket 或 SSE
/                     # React/Vite 静态前端 fallback
```

### 3. agent 已同步 server 主连接路径

自有协议 `smalux_json` 的 agent 主连接路径已经从旧的 `/api/agents/connect` 改为：

```text
/agent/v1/connect
```

关键文件：

- `crates/smalux-agent/src/export/adapter.rs`
- `crates/smalux-agent/src/export.rs`
- `crates/smalux-agent/README.md`
- `crates/smalux-server/README.md`
- `crates/smalux-server/plan.md`

已确认：

```text
rg -F '/api/agents/connect' crates/smalux-agent crates/smalux-server
```

无结果。

Komari 路径保持不变：

```text
/api/clients/report
/api/clients/uploadBasicInfo
/api/clients/task/result
/api/clients/terminal
```

### 4. React 前端托管规则

最终决策：

```text
如果编译了 frontend-embed：
  frontend enabled 时直接使用内置前端。

如果没有编译 frontend-embed：
  frontend enabled 时使用 frontend-dir 目录。

如果 frontend disabled：
  server 不托管前端，适合前端单独部署。
```

不再建议 `--frontend-mode disabled|dir|embedded`。

建议 CLI：

```text
--frontend-enabled
--frontend-dir
--frontend-spa-fallback
```

配置模型建议：

```rust
pub struct FrontendConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub spa_fallback: bool,
}
```

需要后续同步 `crates/smalux-server/README.md` 和 `crates/smalux-server/plan.md`，把旧的 `FrontendMode::Disabled | Dir | Embedded` 改成这套规则。

## server 计划文档

详细 server 实现计划已经写入：

```text
crates/smalux-server/plan.md
```

里面覆盖：

- CLI/config
- 日志和 bootstrap
- MemoryRepository
- SQLite/SeaORM
- agent 认证
- `/agent/v1/connect`
- ingest snapshot/delta/heartbeat
- command 下发和 ack/result/error
- REST API
- frontend realtime
- React/Vite 托管
- session
- Komari 兼容
- 安全、性能、测试和多轮检查流程

## 当前验证结果

最近验证通过：

```powershell
cargo fmt --all --check
cargo check -p smalux-agent
cargo check -p smalux-server
cargo test -p smalux-agent
```

`cargo test -p smalux-agent` 结果：

```text
278 passed, 4 ignored
```

## 下一步建议

优先做 server：

1. 更新 `crates/smalux-server/README.md` 和 `plan.md` 的前端托管规则，去掉运行时 `frontend-mode` 枚举。
2. 实现 `cli/args.rs` 和 `config/model.rs`。
3. `main.rs` 改成 async bootstrap。
4. 实现最小 `GET /api/v1/health`。
5. 实现 `/agent/v1/connect` 的最小 WebSocket upgrade。
6. 先用 `MemoryRepository` 跑通 agent snapshot/heartbeat 到 latest state。
7. 再接 REST `GET /api/v1/agents` 和 `GET /api/v1/agents/{agent_id}/latest`。

## 协作注意

- 始终使用简体中文沟通；代码标识符、命令、日志、报错保持原文。
- 代码注释用中文，日志内容用英文。
- 修改含中文文件前检查 BOM；当前 `session.md` 是 UTF-8 无 BOM。
- 修改现有文件优先用 `apply_patch`。
- 不要回滚用户未明确要求回滚的改动。
- 新功能同步更新 README、测试和本文件。
