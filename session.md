# Smalux Session Handoff

## 恢复目标

用于在其他电脑或新会话中快速恢复当前开发上下文。

- 项目路径：`F:/code/rust/smalux`
- 当前分支：`dev`
- 当前重点：继续推进 `smalux-server`

## 当前工作区

工作区当前有未提交改动，主要集中在 `crates/smalux-server`，同时也有文档更新。

最近一轮文档整理已经把长篇 README 收敛为：

- 根 README：workspace 总览。
- crate README：各 crate 职责、入口和扩展边界。
- `crates/smalux-server/plan.md`：server 实现协议和参数速查。
- `session.md`：恢复上下文。

恢复后先执行：

```powershell
git status --short
cargo fmt --all --check
cargo check --workspace
cargo test -p smalux-core
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo test -p smalux-server
```

如果 `git status --short` 里出现你不认识的文件，不要直接回滚，先确认来源。

## 当前项目状态

| crate | 状态 |
| --- | --- |
| `smalux-agent` | 主体已实现：采集、导出、动态配置、Komari、remote task/job/shell |
| `smalux-core` | 共享模型、日志和脱敏工具已可复用 |
| `smalux-protocol` | frame、codec、wire、secure_psk 已集中 |
| `smalux-server` | 正在开发：CLI/config/bootstrap/DB/最小 axum 已有，业务闭环未完成 |

## 关键设计结论

- 自有协议主连接路径：`/agent/v1/connect`。
- 自有协议远程探测统一到通用 `job_apply(kind=probe)` / `job_result(kind=probe)`。
- 第三方兼容只放 adapter，不污染自有协议模型。
- agent 上报由采集事件驱动，不再用单一 reporter interval 决定所有数据发送。
- remote shell / remote task 的启用开关是 agent CLI-only，server 运行时不能动态打开。
- 日志级别统一使用 `RUST_LOG`，日志内容用英文，代码注释用中文。
- server token/key 后续由添加 agent 流程生成并存数据库，不放 server CLI。
- server 前端槽位分为 `site` 和 `admin`，支持 `embedded`、`directory`、`external`。

## 当前 server 结构

```text
src/
  bootstrap.rs       # 启动编排
  state.rs           # AppState
  cli/args.rs        # ServerArgs
  config/            # defaults/model/validation
  http/              # router/middleware/agent/web
  service/           # agent/web/event
  storage/           # database URL/init/migration/repository
```

当前真实路由：

- `GET /agent/v1/connect`
- `GET /api/v1/health`
- `GET /api/v1/realtime/*`
- `/`
- `/admin`
- `/assets/site/*`
- `/assets/admin/*`

## 文档入口

- 项目总览：[README.md](README.md)
- agent：[crates/smalux-agent/README.md](crates/smalux-agent/README.md)
- core：[crates/smalux-core/README.md](crates/smalux-core/README.md)
- protocol：[crates/smalux-protocol/README.md](crates/smalux-protocol/README.md)
- server：[crates/smalux-server/README.md](crates/smalux-server/README.md)
- server 协议字段速查：[crates/smalux-server/plan.md](crates/smalux-server/plan.md)

## 下一步建议

1. 完成 server agent 认证和 `secure_psk` responder。
2. 完成 `/agent/v1/connect` wire/frame 读写循环。
3. 接收 `snapshot` / `heartbeat` 并写入 latest state。
4. 增加 REST 查询。
5. 增加命令下发和 ack/result 回收。
6. 再补 dashboard realtime、session、权限和审计。

## 协作约定

- 始终用简体中文沟通。
- 代码标识符、命令、日志、报错保留原文。
- 代码注释用中文，日志内容用英文。
- 修改文件优先使用 `apply_patch`。
- 不回滚用户未明确要求回滚的改动。
