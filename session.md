# Smalux 会话交接记录

日期：2026-09-05

## 本次目标

用户要求把当前架构和最小 Agent <-> Server 联调状态写入文档，并保存会话，方便下一次继续。
用户已明确的设计取舍：

- Agent 必须轻量；不为远程 Job、TaskReport、JobEvent 或 Scheduler 运行状态引入跨进程持久化。
- Agent 重启后，Server 应识别新进程，并重新下发权威 Job/runtime；当前代码使用
  `AgentReconcileSummary.instance_id` 实现该行为。
- 管理 REST/Web UI、指标查询/告警、安装升级、可靠业务 ACK、插件分发暂不实现。
- Plus 当前只保留本地 Worker 的基础能力；仓库、签名、灰度和回滚后续再讨论。

## 当前正式最小闭环

正式 `smalux-server` 与 `smalux-agent` 已能完成：

1. Server 通过本地 IPC CLI 创建一次性 `token_id.psk` 注册凭据。
2. Agent 首次以 XXpsk3 注册，按 pending -> commit -> committed 顺序保存认证身份。
3. 后续启动或短暂断线使用保存的 Agent 私钥与 Server 公钥进行 IK；不再读取 Token。
4. Agent 建立加密 gRPC 长流后上报 reconcile 摘要、远程 Job policy、capability 和 plugin inventory。
5. Server 用数据库中的权威 catalog/runtime 比较摘要，必要时下发完整 `ReplaceAllJobs`。
6. Agent `RemoteJobController` 校验/编译 Job 并更新 Scheduler；Task 生成 TaskReport，Scheduler 生成
   JobEvent。
7. Server 以幂等身份保存 TaskReport/JobEvent；JobCommandResult 触发状态更新或重新同步。
8. Agent 重启时 `instance_id` 改变，Server 强制完整对账并下发权威目录；Agent 不持久化远程 Job。

关键源码位置：

- Agent 进程装配、事件循环、reconcile 与 outbox：`crates/smalux-agent/src/main.rs`
- Agent 持久化身份模型：`crates/smalux-agent/src/client/state/model.rs`
- XX/IK 建立和 pending 恢复：`crates/smalux-agent/src/client/connection.rs`
- 有界内存 outbox：`crates/smalux-agent/src/outbox.rs`
- Server Session 业务循环和 Job 下发：`crates/smalux-server/src/service/agent/transport/session/business.rs`
- Server 进程实例检测：`crates/smalux-server/src/service/agent/session_registry.rs`
- TaskReport/JobEvent 幂等数据库写入：`crates/smalux-server/src/database/task_report.rs`、`job_event.rs`
- reconcile wire schema：`crates/smalux-protocol/proto/smalux/agent/v1/reconciliation.proto`

## 本次仓库文档变更

修改了已有文档，使其与当前代码一致：

- `README.md`：不再声称已有 Web 管理界面或尚未定义的 gRPC 合约，正确说明 Server 控制面、
  Protocol、数据持久化和 Docusaurus 文档站。
- `website/docs/installation/server.md`：标明正式控制面已经包含注册、Job 对账、报告/事件入库和本地
  CLI；修正默认监听地址为 `127.0.0.1:12345`；删除“CLI 覆盖入口尚未实现”。
- `website/docs/getting-started/quick-start.md`：新增正式 Agent/Server 最小联调步骤；Protocol Example
  保留为协议学习入口，而非正式联调唯一入口。
- `website/docs/installation/agent.md`：明确认证身份是持久化边界；Job/运行态/报告是进程内状态；
  明确内存队列满时丢弃最旧项，重启后由 Server 对账恢复 Job。
- `website/docs/getting-started/overview.md`：更新 Protocol 职责为认证、加密传输和状态机；更新为
  重启后从 Server 权威目录恢复。
- `website/docs/reference/project-status.md`：将本地持久化 outbox/业务 ACK 标为可选后续能力。
- `website/docs/intro.md`：同步 Server 已有控制面和持久化能力，Web 管理产品仍未实现。

新增文档：

- `website/docs/reference/agent-server-runtime.md`：详述模块职责、状态所有权、XX 注册、IK 重连、
  reconcile、Job 下发、TaskReport/JobEvent 幂等、断线/重启、手工最小联调、故障定位、当前保证与
  不保证、自动化测试和源码阅读入口。
- `website/sidebars.ts`：已将新页面加入“参考”分类。

## 手工联调步骤

需要恢复本机 Rust toolchain 后，在仓库根目录：

```powershell
# 终端 1
$env:RUST_LOG = "smalux_server=info,smalux_protocol=info"
cargo run -p smalux-server -- run

# 终端 2：创建一次性 Token。完整凭据只出现一次。
cargo run -p smalux-server -- registration-token create `
  --agent-name edge-agent `
  --credential-file C:/smalux/edge-agent.token

# 终端 3：首次 XXpsk3 注册
$env:RUST_LOG = "smalux_agent=info,smalux_protocol=info"
cargo run -p smalux-agent -- run `
  --server-endpoint http://127.0.0.1:12345 `
  --token-file C:/smalux/edge-agent.token
```

观察状态：

```powershell
cargo run -p smalux-server -- agent list --online
cargo run -p smalux-server -- session list
cargo run -p smalux-agent -- status
cargo run -p smalux-agent -- identity show
```

停止并再次启动 Agent 时不要传 Token；预期连接模式应为 IK，且因新进程实例触发 Server 完整对账。

## 验证状态

本次已经通过：

- `pnpm --dir website build`
- `pnpm --dir website typecheck`
- 新文档链接/侧栏引用检查
- 修改文档 UTF-8 无 BOM 检查
- Markdown `git diff --check` 无实际内容错误（Git 因整个工作树既有 LF/CRLF 配置输出警告）

Rust 检查在当前系统未执行，原因是环境而非代码：

- `rustup toolchain list` 显示无已安装 toolchain。
- `cargo` 无默认 toolchain。
- `cargo +stable` 下载/同步时无法创建 `C:/Users/19766/.rustup/tmp/...` 临时文件，返回 Access Denied。

恢复 toolchain 与目录权限后，必须执行：

```powershell
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

重点闭环测试：

```powershell
cargo test -p smalux-server encrypted_agent_executes_server_job_and_persists_report -- --nocapture
cargo test -p smalux-server official_agent_client_registers_then_reconnects_with_saved_identity -- --nocapture
```

注意：之前尝试附加 `--exact` 时使用了不含模块路径的测试名，Cargo 筛选到 0 个测试；下一次应使用
上面的子串筛选命令，或先列出完整测试名后再做精确过滤。

## 工作树注意事项

- 工作树在本次开始前已大量 dirty；不要回退或整批格式化无关文件。
- 本次只修改 root README、website 文档和 `website/sidebars.ts`，并新增
  `website/docs/reference/agent-server-runtime.md` 与本根目录 `session.md`。
- 数据库、Agent/Protocol/Plus 代码的大量修改和未跟踪文件来自此前开发，继续工作时必须先读当前内容。
- 所有本次接触的中文 Markdown 都是 UTF-8 无 BOM。

## 下一步建议

1. 修复 Rust toolchain 的安装/临时目录权限，然后运行完整 workspace 验证。
2. 按 [Agent 与 Server 运行闭环](website/docs/reference/agent-server-runtime.md) 的联调章节执行一次真实
   Server/Agent 联调，重点观察 XX -> IK 和重启后 reconcile。
3. 若需要人工下发 Job，优先补一个受控的 JobDefinition 生成/查看 CLI 或测试 fixture；当前
   `server job replace` 仅接收编码后的 protobuf 文件。
4. 不要在未重新讨论需求前实现磁盘 outbox、业务 ACK、Web 管理 API、插件分发或多节点同步。

## Suggested skills

- `diagnose`：Rust toolchain/权限问题，或真实联调失败时执行“复现 -> 最小化 -> 假设 -> 修复 -> 回归”。
- `tdd`：后续添加 JobDefinition 生成工具、管理 API 或可靠上报能力时先补失败测试。
- `improve-codebase-architecture`：准备重构跨 crate 调用关系、降低调用深度时使用。
- `handoff`：下一次需要再次中断并交接时，更新临时会话记录。
