# Smalux 会话交接记录

## 最新进度：后端首批 Web 只读 API

- 用户确认后端优先、前端后适配；首批范围为 Agent/Job/Report/Event 核心闭环及既有 CPU/内存指标。前端 API 可以不同，暂不改前端。新增数据库迁移需另行确认。
- 本批只增加只读 Web 查询，不新增 DB 表或迁移，复用 AdminService、Job catalog、TaskReport/JobEvent 表和已有 Agent 会话目录。
- JSON-RPC 新增 `agent.list/get`、`job.list/get`、`report.list`、`event.list`；同源会话认证继续沿用 Cookie/Origin/CSRF 边界。Agent 查询使用真实 ID/展示名称/授权状态/在线状态；Job 只读权威 catalog；Report/Event 仅返回摘要，不暴露原始 Proto payload。
- Report/Event 查询支持 Agent/Job、毫秒时间窗 `[fromMs,toMs)`、有界限流及稳定复合时间+记录 ID 倒序分页；DB 分页查询保留旧 CLI 入口。Agent ID cursor 为排序键；Report/Event cursor 当前 wire 格式为 `timestampMicros|recordId`，非签名凭据。
- Interim Web DTO 与 `WEB_API.md` 中完整目标 VO 尚不同：Agent 列表使用 `limit/after`，Job 当前 summary 有 ID/revision/enabled/taskKind，report/event 是摘要分页。Job detail 不向 Web 暴露 Protobuf；写 API、Operation、完整 resource ACL、Job 参数结构编辑和前端接入均未实现。对应边界已在 `WEB_API.md` §5 当前过渡契约中说明。
- 本次后端验证：`cargo test -p smalux-server --lib --locked --offline -j 1` **132/132 通过**；`cargo fmt --all -- --check`、`cargo clippy -p smalux-server --lib --locked --offline -j 1 -- -D warnings`、Server 正式二进制构建均通过。全工作区测试仍待资源充足时验证，既有 os error 1455 阻塞仍适用。
- 本批未修改前端、未添加迁移或依赖；原有未提交改动全部保留。工作区原已有 Server/Web 未提交和未跟踪内容，本次编辑叠加在其上；未提交/推送。
- 下一步：先评审当前 interim API shape 与 scope，再讨论 Job 写 API/持久化 operation 设计；若需要 migration，单独征求批准后再改。之后再由前端按冻结契约接入。

### 下次继续入口

核心 Web handler：`crates/smalux-server/src/web_auth.rs`；Agent/Job 只读回归：`crates/smalux-server/src/web_auth/regression.rs`；稳定 Report/Event 游标 DB 查询与测试：`crates/smalux-server/src/database/task_report.rs`、`job_event.rs`；既有 Agent/Job 查询服务：`crates/smalux-server/src/management/service.rs`；指标与 WS：`crates/smalux-server/src/web_metrics.rs`、`web_auth/ws.rs`。


## 上一批进度：R0/R1 真实登录基础子集

- 用户已批准并实现：本地 `auth bootstrap --username` 隐藏交互初始化、Argon2id、独立用户/会话摘要/安全事件/bootstrap claim 迁移；登录、恢复会话、退出、meta 与已认证 `session.info`。
- Web 默认关闭。生产 HTTPS 同源反代、Server 回环监听；显式开发模式才允许回环 HTTP。精确 Origin、JSON、自定义客户端头、退出 CSRF、Cookie flags、8 KiB 请求限制、30 次/分钟全局限流与 2 并发哈希均已实现。默认会话 absolute=24h、idle=30min，可配置。
- 后端入口 `crates/smalux-server/src/web_auth.rs`，持久化边界 `web_auth/store.rs`，回归 `web_auth/regression.rs`，边界说明 `web_auth/SPEC.md`。启动参数及反代要求见 `crates/smalux-server/README.md`。
- 前端新增 `src/shared/auth/`，真实模式只显示登录/服务端身份与能力，不挂载 Mock 业务；Cookie 同源请求，内存会话、不信任 localStorage 假身份，401/到期撤销显示，网络错误不伪造登录/退出成功。旧显式 Mock 模式保留。
- 最终验证：`cargo test --workspace --locked --offline -j 1` 全通过（336 项，Server 107 项，8 项文档测试忽略）；前端 12 个文件、93/93 项通过，typecheck/build 通过；正式 Server 二进制构建通过。共享启动入口的 Web 安全校验另有回归，CLI 与 library 一致。
- 新认证回归先复现协议/能力目录缺陷再修复；覆盖 Cookie、CSRF、错误凭据、到期/禁用用户、限流、数据库失败回滚、重复/并发 bootstrap、旧 schema 升级保留数据与文件 SQLite 重开。
- 已用隔离 SQLite、真实 Server 进程与同源测试反代验证：初始401、错误密码401、登录200、恢复200、session.info、错误CSRF403、退出200、旧Cookie401。账号使用临时测试夹具，不代表 CLI 隐藏输入交互已实测；测试进程已关闭。
- 环境限制：浏览器工具报告 `browser guest is not available`，预览不支持附加工作区，浏览器交互未验证。生产 HTTPS 反代、PostgreSQL/MySQL 实机、容量/跨平台未验证。构建保留大包/Windows linker 提示；首次并行编译内存不足后以 `-j 1` 成功。
- 仍未实现：用户管理/改密、完整审计管理、WS/MFA/step-up、Agent/Job/报告 Web API。下一步建议按已批准契约推进 R2 最小只读/任务链路，不把登录基础宣称为全部 R0/R1 完成。
- 本批及上一批前端修复、两份 session.md 均留在工作区，未提交/推送。基线仍为后端 `e6aeef1`、前端 `2155b94`。

## 上一批进度：联调基础修复完成

本节是较早批次的历史记录；当前实现范围、环境阻塞及验证结果以文件顶部“最新进度”为准。

### 实现前基线提交

- 后端 `dev`：`e6aeef1`，保存 `WEB_API.md` 设计草案。
- 前端 `vibe-dev`：`2155b94`，保存 `api.md` 接口清单及已有弹窗样式调整。
- 两次提交均未推送。后续联调基础修复仍在前端工作区，尚未提交；本次纪要更新也未提交。

### 本批已完成

用户批准先修联调基础，未开展登录授权、数据库迁移或真实业务 API。

- 前端 `src/shared/api/http/http-client.ts`：`enableMock=false` 时 GET/POST/PUT/DELETE 的网络异常、非 2xx 响应与 JSON 解析错误直接拒绝，不再进入 Mock 回退；保留 HTTP 状态、原生错误及成功请求语义。
- 显式 Mock 模式及现有配置默认值不变；失败写操作不会进入模拟后端修改数据。
- 新增 `src/shared/api/http/http-client.test.ts`，包含 36 项回归；修正 `src/app/config/runtime-config.test.ts` 的默认值期望，并验证显式 `false` 被保留。
- 新增 `src/shared/api/http/SPEC.md` 记录真实 REST 与 Mock 边界。上述路径均相对 `F:/code/node/smalux_frontend`。

### 已执行验证

- 评估阶段：`cargo test --workspace --locked --offline`，324 通过、0 失败，8 项文档测试忽略；包含加密连接下任务执行与报告持久化测试。本批未修改 Rust 源码。
- 前端按失败测试到修复通过执行；最终独立复跑 `pnpm.cmd run test`：8 个文件、77/77 项通过。
- `pnpm.cmd run typecheck`、`pnpm.cmd run build`、`git diff --check` 均通过；相关文件 UTF-8 无 BOM、中文抽样正常。
- 构建仍有压缩后 chunk 超过 500 kB 的警告，未在本批优化。未执行浏览器端到端、生产部署、容量或全平台验证。

### 当前成熟度与下一步

Agent/Server 核心闭环已形成，但 Web 前端仍主要依赖 Mock，真实管理产品尚未接通。普通 HTTP 目前只有 `/api/v1/health`；`WEB_API.md` 为未实现草案，不能计入功能完成度。

下一阶段建议先统一契约，再打通“登录授权 → Agent 列表/详情 → 下发采集任务 → 查询真实报告”的最小 Web 链路；需另行确认安全与数据库设计。指标聚合/保留、告警、Rustic 备份和生产交付仍待后续实现。本批无阻塞。

---

## 历史交接记录

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
