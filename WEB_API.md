---
title: Smalux Web API v1 设计参考
status: draft / proposed
apiVersion: v1
designVersion: 1.0.0
implementation: 未实现；本文为设计草案，不是可调用接口清单
scope: 单 Server 管理域；首版按 Rust 优先分期
---

# Smalux Web API v1 设计参考

> **草案（draft / proposed）**。本文只定义拟议契约、权限和安全边界，不代表任一 Web API 已实现或可通过 `curl` 调用。普通 HTTP 当前仅有 `/api/v1/health`；Agent gRPC 与本地 CLI 属于既有通道，不因此获得本文定义的 Web 能力。前端 `api.md` 是 Mock 需求索引，不是后端实现证据。
>
> v1 是面向单个 Smalux Server 管理域的 Web 契约版本；Agent 是被管理主机，不是 Server 实例。本文推荐方案尚待设计验收，特别是部署域名、保留期、会话期限、角色策略细节和具体生产容量需在实施前确认。本文不含代码、数据库迁移或依赖变更。

## 1. 目标、原则与非目标

### 1.1 目标

- 定义可供 Rust 应用服务与后续前端适配共同遵循的 Web DTO/VO，不直接暴露 Rust Entity、Agent Proto 或 IPC 请求。
- 先建立正确的领域模型、写入验证、持久化操作和真实查询，再由前端适配旧 UI 字段；不为了兼容 `serverId` 等旧字段扭曲领域身份。
- 首版闭环：用户会话、Agent 安全接入、Agent 元数据、采集/Probe Job、已安装 Plus runtime、真实操作状态、真实报告和受限指标查询。
- 同时完整规划主题包和安全预览/公开站点，以及告警、通知、备份、审计、部署状态、计费配置等后续领域；未到阶段的 API 不提供伪成功实现。
- 保留现有 Agent gRPC、Noise 注册语义和 CLI IPC；Web 调用共用 Rust 业务规则，不启动 CLI 子进程、不把整个 IPC 暴露到网络。

### 1.2 架构与通道

```text
浏览器 / 后续 API client
  ├─ REST：会话认证、meta、二进制上传下载、公共只读站点
  ├─ POST /api/v1/rpc：全部普通管理查询与写入（JSON-RPC 2.0）
  └─ /api/v1/ws：仅订阅控制、心跳和服务端通知
             │
Rust Web DTO/鉴权/验证/审计
             │
共用领域、应用、查询服务（CLI 与 Web 复用业务规则）
  ├─ 持久化领域状态、操作与查询投影
  └─ 既有 Agent gRPC / Noise / Scheduler / TaskReport
```

Rust 领域/应用/查询层是业务逻辑权威，Web DTO/VO 是边界适配。Web 传输不得以 RPC 包装任意 CLI 命令，也不得为 UI 凭空构造状态。REST 普通管理 CRUD 不与 RPC 并行提供第二套语义。WebSocket 不接受普通写入，也不允许客户端在 WS 失败时自动将写请求重发至 HTTP；写操作始终经 HTTP RPC。

REST `/api/v1/health` 和 Agent gRPC 保持既有用途。静态管理页面不改变 API 契约；错误 API 路由不得回退成 SPA HTML。静态嵌入/部署模式是构建与部署配置，不提供运行时切换 API。

### 1.3 明确非目标

- 本草案不实现接口，不保证任意方法已存在，不代表前端 Mock 页面已对接真实服务。
- 不承诺首版多租户、多 Server 协调、插件市场/服务器端插件二进制安装、公开默认管理员密码、客户端自授予管理员权限。
- 首版不提供 Shell、任意命令、审批、脚本执行、RunNow 重放或任意 URL 抓取。
- 用户主题是静态 Web 展示包，不是 Rust/Plus 插件；不能接管管理控制台、登录或系统操作。
- Agent 仍是轻量端；本文不要求引入 Agent 磁盘 outbox、跨重启调度恢复或不可能保证的副作用 exactly-once。

## 2. 版本、身份、标量与默认值

### 2.1 标识及时间

| 字段 | 类型 | 语义 |
| --- | --- | --- |
| `agentId` | UUID 字符串 | 唯一 Agent 主机身份；不表示 Server 服务实例。 |
| `jobId` | UUID 字符串 | Agent 的一个期望任务定义。 |
| `operationId` | UUID 字符串 | 一次可查询的持久化配置/控制操作。 |
| `requestId` | JSON-RPC `id` 或服务跟踪标识 | 单次协议请求关联；不用于业务去重。 |
| `clientMutationId` | UUID 字符串 | 客户端为一次写入意图生成并在重试中保持不变。 |
| `revision` | 十进制字符串 | 单调版本号，避免 JavaScript 64 位精度损失。比较由 Server 执行。 |
| 时间点 | `integer` | UTC Unix 毫秒；有限安全整数。字段名以 `Ms` 结尾。 |
| 日期 | `YYYY-MM-DD` 字符串 | 仅日历日期，不是时间戳。 |

UUID 在传输中使用规范小写带连字符格式。`requestId` 允许字符串或整数，写接口必须带 ID 且不接受 JSON-RPC notification。所有请求 DTO 拒绝未知字段；只读响应可增加可选字段。未知必需枚举值按协议错误拒绝，不能猜测降级。

### 2.2 数值、空值和数据质量

- 字节量字段明确使用 `Bytes`；速率字段明确使用 `Bps`（bytes per second）。可安全由 JS 精确表示的容量/速率使用非负 `integer <= 9007199254740991`；精确累计计数使用十进制字符串 `UInt64String`，不得先转浮点数。无法表示的值返回 `null` 并给出质量原因，不截断或饱和。
- `cpuUsagePercent` 为 `[0,100]` 的有限数；其它百分比字段也定义范围。进程 CPU 等可超过 100 的度量不得复用此字段。
- `null` 表示当前值不可用，并应伴随 `quality`/`reason`；真实 `0` 仍为零。`unknown`、`unavailable`、`stale`、`warmingUp` 不得被替换为假零或示例数据。
- 列表使用稳定排序；可选 `sortBy` 必须来自该方法枚举白名单，排序末尾追加稳定 ID。请求筛选决定分页和游标范围。
- `page` 默认 `1`，`pageSize` 默认 `20`，允许 `1..100`；除专门说明外排序按 ID 稳定升序。流水默认 `limit=50`，允许 `1..100`，用绑定筛选条件的不透明游标翻页。
 - 历史时间范围默认最近 `1h`，最大跨度 `7d`；默认 `maxPoints=500`，允许 `1..1000`。超过边界返回验证错误，不静默截断。图表空桶为 `null`。
 - 默认请求值集中于本节；实现不得让前后端各自采用不同默认。部署可降低而不可越过安全上限。
### 2.3 JSON 值

本文 `JsonObject` 是有限深度、有限字节数的 JSON 对象；键为字符串，值仅可为 `null | boolean | finite number | string | JsonValue[] | JsonObject`。拒绝 NaN、Infinity、重复键、超深嵌套和超过对应方法限制的正文。涉及配置的每个领域还需专用 Schema 验证，通用 JSON 类型不代表接受任意配置。

## 3. JSON-RPC、错误和权限约定

### 3.1 HTTP RPC envelope

所有普通业务 RPC 使用 `POST /api/v1/rpc`，`Content-Type: application/json`，单一 JSON-RPC 2.0 请求对象。首版不接受 batch。成功为 `{ "jsonrpc":"2.0", "id":..., "result": ... }`，不再套 `ok: true`。失败为标准 `error`，协议错误码沿用 JSON-RPC 约定：`-32700` parse、`-32600` invalid request、`-32601` method not found、`-32602` invalid params、`-32603` internal error。HTTP 401/403/413/429 用于认证、授权、传输体积和限流；通过 HTTP envelope 后的领域失败一律用下表固定 JSON-RPC code 和稳定 `kind`，HTTP 状态仍为 200。代理/传输层错误不映射成领域 kind，同一 kind 跨版本不复用编号。

```json
{
  "jsonrpc": "2.0",
  "id": "req-uuid",
  "error": {
    "code": -32009,
    "message": "资源版本已变化",
    "data": {
      "kind": "REVISION_CONFLICT",
      "requestId": "req-uuid",
      "traceId": "trace-id",
      "fieldErrors": [],
      "currentRevision": "12"
    }
  }
}
```

`fieldErrors` 的元素为 `{path:string, code:string, message:string}`；路径采用 DTO JSON Pointer 风格。`traceId` 仅用于关联服务端诊断。响应不得包含堆栈、SQL、内部路径、凭据、Cookie 或未授权字段。

### 3.2 稳定错误分类

| `kind` | JSON-RPC code | 语义 | 适用范围 |
| --- | ---: | --- | --- |
| `UNAUTHENTICATED` | `-32001` | 没有有效会话 | HTTP 401 或 RPC 会话失效。 |
| `FORBIDDEN` | `-32002` | 身份有效但缺少权限/资源授权 | HTTP 403 或 RPC 领域错误。 |
| `VALIDATION_FAILED` | `-32003` | 字段、范围、状态前置条件不成立 | 含 `fieldErrors`。 |
| `NOT_FOUND` | `-32004` | 资源不存在或不可见 | 防泄露时统一处理。 |
| `REVISION_CONFLICT` | `-32009` | CAS 期望版本不匹配 | 写操作无部分变更，保留示例编号。 |
| `IDEMPOTENCY_CONFLICT` | `-32010` | 同业务键对应不同规范化输入 | 不覆盖原操作/回执。 |
| `CAPABILITY_UNSUPPORTED` | `-32011` | Agent/插件/协议明确不支持能力 | 不伪造应用。 |
| `POLICY_BLOCKED` | `-32012` | 策略拒绝，需证据支持 | 保留期望配置并呈现真实阻断。 |
| `RATE_LIMITED` | `-32013` | 会话、登录、上传或接口被限流 | HTTP 429 或 RPC。 |
| `PAYLOAD_TOO_LARGE` | `-32014` | 正文/文件超出上限 | HTTP 413 或 RPC；创建资源前拒绝。 |
| `IDEMPOTENCY_KEY_EXPIRED` | `-32015` | 上传/秘密恢复回执超期 | 查询既有结果，不自动新签发。 |
| `STATE_CONFLICT` | `-32016` | 生命周期或引用条件不满足 | 如删除仍激活/被引用版本。 |
| `UNSUPPORTED_FEATURE` | `-32017` | 当前部署未提供该阶段能力 | 不得成功空壳或回退 Mock。 |
| `INTERNAL_ERROR` | `-32018` | 未分类服务端故障 | 不含实现细节，带 `requestId/traceId`。 |

HTTP 401/403/413/429 是认证、授权、传输体积和限流边界；已通过 HTTP 的方法级领域错误使用 JSON-RPC error。网络超时只表示结果未知，客户端应以同一幂等键重试或查询操作，不能提示确定回滚。

### 3.3 角色、权限和敏感数据

单管理域内置 `admin`、`operator`、`viewer`。授权始终由 Server 依据会话、方法、目标资源和当前配置检查；客户端角色、菜单隐藏、`localStorage` 和传入的 `actorId` 均不构成授权。

| 能力 | `admin` | `operator` | `viewer` |
| --- | --- | --- | --- |
| 读取基础 Agent、允许范围的最新/历史 summary | 是 | 是 | 是 |
| Agent 注册凭据签发、用户/会话管理、Agent 吊销 | 是 | 否 | 否 |
| 创建/编辑/停用采集或 Probe Job | 是 | 受策略限制 | 否 |
| Plus runtime 秘密读写、主题发布/激活、备份恢复 | 是 | 默认否 | 否 |
| 进程/Socket 详细信息、原始报告、私网地址 | 显式授权 | 默认否 | 默认否 |
| 审计完整流水 | 是 | 否 | 否 |

`admin`/`operator`/`viewer` 是最小内置角色，不定义可由客户端任意扩展角色。敏感 Plus 配置、注册凭据明文、秘密字段、原始进程/Socket/插件结果不因拥有 viewer 身份而自动可读。未来 API token 的用途和 scopes 与 Agent Noise 注册凭据无关。

## 4. DTO 与响应值对象目录

方法表引用本节定义的 VO；每行列出必需字段，除注明可选外均必需。`nullable` 表示字段必需但值可为 JSON null。标识为 UUID 字符串或领域定义的不透明字符串；revision、精确累计计数为十进制字符串；时间为 UTC Unix 毫秒安全整数。未注类型的 `name/kind/state/code/reason/slug` 等均为字符串；`T[]` 表示数组；复杂对象引用 VO 或 §2.3 的 `JsonObject`。请求端单独注明的 optional 字段除外。

### 4.1 通用、会话和操作

| VO | 必需字段及类型 | 枚举/约束 |
| --- | --- | --- |
| `Page<T>` | `items:T[]`, `page:integer`, `pageSize:integer`, `total:UInt64String` | 精确总数。 |
| `CursorPage<T>` | `items:T[]`, `nextCursor:string|null`, `hasMore:boolean` | cursor 绑定筛选、排序和授权域。 |
| `FieldError` | `path:string`, `code:string`, `message:string` | 不返回敏感输入。 |
| `Role` | `string`（别名） | JSON 字符串枚举 `admin`、`operator`、`viewer`，不是 `{value:...}` 对象。 |
| `SessionView` | `userId:string`, `username:string`, `role:Role`, `csrfToken:string`, `expiresAtMs:integer`, `idleExpiresAtMs:integer`, `permissions:string[]` | 不含 Cookie/session token。 |
| `UserSummary` | `userId:string`, `username:string`, `role:Role`, `enabled:boolean`, `revision:revision`, `createdAtMs:integer`, `lastLoginAtMs:integer|null` | 不含密码哈希/MFA secret。 |
| `SessionSummary` | `sessionId:string`, `createdAtMs:integer`, `lastSeenAtMs:integer`, `expiresAtMs:integer`, `current:boolean`, `userAgentSummary:string|null` | 不含原始 IP/token。 |
| `MetaView` | `apiVersion:string`, `designContractVersion:string`, `transports:string[]`, `health:string` | transport 拟定值 `http-json-rpc`、`websocket`。 |
| `FeatureState` | `name:string`, `state:string`, `reason:string|null` | state=`available`、`planned`、`unsupported`。 |
| `MethodState` | `method:string`, `state:string`, `permission:string`, `inputSchemaVersion:string` | 未实现时标 planned/unsupported。 |
| `FeatureCatalog` | `features:FeatureState[]`, `rpcMethods:MethodState[]` | 仅登录后读取。 |
| `OperationState` | `string`（别名） | JSON 字符串枚举 `pending`、`waitingAgent`、`sent`、`applied`、`blocked`、`rejected`、`superseded`、`unknown`，不是 `{value:...}` 对象。 |
| `OperationView` | `operationId:string`, `kind:string`, `targetId:string`, `state:OperationState`, `createdAtMs:integer`, `updatedAtMs:integer`, `expectedRevision:revision|null`, `appliedRevision:revision|null`, `commandId:string|null`, `reason:string|null`, `retryable:boolean` | Agent apply 操作见 §5.2；retryable 不承诺 exactly-once。 |
| `OperationDetail` | `operation:OperationView`, `desiredDigest:string|null`, `sentAtMs:integer|null`, `ackAtMs:integer|null`, `evidence:string[]`, `supersededByOperationId:string|null` | 不含秘密配置。 |
| `AuditEntry` | `auditId:string`, `atMs:integer`, `actorUserId:string|null`, `actorName:string`, `action:string`, `targetType:string`, `targetId:string|null`, `outcome:string`, `requestId:string|null`, `summary:string` | outcome=`succeeded`、`rejected`、`failed`；脱敏且只读。 |

### 4.2 Agent、Job、报告

| VO | 必需字段及类型 | 枚举/约束 |
| --- | --- | --- |
| `EnrollmentSummary` | `enrollmentId:string`, `label:string`, `state:string`, `createdAtMs:integer`, `expiresAtMs:integer`, `consumedAtMs:integer|null`, `agentId:string|null`, `revision:revision` | state=`active`、`consumed`、`expired`、`revoked`；不含凭据。 |
| `AgentSummary` | `agentId:string`, `displayName:string`, `labels:string[]`, `region:string|null`, `note:string|null`, `status:string`, `lastSeenAtMs:integer|null`, `metadataRevision:revision`, `capabilityObservedAtMs:integer|null` | status=`online`、`offline`、`unknown`、`revoked`。 |
| `AgentDetail` | `agent:AgentSummary`, `version:string|null`, `os:string|null`, `arch:string|null`, `addressSummary:string|null`, `createdAtMs:integer`, `revokedAtMs:integer|null` | 地址按权限脱敏。 |
| `Capability` | `name:string`, `version:string|null`, `support:string`, `observedAtMs:integer|null` | support=`supported`、`unsupported`、`unknown`。 |
| `CapabilityView` | `agentId:string`, `observedAtMs:integer|null`, `freshness:string`, `capabilities:Capability[]` | freshness=`fresh`、`stale`、`unknown`。 |
| `AgentPolicyView` | `agentId:string`, `observedAtMs:integer|null`, `freshness:string`, `allowedTaskKinds:string[]|null`, `deniedTaskKinds:string[]|null`, `source:string` | source=`agent-session-observation`；非持久策略快照。 |
| `JobTask` | `kind:string`, `config:JsonObject` | 版本化 kind 各有独立 schema。 |
| `JobSchedule` | `type:string`, `everyMs:integer|null`, `atMs:integer|null`, `cron:string|null`, `timeZone:string|null` | 所有字段必需；type=`interval`、`once`、`cron`；不适用字段必须显式为 `null`；cron 六段含秒与 IANA 时区。 |
| `MisfirePolicy` | `behavior:string` | `skip`、`runOnce`、`catchUpBounded`；逐项能力校验。 |
| `JobDraft` | `name:string`, `task:JobTask`, `schedule:JobSchedule`, `misfire:MisfirePolicy`, `enabled:boolean` | 不含 ID/revision。 |
| `JobView` | `jobId:string`, `agentId:string`, `name:string`, `task:JobTask`, `schedule:JobSchedule`, `misfire:MisfirePolicy`, `enabled:boolean`, `jobRevision:revision`, `catalogRevision:revision`, `createdAtMs:integer`, `updatedAtMs:integer` | ID/revision 由 Server 分配。 |
| `JobValidation` | `valid:boolean`, `normalizedJob:JobDraft|null`, `fieldErrors:FieldError[]`, `warnings:string[]`, `capabilityRequirements:string[]` | 不写库、不发送命令。 |
| `RunSummary` | `runId:string`, `jobId:string`, `agentId:string`, `startedAtMs:integer|null`, `finishedAtMs:integer|null`, `outcome:string`, `reportId:string|null`, `errorCode:string|null` | outcome=`succeeded`、`failed`、`cancelled`、`unknown`；无事件不造 running。 |
| `ReportSummary` | `reportId:string`, `agentId:string`, `jobId:string`, `jobRevision:revision|null`, `taskKind:string`, `startedAtMs:integer|null`, `receivedAtMs:integer`, `sizeBytes:integer`, `quality:string` | 不含原始 payload。 |
| `ReportDetail` | `summary:ReportSummary`, `payload:JsonObject|null`, `payloadVisibility:string` | payloadVisibility=`full`、`redacted`、`denied`。 |
| `EventView` | `eventId:string`, `agentId:string`, `jobId:string|null`, `atMs:integer`, `kind:string`, `severity:string`, `summary:string`, `operationId:string|null` | 仅真实事件。 |

### 4.3 指标、Probe、Plus

| VO | 必需字段及类型 | 枚举/约束 |
| --- | --- | --- |
| `MetricQuality` | `state:string`, `reason:string|null` | state=`valid`、`warmingUp`、`unavailable`、`stale`、`unknown`。 |
| `MetricSample<T>` | `value:T|null`, `sampledAtMs:integer|null`, `receivedAtMs:integer|null`, `quality:MetricQuality`, `sourceJobId:string|null`, `sourceJobRevision:revision|null` | 各指标组时间独立，来自已入库报告。 |
| `CpuMetrics` | `cpuUsagePercent:number`, `logicalCores:integer|null` | CPU 百分比 0..100。 |
| `MemoryMetrics` | `usedBytes:integer|null`, `totalBytes:integer|null`, `usedPercent:number|null` | 安全整数字节；比例 0..100。 |
| `NetworkMetrics` | `receivedBps:integer|null`, `sentBps:integer|null`, `receivedTotalBytes:UInt64String|null`, `sentTotalBytes:UInt64String|null`, `counterReset:boolean` | 计数器重置不产生负速率。 |
| `MetricsLatest` | `agentId:string`, `cpu:MetricSample<CpuMetrics>`, `memory:MetricSample<MemoryMetrics>`, `network:MetricSample<NetworkMetrics>`, `bindingRevision:revision` | 仅包含授权的指标组。 |
| `MetricPoint` | `atMs:integer`, `value:number|null`, `quality:MetricQuality`, `sourceJobId:string|null`, `sourceJobRevision:revision|null` | 空桶 value=null。 |
| `MetricSeries` | `agentId:string`, `metric:string`, `unit:string`, `aggregation:string`, `points:MetricPoint[]` | 私有查询；points 内含 sourceJobId/sourceJobRevision。 |
| `PluginSummary` | `pluginId:string`, `name:string`, `version:string`, `installed:boolean`, `schemaHash:string`, `capabilities:string[]` | 真实已安装清单。 |
| `PluginSchemaView` | `pluginId:string`, `version:string`, `schemaHash:string`, `taskKind:string`, `schema:JsonObject`, `sensitivePaths:string[]`, `supportedUi:boolean` | 可信描述符，不执行其中代码。 |
| `RuntimeConfigView` | `pluginId:string`, `agentId:string`, `revision:revision`, `config:JsonObject|null`, `redacted:boolean`, `runtimeState:string`, `observedAtMs:integer|null` | runtimeState=`unknown`、`configured`、`acknowledged`、`active`、`paused`、`failed`；ACK 不等于 Worker 活跃。 |
| `ProbeResult` | `target:string`, `protocol:string`, `attempted:integer`, `succeeded:integer`, `lossPercent:number|null`, `latencyMs:number|null`, `quality:MetricQuality` | protocol=`icmp`、`tcp`、`http`、`udp`，不含 wss。 |

### 4.4 主题与公共站点

| VO | 必需字段及类型 | 枚举/约束 |
| --- | --- | --- |
| `ThemeSummary` | `themeId:string`, `name:string`, `description:string`, `ownerUserId:string`, `state:string`, `createdAtMs:integer`, `metadataRevision:revision`, `themeRevision:revision`, `publishedVersionId:string|null` | state=`draft`、`ready`、`archived`、`rejected`；published 表示已发布，不是跨站点激活状态。 |
| `ThemeVersion` | `themeVersionId:string`, `themeId:string`, `version:string`, `contentSha256:string`, `manifest:ThemeManifest`, `validationState:string`, `validationOperationId:string|null`, `createdAtMs:integer`, `publishedAtMs:integer|null` | 不可变；状态=`validating`、`ready`、`rejected`。 |
| `ThemeManifest` | `formatVersion:integer`, `name:string`, `version:string`, `entry:string`, `assets:string[]`, `configSchema:JsonObject`, `bridgeVersion:string`, `contentSecurity:string` | contentSecurity=`externalModules`、`bundledClassic`；限制见 §7。 |
| `ThemeCheck` | `code:string`, `passed:boolean`, `message:string`, `path:string|null` | 扫描/签名不等于沙箱。 |
| `ThemeValidation` | `themeVersionId:string`, `state:string`, `contentSha256:string`, `checkedAtMs:integer`, `checks:ThemeCheck[]` | state=`validating`、`ready`、`rejected`。 |
| `PreviewScope` | `agentIds:string[]`, `metrics:string[]`, `historyFromMs:integer|null`, `historyToMs:integer|null` | 指定只读脱敏范围。 |
| `ThemePreviewLease` | `previewId:string`, `themeVersionId:string`, `expiresAtMs:integer`, `resourceBaseUrl:string`, `bridgeVersion:string`, `scope:PreviewScope` | 短 TTL、actor/version/scope 绑定，无长期 token。 |
| `ThemeConfigView` | `themeVersionId:string`, `revision:revision`, `values:JsonObject` | 版本绑定、仅非秘密展示配置；不得存储/返回 secret 或 `secretPaths`。 |
| `SiteDataPolicy` | `agentIds:string[]`, `metrics:string[]`, `allowHistory:boolean`, `maxHistoryDays:integer`, `revision:revision` | 默认空 allowlist。 |
| `SiteView` | `siteId:string`, `slug:string`, `enabled:boolean`, `visibility:string`, `themeVersionId:string|null`, `bindingRevision:revision`, `dataPolicy:SiteDataPolicy`, `updatedAtMs:integer` | visibility=`private`、`public`；公开需显式策略。 |
| `PublicMetric` | `name:string`, `unit:string`, `value:number|null`, `sampledAtMs:integer|null`, `quality:MetricQuality` | 仅发布 allowlist。 |
| `PublicMetricPoint` | `atMs:integer`, `value:number|null`, `quality:MetricQuality` | 公开安全白名单，不含 source/job/agent 标识或 revision。 |
| `PublicMetricSeries` | `agentKey:string`, `metric:string`, `unit:string`, `aggregation:string`, `points:PublicMetricPoint[]` | 仅公开历史 DTO。 |
| `PublicSiteSnapshot` | `slug:string`, `publishedAtMs:integer`, `bindingRevision:revision`, `themeVersionId:string`, `agents:PublicAgent[]` | Theme Bridge/预览仅用 public-equivalent DTO。 |
### 4.5 后续运营值对象

| VO | 必需字段及类型 | 枚举/约束 |
| --- | --- | --- |
| `AlertCondition` | `metric:string`, `operator:string`, `threshold:number`, `agentIds:string[]`, `forMs:integer` | operator=`gt`、`gte`、`lt`、`lte`、`eq`。 |
| `AlertRuleView` | `ruleId:string`, `name:string`, `enabled:boolean`, `severity:string`, `condition:AlertCondition`, `windowMs:integer`, `cooldownMs:integer`, `mutedUntilMs:integer|null`, `revision:revision` | severity=`info`、`warning`、`critical`。 |
| `AlertEventView` | `alertEventId:string`, `ruleId:string`, `agentId:string|null`, `state:string`, `severity:string`, `openedAtMs:integer`, `acknowledgedAtMs:integer|null`, `resolvedAtMs:integer|null`, `summary:string`, `revision:revision` | state=`open`、`acknowledged`、`resolved`、`suppressed`。 |
| `NotificationChannelView` | `channelId:string`, `kind:string`, `name:string`, `enabled:boolean`, `secretConfigured:boolean`, `updatedAtMs:integer`, `revision:revision` | 不回显 secret。 |
| `DeliveryLogView` | `deliveryId:string`, `channelId:string`, `eventType:string`, `state:string`, `attempts:integer`, `createdAtMs:integer`, `finishedAtMs:integer|null`, `errorCode:string|null` | state=`queued`、`sent`、`failed`、`suppressed`。 |
| `StorageStats` | `databaseBytes:integer|null`, `reportBytes:integer|null`, `freeBytes:integer|null`, `measuredAtMs:integer`, `quality:MetricQuality`, `revision:revision` | 字节数量遵守安全整数界限。 |
| `BackupPlanView` | `planId:string`, `name:string`, `enabled:boolean`, `schedule:JobSchedule`, `targetKind:string`, `retentionCount:integer`, `revision:revision` | targetKind=`local`、`configuredRemote`；凭据 write-only。 |
| `BackupRunView` | `backupId:string`, `planId:string|null`, `state:string`, `startedAtMs:integer`, `finishedAtMs:integer|null`, `sizeBytes:integer|null`, `checksum:string|null`, `verification:string` | state=`queued`、`running`、`succeeded`、`failed`、`cancelled`；verification=`unknown`、`verified`、`failed`。 |
| `ServerOperationView` | `operationId:string`, `kind:string`, `state:string`, `createdAtMs:integer`, `updatedAtMs:integer`, `finishedAtMs:integer|null`, `expectedRevision:revision|null`, `appliedRevision:revision|null`, `completedCount:UInt64String|null`, `totalCount:UInt64String|null`, `errorCode:string|null` | Server 本地任务仅 `queued|running|succeeded|failed|cancelled`；终态有 finishedAtMs，计数运行中可为 null；revision 由对应 storage/maintenance/preview CAS 产生，不是 Agent ACK。 |
| `ServerOperationResult` | `operation:ServerOperationView` | 备份、清理等本地工作使用，不伪装成 Agent apply operation。 |
| `DataDeletePreview` | `previewId:string`, `scope:string`, `beforeMs:integer`, `estimatedRows:UInt64String`, `estimatedBytes:UInt64String`, `expiresAtMs:integer`, `currentRevision:revision`, `requiresStepUp:boolean` | 只预估，确认需 CAS 与 step-up。 |
| `FinanceUnit` | `key:string`, `label:string`, `priceMicros:UInt64String`, `billingPeriod:string`, `enabled:boolean` | key 为受控枚举。 |
| `FinanceConfigView` | `revision:revision`, `currency:string`, `units:FinanceUnit[]`, `updatedAtMs:integer` | typed allowlist。 |
| `DeploymentStatus` | `mode:string`, `embeddedAssets:boolean`, `buildVersion:string`, `observedAtMs:integer` | 只读；mode=`development`、`packaged`。 |
| `StepUpChallenge` | `challengeId:string`, `purpose:string`, `expiresAtMs:integer`, `requiredAction:string` | Server 绑定 actor、目标、输入摘要。 |
| `StepUpProof` | `proof:string`, `challengeId:string` | 短时单次消费，服务端验证。 |
| `ApiTokenSummary` | `tokenId:string`, `name:string`, `scopes:string[]`, `createdAtMs:integer`, `expiresAtMs:integer|null`, `lastUsedAtMs:integer|null`, `revision:revision` | 不含 secret。 |
| `OneTimeSecret` | `secret:string`, `recoveryExpiresAtMs:integer` | 创建专用响应，后续 GET 不返回。 |

内联结果 VO 在对应方法行逐字段定义。

## 5. 已规划 RPC 方法目录

下表 method 均拟经 `POST /api/v1/rpc` JSON-RPC 2.0 调用，当前均未实现。`req`/`opt` 列分别表示必需/可选参数；普通写入还需 `clientMutationId` 和相应 CAS revision。通用 `page/pageSize/limit` 默认值见 §2.2。错误按 §3；方法需在实施版本目录显式声明实际状态。

### 5.1 会话、用户和注册接入

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `session.info` | R1 | 已登录 | 无 | 无 | `FeatureCatalog` | 返回实际启用的 feature/method；未实现项必须标 planned/unsupported。只读。 |
| `user.list` | R1 | admin | 无 | `page,pageSize,sortBy` | `Page<UserSummary>` | 不返回秘密；只读。 |
| `user.create` | R1 | admin | `clientMutationId,username,password,role` | 无 | `UserSummary` | 重名/密码策略失败；密码只处理、不回显；保护创建初始 admin 的本地 bootstrap。 |
| `user.update` | R1 | admin | `clientMutationId,userId,expectedUserRevision` | `role,enabled` | `UserSummary` | revision 冲突；不得禁用/降权最后一位有效 admin。 |
| `user.disable` | R1 | admin | `clientMutationId,userId,expectedUserRevision` | 无 | `UserSummary` | 与 update 同事务吊销会话；不能停用最后 admin。 |
| `enrollment.create` | R2 | admin | `clientMutationId,label,expiresAtMs` | `allowedCapabilities:string[]` | `EnrollmentCreated` | §5.1 下方定义；Noise 注册凭据一次呈现；有短 TTL 加密回执，过期不自动重签。 |
| `enrollment.list` | R2 | admin | 无 | `page,pageSize,state` | `Page<EnrollmentSummary>` | 不显示 Token/派生值。 |
| `enrollment.get` | R2 | admin | `enrollmentId` | 无 | `EnrollmentSummary` | 不返回秘密；不存在或不可见为 NOT_FOUND。 |
| `enrollment.revoke` | R2 | admin | `clientMutationId,enrollmentId,expectedRevision` | 无 | `EnrollmentSummary` | 已消费凭据吊销不伪装成 Agent 下线；记录审计。 |

`EnrollmentCreated` 必需字段：`enrollment:EnrollmentSummary`、`registrationCredential:string`、`recoveryExpiresAtMs:integer`。`registrationCredential` 只在创建/有效同 key 回执中返回一次；存储侧只保留不可逆验证材料，短 TTL 恢复回执单独加密且不得进常规日志。真实 Agent 以既有 Noise 注册流程提交后才建立/上线 Agent；登记 enrollment 不是 Agent 上线。恢复过期后先查询 enrollment，再显式吊销旧凭据并新建，不做隐式二次签发。

`user.create` 密码策略须由 Server 固定并在验证错误中说明规则；初始管理员经本地受保护 bootstrap，不创建公开默认密码接口。R1 登录限流、CSRF 与会话建立先于上述管理 RPC。

### 5.2 Agent、策略、任务与操作

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `agent.list` | R2 | viewer+ | 无 | `page,pageSize,sortBy,status,labels` | `Page<AgentSummary>` | 权限过滤后分页；不等于主机全量暴露。 |
| `agent.get` | R2 | viewer+ | `agentId` | 无 | `AgentDetail` | 私网地址按权限裁剪；revoked Agent 仍可审计查询。 |
| `agent.updateMetadata` | R2 | operator+ | `clientMutationId,agentId,expectedMetadataRevision` | `displayName,labels,region,note` | `AgentSummary` | 单字段 patch；labels 数量/长度限制；CAS；与任务配置分离。 |
| `agent.revoke` | R2 | admin | `clientMutationId,agentId,expectedMetadataRevision` | `reason` | `AgentSummary` | 撤销控制面访问并审计；不声称删除历史数据/物理擦除 Agent。 |
| `agent.capabilities.get` | R2 | viewer+ | `agentId` | 无 | `CapabilityView` | 依据真实最近观察时间；过期/未知不等于 unsupported。 |
| `agent.policy.get` | R2 | operator+ | `agentId` | 无 | `AgentPolicyView` | 仅当前会话观测；Server 未持久化策略时不得声称持久；未观测为 unknown。 |
| `job.list` | R2 | operator+ | `agentId` | `page,pageSize,enabled,taskKind` | `Page<JobView>` | 返回真实期望目录；不把过滤后的空目录解释成无任务。 |
| `job.get` | R2 | operator+ | `agentId,jobId` | 无 | `JobView` | 返回期望配置，不表示 Agent 已应用。 |
| `job.validate` | R2 | operator+ | `agentId,job:JobDraft` | 无 | `JobValidation` | 不写库、不发命令；校验能力、版本化 Task kind、Probe 安全边界和调度。 |
| `job.create` | R2 | operator+ | `clientMutationId,agentId,expectedCatalogRevision,job:JobDraft` | 无 | `JobMutationResult` | 原子创建 Server 分配 ID/revision、操作、幂等记录、审计；仅事务提交后通知 Agent。 |
| `job.update` | R2 | operator+ | `clientMutationId,agentId,jobId,expectedJobRevision,expectedCatalogRevision,patch:JobPatch` | 无 | `JobMutationResult` | JobPatch 仅允许定义字段；Server 事务重建完整目录；CAS 失败无部分更新。 |
| `job.delete` | R2 | operator+ | `clientMutationId,agentId,jobId,expectedJobRevision,expectedCatalogRevision` | 无 | `JobDeleteResult`=`{jobId:string,operation:OperationView,catalogRevision:revision}` | 不返回已删除 JobView；异步同步，历史报告/审计保留策略独立。 |
| `operation.get` | R2 | operator+ | `operationId` | 无 | `OperationDetail` | 资源权限过滤；超时状态为 unknown 而不是失败回滚。 |
| `operation.list` | R2 | operator+ | 无 | `agentId,jobId,state,cursor,limit` | `CursorPage<OperationView>` | cursor 绑定筛选；稳定顺序按时间和 ID。 |
| `operation.catalogSync.get` | R2 | operator+ | `agentId` | 无 | `CatalogSyncView` | §5.2 下方定义；精确呈现实际 command/过滤 catalog/ACK 证据。 |
| `run.list` | R2 | viewer+ | `agentId` | `jobId,fromMs,toMs,cursor,limit` | `CursorPage<RunSummary>` | 只依据真实 TaskRun/事件；没有 started 事件不得合成 running。 |
| `report.list` | R2 | operator+ | `agentId` | `jobId,taskKind,fromMs,toMs,cursor,limit` | `CursorPage<ReportSummary>` | 不返回原始 payload；细节需单独权限。 |
| `report.get` | R2 | operator+ | `reportId` | `includePayload:boolean` | `ReportDetail` | `includePayload` 默认 false；无详细权限时拒绝或 redacted。 |
| `event.list` | R2 | operator+ | `agentId` | `jobId,fromMs,toMs,cursor,limit,severity` | `CursorPage<EventView>` | 只读真实事件；筛选/排序纳入 cursor。 |

`JobPatch` 由允许修改字段的可选子集构成：`name?:string`、`task?:JobTask`、`schedule?:JobSchedule`、`misfire?:MisfirePolicy`、`enabled?:boolean`；拒绝空 patch、未知字段及只读字段。`JobMutationResult` 必需字段为 `job:JobView`、`operation:OperationView`、`catalogRevision:revision`。服务端同时校验 `expectedJobRevision` 与 `expectedCatalogRevision`；浏览器不能发“覆盖全目录”作为单项编辑。新任务 ID 与版本由 Server 分配。

`CatalogSyncView` 字段：`agentId`、`desiredCatalogRevision:revision`、`lastSentCommandId:string|null`、`lastSentAtMs:integer|null`、`lastAckAtMs:integer|null`、`lastAckState:string`、`filteredJobIds:string[]`、`evidence:string[]`。`lastAckState` 允许 `unknown|pending|accepted|rejected|policyBlocked|capabilityUnsupported`。仅当回执确切绑定 commandId、目录摘要、Agent 会话和目标 revision 时可确认 applied；被过滤的空 catalog ACK 不证明被过滤 Job 已应用。

### 5.3 内置采集模板、指标和 Probe

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入约束 |
| --- | --- | --- | --- | --- | --- | --- |
| `metrics.latest` | R3 | viewer+ | `agentIds:string[]` | `metrics:string[]` | `MetricsLatest[]` | agentIds 必需 1..100；metrics 可选，枚举见下文。只读真实入库报告投影。 |
| `metrics.history` | R3 | viewer+ | `agentId,metric` | `fromMs,toMs,aggregation,maxPoints` | `MetricSeries` | from/to 成对可选；省略默认最近 1h；跨度最多 7d、最多 1000 点，默认 500。 |
| `metrics.binding.update` | R3 | operator+ | `clientMutationId,agentId,expectedBindingRevision,bindings:MetricBindingDraft[]` | 无 | `MetricBindingResult` | 原子校验每个 Job 存在、同 Agent 且产出指标；不改任务定义。 |
| `metrics.applyBuiltinProfile` | R3 | operator+ | `clientMutationId,agentId,profileId,expectedCatalogRevision` | 无 | `ProfileApplyResult` | 唯一允许的便捷创建入口；本方法列入 catalog；普通无 Secret collect Job；CAS、审计和同一幂等规则。 |
| `plugin.list` | R4 | operator+ | 无 | `agentId` | `PluginSummary[]` | 仅真实 inventory；不下载/安装 Server 插件，不将 Echo 验证推广到全部插件。 |
| `plugin.schema.get` | R4 | operator+ | `pluginId,version` | 无 | `PluginSchemaView` | plugin/schemaHash/taskKind 绑定；拒绝未知 schema，不执行插件描述符脚本。 |
| `plugin.runtime.get` | R4 | admin | `agentId,pluginId` | 无 | `RuntimeConfigView` | 配置字段依 schema 脱敏；未知秘密元数据时整个 config 隐藏。 |
| `plugin.runtime.validate` | R4 | admin | `agentId,pluginId,version,config:JsonObject` | 无 | `PluginConfigValidation` | 无写入；校验安装版本、schemaHash、秘密更新动作和 Agent 能力。 |
| `plugin.runtime.update` | R4 | admin | `clientMutationId,agentId,pluginId,expectedRuntimeRevision,schemaHash,patch:PluginConfigPatch` | 无 | `RuntimeMutationResult` | 只对已安装 Plus；秘密独立加密存储；配置/operation/idempotency/audit 同事务。 |
| `plugin.runtime.clear` | R4 | admin | `clientMutationId,agentId,pluginId,expectedRuntimeRevision` | `clearSecretPaths:string[]` | `RuntimeMutationResult` | 只能清除明示支持字段；明确保持/替换/清除，不能把 redacted 字符串存入。 |

`MetricBindingDraft` 为 `{metric:string,jobId:string}`；`MetricBindingResult` 为 `{bindings:MetricBinding[],revision:revision}`。`ProfileApplyResult` 为 `{jobs:JobView[],operations:OperationView[],catalogRevision:revision}`。没有采集 Job 时，服务端不得暗中创建，必须由用户显式调用 profile 或普通 Job 创建。

`PluginConfigValidation` 必需字段：`valid:boolean`、`fieldErrors:FieldError[]`、`schemaHash:string`、`warnings:string[]`。`PluginConfigPatch` 为 `{values:JsonObject,secretChanges:SecretChange[]}`；`SecretChange` 是辨别联合：`{path,action:"keep"}`、`{path,action:"replace",value:string}`、`{path,action:"clear"}`，故仅 replace 带必需 value，不适用 value 字段不得出现。`RuntimeMutationResult` 为 `{runtime:RuntimeConfigView,operation:OperationView}`。Echo Plus 联动只能证明 Echo 的相应路径。 |

指标只从真实入库 TaskReport 解码。每组 sample 都带 `sampledAtMs`（现有 Proto 仅有 `started_at` 时就以其表示报告起始时刻，不伪称完成/精确采样时刻）、`receivedAtMs`、quality、sourceJobRevision。迟到报告可以进入历史，但旧 revision/较旧采样不得覆盖新投影；查询服务按显式 Job binding 选择来源。CPU、内存和网络采样时间可不同。网络计数器重置不得产生负速率。

历史聚合采用有界查询/可重建投影；如真实负载要求新增指标投影存储，应确保从权威报告重建。进程/Socket 详细数据独立按需查询、限条数和授权，不随 summary 或 WS 全局广播。R3 先提供真实聚合/查询服务，不以 VO 适配代替缺失查询逻辑。

### 5.4 调度、写操作与操作状态

`JobSchedule` 的全部成员必需，按 type 除适用成员外其余成员都必须是 `null`：interval=`everyMs` 非 null、其余 null；once=`atMs` 非 null、其余 null；cron=`cron` 与 `timeZone` 非 null、`everyMs/atMs` null。禁止把 null 省略或给非当前类型成员填值。cron 固定六段含秒；校验返回下次运行提示时注明时区。misfire 策略逐项受 Agent/Scheduler 能力限制。

建议的状态迁移：`pending -> waitingAgent -> sent -> applied`；Agent 明确拒绝可转 `rejected`；本地策略阻止转 `blocked`；新期望配置覆盖旧操作转 `superseded`；超时/断链且无法证明执行结果转 `unknown`。非每条边都可逆，`applied/rejected/blocked/superseded` 为本操作终态；恢复连接后对账可创建新的观察/操作，不篡改原始事实。`unknown` 不等于失败、未发送或未执行。

desired config（数据库中的 Job/runtime 期望状态）、apply operation（命令发送/Agent ACK）和 execution observation（真实 TaskReport/RunEvent）是三层独立状态。配置 ACK 不能表示任务已运行；runtime ACK 不能表示 Worker 仍活跃。当前没有 Task 开始事件不得生成 `running`。Web 不能通过“过滤后空目录成功”把被拒绝 Job 标成 applied。

业务写幂等键为 `(actor, method, clientMutationId)`，唯一约束与规范化输入 hash 一起持久化。相同键且相同输入返回同一业务结果/operation；同键不同输入返回 `IDEMPOTENCY_CONFLICT`。RPC `id` 仅关联网络请求，不承担业务去重。写请求缺失 `clientMutationId` 必须拒绝。

资源 CAS (`expected*Revision`) 防止覆盖新状态；幂等 key 防止同一意图重复提交，两者不可互相替代。配置、operation、幂等记录、成功审计在同一 Server 事务中提交，之后再通知 Agent。重启后扫描待同步操作并与现有 Agent 会话对账；不能仅靠进程内通知队列导致永久漏发。回执建议至少保留 24h，可配置；未完成 operation 及其关联不按普通 TTL 删除。

未知结果时，客户端首先用 `operation.get` 或资源读取确认状态；安全重试必须复用同一 `clientMutationId` 与相同正文。首版不提供通用 RunNow。未来 RunNow/Shell 需要定义 Agent 重启、ACK 丢失、副作用重复和持久去重边界，不能宣称 exactly-once，也不得扩大 Agent 端持久化负担而不单独验收。

示例 `job.create`：

```json
{
  "jsonrpc": "2.0",
  "id": "request-uuid",
  "method": "job.create",
  "params": {
    "clientMutationId": "mutation-uuid",
    "clientMutationId": "00000000-0000-4000-8000-000000000004",
    "clientMutationId": "00000000-0000-4000-8000-000000000004",
      "name": "CPU 采集",
      "task": { "kind": "smalux.collect.cpu.v1", "config": {} },
      "schedule": { "type": "interval", "everyMs": 5000, "atMs": null, "cron": null, "timeZone": null },
      "misfire": { "behavior": "skip" },
      "enabled": true
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "id": "request-uuid",
  "result": {
    "job": {
      "jobId": "00000000-0000-4000-8000-000000000002",
      "agentId": "00000000-0000-4000-8000-000000000001",
      "name": "CPU 采集",
      "task": { "kind": "smalux.collect.cpu.v1", "config": {} },
      "schedule": { "type": "interval", "everyMs": 5000, "atMs": null, "cron": null, "timeZone": null },
      "misfire": { "behavior": "skip" },
      "enabled": true,
      "jobRevision": "1",
      "catalogRevision": "9",
      "createdAtMs": 1770000000000,
      "updatedAtMs": 1770000000000
    },
    "operation": {
      "operationId": "00000000-0000-4000-8000-000000000003",
      "kind": "job.catalog.apply",
      "targetId": "00000000-0000-4000-8000-000000000001",
      "state": "waitingAgent",
      "createdAtMs": 1770000000000,
      "updatedAtMs": 1770000000000,
      "expectedRevision": "9",
      "appliedRevision": null,
      "commandId": null,
      "reason": null,
      "retryable": true
    },
    "catalogRevision": "9"
  }
}
```

响应示例表达拟定 VO 形状而非真实运行结果；固定时间和 UUID 仅为 JSON 样例。

### 5.5 WebSocket 实时控制

WS 端点为 `GET /api/v1/ws` Upgrade。仅接受 JSON-RPC 2.0 的 `stream.subscribe`、`stream.unsubscribe`、`stream.ping` 控制请求。业务写、普通 CRUD、Job 操作和公开站点管理一律拒绝。握手校验有效会话与 allowlisted Origin；每个订阅在创建、重连和每次数据推送时应用权限与资源授权。

| 方法/通知 | 阶段 | 权限 | 输入 | 输出/失败 |
| --- | --- | --- | --- | --- |
| `stream.subscribe` | R3 | viewer+ | `topic,agentIds,metrics?,sinceCursor?` | `SubscriptionAck`；超订阅数、无权资源、空 agentIds 均拒绝。 |
| `stream.subscribe` | R3 | viewer+；订阅 operation 仅 operator+ | `topic,agentIds?,metrics?,sinceCursor?` | `SubscriptionAck`；超订阅数、无权资源或空 topic 范围均拒绝。 |
| `stream.unsubscribe` | R3 | 原订阅主体 | `subscriptionId,streamEpoch` | `UnsubscribeAck`；epoch 不匹配不取消新流。 |
| `stream.notification` | R3 | 订阅授权范围 | 服务端推送 `StreamEnvelope` | 数据通知、`resyncRequired` 或 operation 状态；断线后 HTTP 查询为权威恢复方式。 |

`SubscriptionAck` 字段：`subscriptionId:string`、`streamEpoch:string`、`sequence:UInt64String`、`snapshot:MetricsLatest[]|null`。`UnsubscribeAck` 为 `{subscriptionId,streamEpoch,unsubscribed:boolean}`；`Pong` 为 `{serverTimeMs,streamEpoch}`。`StreamEnvelope` 为 `{subscriptionId,streamEpoch,sequence:UInt64String,kind:string,data:JsonObject}`；kind=`metrics.update|operation.update|resyncRequired`。metrics payload 为 §4 VO，operation payload 为 `OperationView`。

`SubscriptionAck` 字段：`subscriptionId:string`、`streamEpoch:string`、`sequence:UInt64String`、`snapshot:MetricsLatest[]|OperationSnapshot[]|null`；topic=`metrics|operation`，metrics 的 agentIds 必需且快照为 MetricsLatest[]，operation 的 agentIds 可省略但资源集由 operator+ 授权且快照为 OperationSnapshot[]（`{items:OperationView[],cursor:string|null}`）。viewer 订阅 operation 返回 FORBIDDEN。`UnsubscribeAck` 为 `{subscriptionId,streamEpoch,unsubscribed:boolean}`；`Pong` 为 `{serverTimeMs,streamEpoch}`。`StreamEnvelope` 为 `{subscriptionId,streamEpoch,sequence:UInt64String,kind:"metrics.update"|"operation.update"|"resyncRequired",data:JsonObject}`；resyncRequired data 固定 `{reason:"sequenceGap"|"queueOverflow"|"epochChanged",snapshotRequired:true,latestSequence:UInt64String|null}`。snapshot 与通知均按 topic/权限裁剪；普通写仍只走 HTTP RPC。

建议初始消息上限 `1 MiB`、每连接 16 个订阅、每订阅最多 200 个 Agent、最小推送间隔 1s/默认 2s；上线前通过容量测试确定配置。轮询模式使用 `metrics.latest` 同一 VO（建议 5s，页面隐藏时可降频），不得以 no-op subscribe 或旧 Mock 维持画面。WS 数据不含原始 Process/Socket、凭据、未授权 report。

## 6. REST 身份、元信息和文件传输

| 方法与路径 | 阶段 | 权限 | 必需/可选输入 | 响应 | 失败/写入条件 |
| --- | --- | --- | --- | --- | --- |
| `GET /api/v1/meta` | R0 | 公开 | 无 | `MetaView` | 只暴露必要版本/transport；不是完整 capability 清单。 |
| `POST /api/v1/auth/login` | R1 | 公开但限流 | body:`username,password`; Header:`Origin` | `SessionView`；会话 Cookie 由 Set-Cookie 返回 | TLS、Origin、CSRF/login 防护和限流；错误不泄露账号存在性。 |
| `GET /api/v1/auth/session` | R1 | 会话 | Cookie | `SessionView` | 401 无效/吊销/过期；csrfToken 与会话绑定。 |
| `POST /api/v1/auth/logout` | R1 | 会话+CSRF | Cookie、CSRF header | `LogoutResult`=`{revoked:boolean}` | 吊销服务端 session 摘要并终止 WS；重复 logout 可安全成功。 |
| `POST /api/v1/auth/password/change` | R1 | 会话+CSRF | `currentPassword,newPassword` | `PasswordChangeResult`=`{changedAtMs,revokedSessionCount:integer}` | 密码策略校验；修改与安全吊销相关会话原子化。 |
| `GET /api/v1/auth/sessions` | R1 | admin | Cookie | `Page<SessionSummary>` | 不返回 token、完整 IP 或秘密。 |
| `DELETE /api/v1/auth/sessions/{sessionId}` | R1 | admin+CSRF | path `sessionId` | `SessionRevokeResult`=`{sessionId,revoked:boolean}` | 吊销服务端摘要并关闭其 WS；保护当前授权流程。 |
| `POST /api/v1/themes/uploads` | R4 | admin+CSRF | multipart `file`, `themeId`, `expectedThemeRevision`, `clientMutationId` form part, `Idempotency-Key` header | `ThemeUploadAccepted`=`{uploadId,themeId,themeVersionId,operation:OperationView,expiresAtMs}` | 非 JSON 包体；`Idempotency-Key` 必须等于 form `clientMutationId`；CAS 主题版本、配额/哈希/解包校验；细则见 §7。 |
| `GET /api/v1/themes/uploads/{uploadId}` | R4 | owner/admin | path `uploadId` | `ThemeUploadStatus`=`{uploadId,state,themeVersionId,operation:OperationView|null,expiresAtMs,contentSha256:string|null}` | 只能读授权 upload；失败清理状态可查询到过期。 |
| `GET /api/v1/themes/{themeId}/versions/{versionId}/assets/{path}` | R4 | 依发布状态/preview lease | path IDs、规范相对 `path` | 静态 bytes + `Content-Type`, `ETag`, `X-Content-Type-Options:nosniff` | 只服务 ready 且已发布可见版本，或有效私有 lease；路径规范化并按主题隔离 origin。 |
| `GET /api/v1/themes/{themeId}/versions/{versionId}/download` | R4 | admin | IDs | 原始 ZIP bytes + digest header | 仅授权下载已验证包；审计；不得将资源 path 作为任意文件路径。 |
| `ThemeDetail` 包含 `theme:ThemeSummary` 与 `versions:ThemeVersion[]`；`ThemePublishResult` 等值对象在表中给出精确字段。`SiteDataPolicyDraft` 为 `{agentIds:string[],metrics:string[],allowHistory:boolean,maxHistoryDays:integer}`；保存时 Server 重新检查权限和 allowlist。站点默认关闭且无 Agent/metric 授权，不存在主题自助公开开关。
| `POST /api/v1/auth/step-up/begin` | R5 | 已登录+CSRF | `clientMutationId,method,target,params` | `StepUpChallenge`=`{challengeId:string,nonce:string,expiresAtMs:integer,requiredAction:string}` | TLS/可信 Origin/CSRF；服务端按当前 actor/session 和自身风险策略确认是否需要 step-up，不接收客户端 risk/inputDigest 判断；R5 仅 password re-auth，challenge nonce 绑定 actor、target 和规范化请求摘要。 |
| `POST /api/v1/auth/step-up/complete` | R5 | 同一会话+CSRF | `clientMutationId,challengeId,password` | `StepUpProof` | 比对 challenge nonce、当前 actor 和 password；一次性消费并发短时 proof；错误/重复挑战不得复用。MFA 只在 R6 扩展，不代表登录已支持 MFA。 |
除认证流程自身语义外，REST 状态写入需 CSRF token 和可信 Origin。Cookie 为随机不透明值，`HttpOnly; Secure; SameSite`，服务端只保存摘要并设置 idle/absolute TTL 与吊销状态；WS 握手和后续订阅都重新授权。登录请求同样校验 Origin/CSRF 防跨站登录。localStorage 不保存管理凭据，也不能造 admin 会话。

认证端点按会话语义处理，不套普通 RPC 的 `clientMutationId`。上传另用 `Idempotency-Key`：同一用户、相同 key、相同文件正文 SHA-256 和元数据返回同一 upload/operation；同 key 内容不同返回 `IDEMPOTENCY_CONFLICT`。流中断后以状态端点查进度；不可确认正文哈希时不得按同 key 接受另一正文。上传回执可加密保留至少 24h；未完成校验操作不得过期删除。

首个管理员通过本地安全 bootstrap 建立，不存在公开创建初始管理员 endpoint。R1 后续可增加 TOTP/Passkey，但需明确 challenge/验证/恢复协议；不在本版伪造接口。外部 API token 后续独立 scope 与专用创建/吊销流程，secret 仅 `OneTimeSecret` 一次显示。创建响应丢失仅在短 TTL 加密同 key 回执中恢复，过期后不能自动重签；客户端声明的风险级别/权限 scope 不得授权操作。

### 6.1 Step-up 证明协议（后续敏感操作共用）

R5 catalog 为 `auth.stepUp.begin` / `auth.stepUp.complete`（与 REST 身份 Cookie/CSRF/TLS/Origin 一致）：begin req=`clientMutationId,method,target,params`，返回绑定 actor/session/nonce/目标方法及参数规范化摘要的短期 `StepUpChallenge`；服务端根据目标操作自身策略计算 risk/requiredAction，绝不信任客户端自报 risk/inputDigest。R5 complete req=`clientMutationId,challengeId,password`，要求同一有效会话、CSRF、可信 Origin、TLS，经密码重新验证后签发单次 `StepUpProof`。proof 绑定 actor、规范化目标 method+params 摘要、期限及 purpose，敏感 RPC 写入须提交 proof，服务端原子校验并消费。二者正式纳入 feature catalog，R5 之前为 unsupported；R6 可扩展 MFA proof，不暗示 R1 登录已有 MFA。挑战 begin/complete 是认证挑战的窄例外：各自仍使用 clientMutationId，完成 challenge 单次消费，安全重试仅可取得同一回执；普通管理写无豁免。

## 7. 主题上传、版本发布和安全预览

### 7.1 包定义和生命周期

主题是**版本化且不可变**的完整 HTML/CSS/JS ZIP 包。Server 仅暂存、解包、验证并静态托管；不执行上传 JS、不运行 npm install/build、不将其当 Rust/Plus 插件。`themeId` 是主题逻辑身份，`themeVersionId` 绑定具体内容 SHA-256。一个版本发布后不允许覆盖；修改必须上传产生新 content hash/version。

最小目录树：

```text
my-theme.zip
├─ theme.json
├─ index.html
├─ assets/
│  ├─ app.js
│  └─ app.css
└─ images/
   └─ logo.svg
```

`theme.json` 示例：

```json
{
  "formatVersion": 1,
  "name": "Operations Wallboard",
  "version": "1.2.0",
  "entry": "index.html",
  "assets": ["assets/app.js", "assets/app.css", "images/logo.svg"],
  "configSchema": {
    "type": "object",
    "properties": { "title": { "type": "string", "maxLength": 80 } },
    "additionalProperties": false
  },
  "bridgeVersion": "1",
  "contentSecurity": "externalModules"
}
```

manifest 必须有精确字段、合法 semver、受支持 `formatVersion`/`bridgeVersion`，`entry` 与全部 `assets` 均指向包内存在的文件；路径不能重复、大小写冲突或逃出根。`configSchema` 仅支持有界 JSON Schema 子集；拒未知关键字，禁止 theme 脚本提供可执行表单/管理行为。manifest 与包内容一起参与 SHA-256。

状态是三个独立维度：主题元数据/生命周期（draft/ready/archived/rejected）、具体版本验证/发布（validating/ready/rejected，未发布/已发布）、站点 binding 和 visibility（private/public）。上传接受不等于验证完成；validation ready 不等于 publish；publish 不等于 activate；activate 不等于 public visibility。

推荐可配置安全限额：压缩 ZIP 最大 20 MiB、解压总量最大 100 MiB、最多 1000 entries、最大压缩比 50:1、路径深度 8。超过任一限制在无资源耗尽前拒绝，并记录稳定原因码。失败临时对象/半解包内容清理有上限和期限；只允许受控暂存区，不拼接用户路径写任意目录。

### 7.2 RPC catalog

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入条件 |
| --- | --- | --- | --- | --- | --- | --- |
| `theme.list` | R4 | admin | 无 | `page,pageSize,state` | `Page<ThemeSummary>` | 只读；包含授权管理范围。 |
| `theme.create` | R4 | admin | `clientMutationId,name,description` | 无 | `ThemeSummary` | 创建逻辑主题身份，初始无版本；metadata/theme revision 建立。 |
| `theme.get` | R4 | admin | `themeId` | 无 | `ThemeDetail` | 仅管理读取，不公开私有 metadata。 |
| `theme.updateMetadata` | R4 | admin | `clientMutationId,themeId,expectedMetadataRevision` | `name,description` | `ThemeSummary` | CAS 仅修改可变 metadata，不修改不可变版本内容。 |
| `theme.version.get` | R4 | admin | `themeId,themeVersionId` | 无 | `ThemeVersion` | 只读详情及 manifest/hash。 |
| `theme.version.publish` | R4 | admin | `clientMutationId,themeId,themeVersionId,expectedThemeRevision` | 无 | `ThemePublishResult`=`{version:ThemeVersion,theme:ThemeSummary}` | ready/hash 校验通过版本；CAS 仅改发布元数据/themeRevision，不激活或公开。 |
| `theme.archive` | R4 | admin | `clientMutationId,themeId,expectedThemeRevision` | 无 | `ThemeSummary` | 仍被 site 引用时须先切换/解绑。 |
| `theme.delete` | R4 | admin | `clientMutationId,themeId,expectedThemeRevision` | 无 | `ThemeDeleteResult`=`{themeId:string,deleted:boolean}` | active/site/lease/reference 存在时拒绝；审计/异步清理。 |
| `theme.preview.create` | R4 | admin | `clientMutationId,themeVersionId` | `scope:PreviewScope` | `ThemePreviewLease` | 版本 ready 且隔离 origin 已配置；默认 scope 空，短 TTL。 |
| `theme.preview.revoke` | R4 | admin | `clientMutationId,previewId` | 无 | `ThemePreviewRevokeResult`=`{previewId:string,revoked:boolean}` | 撤销 lease、资源能力、bridge/缓存及订阅。 |
| `theme.config.get` | R4 | admin | `themeVersionId` | 无 | `ThemeConfigView` | 读取该版本 Schema 下的非秘密展示配置。 |
| `theme.config.update` | R4 | admin | `clientMutationId,themeVersionId,expectedConfigRevision,patch:JsonObject` | 无 | `ThemeConfigView` | 按对应版本不可变 configSchema 校验并 CAS；仅非秘密值。 |
| `site.create` | R4 | admin | `clientMutationId,slug` | 无 | `SiteView` | 创建 disabled/private/空策略且未绑定主题的站点；返回初始 siteId/bindingRevision。 |
| `site.list` | R4 | admin | 无 | `page,pageSize` | `Page<SiteView>` | 只读；为已有 siteId 提供发现入口。 |
| `site.get` | R4 | admin | `siteId` | 无 | `SiteView` | 未绑定时 `themeVersionId:null`。 |
| `site.updateDataPolicy` | R4 | admin | `clientMutationId,siteId,expectedBindingRevision,policy:SiteDataPolicyDraft` | 无 | `SiteView` | 校验 agent/metric allowlist；变更令缓存和 lease 失效。 |
| `site.activateTheme` | R4 | admin | `clientMutationId,siteId,expectedBindingRevision,themeVersionId` | 无 | `SiteView` | 版本 ready+published；验证对应版本配置；CAS 更新此 Site binding，不隐式公开。 |
| `site.rollbackTheme` | R4 | admin | `clientMutationId,siteId,expectedBindingRevision,themeVersionId` | 无 | `SiteView` | 验证目标版本自身 configSchema/配置且仍已发布；原子切换该 site binding。 |
| `site.update` | R4 | admin | `clientMutationId,siteId,expectedBindingRevision` | `enabled,visibility,slug` | `SiteView` | public 需已绑定版本、显式非空策略、安全 origin；CAS。 |
| `site.snapshot.get` | R4 | admin | `siteId` | 无 | `PublicSiteSnapshot` | 未绑定版本返回 STATE_CONFLICT；不得伪造快照；只返回 public DTO。 |

`ThemeDetail` 包含 `theme:ThemeSummary` 与 `versions:ThemeVersion[]`。上传新 ThemeVersion 时必须指定 `themeId` 与 `expectedThemeRevision`，并在创建版本成功后 CAS 更新主题 revision；已创建的版本内容、manifest/hash 永远不可变。`ThemeSummary.publishedVersionId` 只描述发布版本，不表示全局激活；激活权威只在各 `SiteView.themeVersionId`。`ThemeConfigView` 与配置记录均绑定 `themeVersionId`；没有跨版本共享/混用配置，主题不得存储或获知秘密。`SiteDataPolicyDraft` 为 `{agentIds:string[],metrics:string[],allowHistory:boolean,maxHistoryDays:integer}`，服务端重新验证授权。site 默认 disabled/private/空策略。
只接受明确允许的文本/静态媒体类型；拒绝可执行二进制、脚本宿主文件和 MIME/扩展不一致。HTML、JS、CSS 和所有静态响应设置 `X-Content-Type-Options: nosniff`；禁止目录列表、任意压缩包解压或上传文件直通管理站点。验证内容 hash、manifest、包树、引用资源和 MIME；校验过程关联持久 operation，失败理由可查，临时数据清理可重试。

上传采用 multipart 二进制，不把 HTML/CSS/JS 放进 JSON 字符串。每用户/每管理域的并发上传、总暂存空间、速率、校验时长和保留策略均限额。仅管理员签名验证/扫描不证明代码无恶意，绝不能替代隔离执行边界；本设计根本不在 Server 执行主题代码。

### 7.4 隔离 origin、iframe 与 Bridge

上传主题在**专用且不携带管理 Cookie 的受控资源 origin**提供；推荐与管理站点不同 site。使用受信任的预览/公开外壳将页面放入 `iframe sandbox="allow-scripts"`。不授予 `allow-same-origin`、forms、popups、top-navigation、downloads 等权限。无法部署独立安全 origin 时，禁止第三方主题预览/激活；不得退回管理站点同源注入或 `innerHTML` 渲染。

响应 CSP 使用 `sandbox allow-scripts`、`connect-src 'none'`、禁止 top-level navigation 等限制。禁用 `unsafe-eval`、Service Worker、外部资源和远程字体；主题只使用通过验证的包内静态资源。manifest 的 `externalModules` 使用模块脚本，模块仅允许包内路径且资源 CORS 不带 credentials；外壳按精确包资源规则 CSP 授权。若实现选择 `bundledClassic`，须在隔离响应中对 inline/classic 脚本制定精确 hash/nonce 与 CSP，不可宣称 opaque origin 下 `'self'` 已天然可用。默认不允许 inline script/style。另设 `Referrer-Policy: no-referrer`、`nosniff`，禁止 Service Worker 注册；静态资产不可带管理 Cookie。

Admin preview 由受信任外壳承载 sandbox iframe，`ThemePreviewLease` 是短 TTL、actor + themeVersion + scope 绑定的服务端授权。过期、退出、撤权、site policy 变更时撤销 lease、资源访问、缓存与关联流。不得在长期 URL/query 中放 session/token。若私有预览资源使用 ticket，ticket 应短时限、仅限具体 version/path/scope、避免写访问日志/Referer；匿名公共资产仅发布且 site public 时开放。

opaque iframe 的 `postMessage` origin 通常为 `"null"`，不得仅凭 origin 信任。受信任外壳通过精确 `event.source === iframe.contentWindow` 和随机 nonce 建立一次性 `MessageChannel` 握手；握手后只用专属 port，导航/重载立即作废旧 port 与 nonce。校验消息 schema、大小、速率和版本；不接受主题指定任意 URL、RPC method 或转发 header。

Bridge 仅允许读取白名单 `PublicSiteSnapshot` 和受限历史数据；不得充当通用 RPC 代理。scope 中 Agent 和 metric 明确到具体 ID/集合，数据先由 Server 按 site policy 脱敏并裁剪，再经 host broker 投递。禁止 Cookie/token、管理 DTO、raw report、process/socket、private IP、内部日志和凭据。主题脚本无网络连接权限，使用 host broker 读取公开状态；站点公开 API 是单一可信数据路径。

## 8. 公共只读站点 API

以下匿名 REST 仅服务已启用、`visibility=public` 的 site；默认无公开站点或空数据策略。所有返回值由 `PublicSiteSnapshot` DTO 裁剪，绝不返回管理 VO/内部 agent UUID。已公开到浏览器的数据无法保证撤回，发布/关闭前须明确告知站点管理员。

| 方法与路径 | 阶段 | 权限 | 输入 | 响应 | 失败/读取边界 |
| --- | --- | --- | --- | --- | --- |
| `GET /api/v1/public/sites/{slug}` | R4 | 公开 | slug；可选 `If-None-Match` | `PublicSiteSnapshot` | site 不存在/关闭/非 public 不暴露；按发布 binding 与 allowlist 快照。 |
| `GET /api/v1/public/sites/{slug}/history` | R4 | 公开 | `agentKey,metric,fromMs,toMs`；可选 `maxPoints` | `MetricSeries`（agent 标识替换为 agentKey 的公开等价结构） | 仅 allowHistory 与允许 metric；历史日数受 SiteDataPolicy 限制。 |
| `GET /api/v1/public/sites/{slug}/themes/{themeVersionId}/{path}` | R4 | 发布公开主题 | 精确已发布 version/path | 静态 bytes/ETag/cache headers | draft、私有、未发布版本不可匿名读取；严格路径检查与 CSP/nosniff。 |

公共 history DTO 中 `agentId` 以 `agentKey` 表示，返回结构为 `{agentKey,metric,unit,aggregation,points:MetricPoint[]}`；不能使用管理 `MetricSeries` 原样暴露内部 ID。站点活跃版本切换/回滚原子更新 `bindingRevision`，缓存 key 绑定 site + binding revision + content hash。撤销 public、删除或改变 allowlist 时先拒绝新访问，再使缓存、lease、订阅失效；已到浏览器的数据无法远程收回。

## 9. 后续告警、通知、备份、财务与运维方法

下列方法完整定义规划边界，但阶段 R5/R6 之前一律 `UNSUPPORTED_FEATURE`，不得 mock 成成功。每一域仍共用 RPC envelope、写幂等、服务端授权和错误规则。

### 9.1 告警规则与事件（R5）

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入条件 |
| --- | --- | --- | --- | --- | --- | --- |
| `alert.rule.list` | R5 | viewer+ | 无 | `page,pageSize,enabled` | `Page<AlertRuleView>` | 只读配置；viewer 读取脱敏规则。 |
| `alert.rule.create` | R5 | admin | `clientMutationId,name,enabled,severity,condition,windowMs,cooldownMs` | 无 | `AlertRuleView` | 按 metric 单位/阈值验证；窗口有界。 |
| `alert.rule.update` | R5 | admin | `clientMutationId,ruleId,expectedRevision` | `name,enabled,severity,condition,windowMs,cooldownMs,mutedUntilMs` | `AlertRuleView` | CAS；mute 不清除历史事件。 |
| `alert.rule.delete` | R5 | admin | `clientMutationId,ruleId,expectedRevision` | 无 | `AlertRuleDeleteResult`=`{ruleId,deleted:boolean}` | 历史事件保留；检查引用/生命周期。 |
| `alert.event.list` | R5 | operator+ | 无 | `agentId,ruleId,state,fromMs,toMs,cursor,limit` | `CursorPage<AlertEventView>` | 只列真实求值事件。 |
| `alert.event.acknowledge` | R5 | operator+ | `clientMutationId,alertEventId,expectedStateRevision` | `note` | `AlertEventView` | 仅 open 可确认；重复请求幂等。 |
| `alert.event.resolve` | R5 | operator+ | `clientMutationId,alertEventId,expectedStateRevision` | `reason` | `AlertEventView` | 真实解除条件或显式操作，保留生命周期。 |
### 9.2 通知渠道与投递日志（R5）

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入条件 |
| --- | --- | --- | --- | --- | --- | --- |
| `notification.channel.list` | R5 | admin | 无 | `page,pageSize` | `Page<NotificationChannelView>` | secret 仅返回 `secretConfigured`。 |
| `notification.channel.create` | R5 | admin | `clientMutationId,kind,name,config:JsonObject` | 无 | `NotificationChannelView` | 校验 channel schema；secret 加密 write-only。 |
| `notification.channel.update` | R5 | admin | `clientMutationId,channelId,expectedRevision` | `name,enabled,configPatch:JsonObject` | `NotificationChannelView` | CAS；secret 按辨别联合 keep/replace/clear，不回显。 |
| `notification.channel.delete` | R5 | admin | `clientMutationId,channelId,expectedRevision` | 无 | `ChannelDeleteResult`=`{channelId,deleted:boolean}` | 活跃 delivery 引用时拒绝或先停用；留审计。 |
| `notification.channel.test` | R5 | admin | `clientMutationId,channelId` | `testPayload:JsonObject` | `DeliveryLogView` | 明确副作用、限频、脱敏和审计。 |
| `notification.delivery.list` | R5 | admin | 无 | `channelId,state,fromMs,toMs,cursor,limit` | `CursorPage<DeliveryLogView>` | 只读，不含凭据和秘密正文。 |

### 9.3 存储、备份恢复与数据清理（R5）

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入条件 |
| --- | --- | --- | --- | --- | --- | --- |
| `storage.stats.get` | R5 | admin | 无 | 无 | `StorageStats` | 只读测量并注明质量，不假报容量。 |
| `backup.plan.list` | R5 | admin | 无 | `page,pageSize` | `Page<BackupPlanView>` | 凭据不回显。 |
| `backup.plan.create` | R5 | admin | `clientMutationId,name,enabled,schedule,targetKind,retentionCount` | `targetConfig` | `BackupPlanView` | 校验目标/容量/schedule；秘密加密。 |
| `backup.plan.update` | R5 | admin | `clientMutationId,planId,expectedRevision` | `name,enabled,schedule,retentionCount,targetConfigPatch` | `BackupPlanView` | CAS；凭据 write-only keep/replace/clear。 |
| `backup.plan.delete` | R5 | admin | `clientMutationId,planId,expectedRevision` | 无 | `BackupPlanDeleteResult`=`{planId,deleted:boolean}` | 检查运行中任务/引用；不删除已有备份。 |
| `backup.run.create` | R5 | admin | `clientMutationId,planId` | `reason` | `BackupRunView` | 异步 Server 本地 operation；幂等；检查目标可写。 |
| `backup.run.list` | R5 | admin | 无 | `planId,state,fromMs,toMs,cursor,limit` | `CursorPage<BackupRunView>` | 仅列真实运行记录。 |
| `backup.run.get` | R5 | admin | `backupId` | 无 | `BackupRunView` | 不泄露远端凭据。 |
| `backup.run.download` | R5 | admin | `backupId` | 无 | `BackupDownloadGrant`=`{grantId:string,expiresAtMs:integer,checksum:string}` | 短时、单次授权消费；审计、checksum 校验。 |
| `backup.restore` | R5 | admin + step-up | `clientMutationId,backupId,expectedMaintenanceRevision,stepUpProof` | `dryRun:boolean` | `BackupRestoreOperation`=`{operation:ServerOperationView,verification:string,maintenanceMode:boolean}` | 仅本地受控维护流程；需先读取维护状态/进入维护并取得 revision；HTTP 不覆盖活动 DB。 |
| `backup.prune.preview` | R5 | admin | `expectedStorageRevision,keepCount` | `beforeMs` | `PrunePreview`=`{previewId:string,expectedStorageRevision:revision,expiresAtMs:integer,candidateCount:UInt64String,candidateBytes:UInt64String,requiresStepUp:boolean}` | 不删除；列出满足保留策略且未被引用的候选集。 |
| `backup.prune.commit` | R5 | admin + step-up | `clientMutationId,previewId,expectedStorageRevision,stepUpProof` | 无 | `ServerOperationResult` | 短时 preview 绑定精确候选集、CAS 与 step-up；只删获准文件。 |
| `data.retention.get` | R5 | admin | 无 | 无 | `RetentionPolicyView`=`{revision:revision,reportDays:integer,auditDays:integer,backupDays:integer,updatedAtMs:integer}` | 只读策略。 |
| `data.delete.preview` | R5 | admin | `scope,beforeMs` | `agentId,taskKind` | `DataDeletePreview` | 仅估算；绑定筛选和当前 revision。 |
| `data.delete.commit` | R5 | admin + step-up | `clientMutationId,previewId,expectedRevision,confirmation,stepUpProof` | 无 | `ServerOperationResult` | preview 未过期、确认、CAS、proof 匹配；异步 Server 清理。 |
| `storage.remoteTarget.test` | R5 | admin | `clientMutationId,targetConfig` | 无 | `RemoteTargetTestResult`=`{state:string,checkedAtMs:integer,errorCode:string|null}` | 出站策略/SSRF 防护；秘密不记不返。 |

### 9.4 财务配置、审计与部署状态（R5）

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入条件 |
| --- | --- | --- | --- | --- | --- | --- |
| `finance.config.get` | R5 | admin | 无 | 无 | `FinanceConfigView` | 只读 typed allowlist。 |
| `finance.config.update` | R5 | admin | `clientMutationId,expectedRevision,currency,units:FinanceUnit[]` | 无 | `FinanceConfigView` | 只收受控 keys/范围/货币，拒绝任意系统配置透传。 |
| `audit.list` | R5 | admin | 无 | `actorUserId,action,targetType,fromMs,toMs,cursor,limit` | `CursorPage<AuditEntry>` | append-only，只读；摘要不得含秘密。 |
| `deployment.status.get` | R5 | viewer+ | 无 | 无 | `DeploymentStatus` | 实际只读构建/部署状态。 |

### 9.5 TOTP、Passkey、API Token（R6）

| 方法 | 阶段 | 权限 | req | opt | 响应 VO | 失败/写入条件 |
| --- | --- | --- | --- | --- | --- | --- |
| `mfa.totp.enroll.begin` | R6 | 本人有效会话+CSRF | `clientMutationId` | `stepUpProof` | `TotpEnrollChallenge` | secret 一次显示；R6 才规划 TOTP，未完成不启用。 |
| `mfa.totp.enroll.complete` | R6 | 本人+CSRF | `clientMutationId,challengeId,code` | 无 | `MfaStatus` | Server 验证后原子启用；挑战单次消费。 |
| `mfa.passkey.begin` | R6 | 本人会话 | `clientMutationId,purpose` | 无 | `PasskeyChallenge` | 绑定 Origin/RP ID/actor/purpose。 |
| `mfa.passkey.complete` | R6 | 本人会话 | `clientMutationId,challengeId,credentialResponse:JsonObject` | 无 | `MfaStatus` | Server 校验 challenge、签名和 counter。 |
| `apiToken.create` | R6 | admin + step-up | `clientMutationId,name,scopes:string[],stepUpProof` | `expiresAtMs` | `ApiTokenCreated`=`{token:ApiTokenSummary,secret:OneTimeSecret}` | proof 绑定规范化请求；scope 白名单/期限；secret 一次显示及加密短 TTL 恢复。 |
| `apiToken.list` | R6 | admin | 无 | `page,pageSize` | `Page<ApiTokenSummary>` | 绝不返回明文/摘要。 |
| `apiToken.revoke` | R6 | admin | `clientMutationId,tokenId,expectedRevision` | 无 | `ApiTokenRevokeResult`=`{tokenId:string,revoked:boolean}` | 即时吊销并审计；与 enrollment token 隔离。 |

普通管理写始终要求 UUID `clientMutationId`、适用 CAS 和幂等记录。仅认证挑战 begin/complete 是窄例外：begin 仍需其独立 `clientMutationId`，complete 必须携带一次性 `challengeId` 并在成功/失败安全判定后消费；同一挑战重试只可返回相同已完成回执，不可再次执行副作用。此例外不豁免任一管理写的幂等要求。R6 之前不提供登录 MFA。

`backup.restore` 的维护状态由 R5 只读 `maintenance.status.get` 读取；仅本机受控维护入口可原子进入/退出维护并递增 revision，HTTP RPC 不得切换维护态或覆盖正在使用的数据库。下载 grant 由 `POST /api/v1/backups/downloads/{grantId}/consume` 携带会话消费一次，服务端在授权后流式发送备份；URL 不含 bearer secret，grant 绑定 actor/backup/checksum/期限。
### 9.6 Shell、审批和脚本执行：远期独立产品边界（R6+）

当前不列入可调用 RPC catalog，也不将 `command` 塞进 `JobTask`。未来若产品批准，需另行设计 `execution.create/list/get/cancel`、`approval.request/list/decide` 与受控脚本库 API：执行必须有独立 `executionId`、目标 Agent、固定命令/脚本引用与内容 digest、理由、创建 actor、风险分类、超时/输出限额、幂等 key、审计；命令由 Server allowlist/策略验证，绝不为通用字符串提供隐式 root shell。

有审批要求时，状态至少区分 `pendingApproval|approved|rejected|expired|dispatching|running|succeeded|failed|cancelRequested|cancelled|unknown`；批准人不得与申请人相同（若策略要求分权），批准绑定目标/命令 digest/过期时间并单次消费。取消是请求，不承诺远程进程已终止；ACK 缺失须标 unknown。Agent 能力、风险、输出脱敏和跨重启重复副作用都须单独验证。没有该独立安全设计前所有 Shell/审批方法明确 `UNSUPPORTED_FEATURE`。

## 10. Agent、Probe、Plus 任务配置约束

首版 taskKind 至少区分内置采集、Probe 和已安装 Plus。每个 kind 用版本化 `kind` + 自有 config Schema，Server 使用可信目录验证/编码 Agent Proto；不得只接受前端 Zod 或任意 protobuf JSON。无秘密的基础采集可提供显式 profile；不自动为新 Agent 创建 Job。

Probe 首版仅按 Agent 当前 observed capability 开放 `ICMP/TCP/HTTP/UDP`，不伪造 `wss`。应限制目标长度、协议、端口范围、尝试次数、并发、超时、响应体量、重定向和频率；Agent 网络目标还受部署 egress policy/Agent 权限限制。私网探测是明确产品授权能力，应有管理域/Agent 范围策略和审计；这不授权 Server 替 UI 抓取任意 URL。HTTP Probe 如允许 URL，须拒绝凭据、危险 scheme、越权重定向并明确域名/网段策略。

Plus 仅操作 Agent 已安装 inventory。以 `pluginId + version + schemaHash + taskKind` 绑定描述符、runtime config 和 Job。Server 不安装插件二进制，不信任动态返回 JS/HTML。runtime config 读路径按 schema 脱敏；若秘密字段元数据未知，保守隐藏整个配置。秘密以部署独立密钥加密保存，密钥不放数据库备份/Git。schema/worker ACK 不能代表 Worker 仍实时活跃；实时活跃状态必须有观测时间/来源。

## 11. 查询、缓存与真实状态

查询服务须从已入库 report/event、Agent 会话观测及持久化 Job/operation 得出结果。投影是可重建读取优化，不是第二事实源；历史 report 按 cursor 查询。Job binding 明确每个 dashboard metric 来源，多个相同 taskKind 时不任意取报告。乱序/旧 revision 报告可入历史但不覆盖更新投影；Sample 的 `sampledAtMs` 与 `receivedAtMs` 必须分开。

值缺失应给出质量原因（如 `notConfigured`、`awaitingReport`、`permissionDenied`、`unsupported`、`clockSkew`、`sourceStale`）。离线与 stale 是不同维度：Agent 离线仍可有最后观测，但其质量 stale；无样本不得伪造 0。Task 只有真实事件才显示运行状态。客户端展示层不可把空列表改成示例 Agent，不可把请求失败切换为 Mock。

HTTP poll 和 WS 推送共用同一 VO/权限过滤。缓存必须按 actor/site policy/revision 分区；权限撤销、用户退出、站点策略修改、主题撤销时清理/失效。历史查询默认 7 天窗口/最多 1000 点；采样桶聚合和计数 reset 规则按 metric 定义。原始报告/详细进程/Socket 不作为普通 metrics summary 的扩展字段。

## 12. 持久化、恢复和兼容性原则

建议由 Rust 业务层复用已有 Agent、keyring、Job catalog/history、runtime、TaskReport、事件和 command 存储；扩展必要的用户/session、metadata、operation、幂等、audit、指标绑定/投影、主题/site/lease、告警/通知/备份领域存储。此为设计方向，不声称这些表已存在。

所有新存储 schema 以新增版本 migration 建立，不改写既有 migration。涉及会话/凭据/operation 的状态要有服务端持久化与恢复策略。写入配置和 operation 必须能事务关联；Agent 通知发生在提交后，重启后扫描未同步 operation 并对账。Agent 在线状态在 Server 启动后须等新连接观测，不能从数据库旧连接假报 online。

保持现有 CLI 与 Agent 兼容；Web 与 CLI 共用业务验证与领域规则，不应因 Web 使用 DTO 而分叉授权状态。若需要 Agent 新字段/协议变更，须独立兼容设计和验收。migration 回滚优先通过应用版本兼容，不以删表丢数据作为回滚。部署默认关闭未验收 Web feature。

前端旧字段只在后置 adapter 做映射；`serverId` 不作为 `agentId` 契约。页面同一运行模式不得混用真实数据与假数据。真实模式网络错误/权限错误/空数据/未知指标分别呈现；不可由接口错误回落 demo。`api.md` 仅作为旧 Mock 需求迁移索引，方法处置建议见 §13.2。

## 13. 分期与前端旧方法映射

### 13.1 Rust 优先分期

| 阶段 | 后端交付顺序 | 退出验收 |
| --- | --- | --- |
| R0 | 领域契约、VO schema、错误/权限定义、用例和不变量 | 每个方法输入输出/error/enum 有正反例；明确所有未知状态和阶段 feature gate。 |
| R1 | Rust Web 身份、bootstrap、会话、CSRF/Origin、角色/授权、审计基础、meta | HTTP/WS 未授权拒绝；退出/吊销后两通道均失效；秘密不入日志。 |
| R2 | Agent/enrollment、metadata、Job CAS、持久 operation/idempotency、Agent 目录应用、报告/事件查询 | agent 注册真实 Noise 流程；创建/修改/停用任务、离线/策略拒绝/ACK 丢失可解释；重启恢复同步。 |
| R3 | 真实 metrics 查询/历史/投影/显式 profile、订阅与限流 | 有界查询、乱序保护、真实零值与 null、权限/重连/resync 正确；服务层已具备真实聚合。 |
| R4 | Plus runtime（已安装/Echo）、主题 ZIP/隔离 origin/预览/站点公共数据 broker | Echo 端到端；恶意 ZIP/HTML/消息/租约/权限撤销安全测试；主题不接触管理 Cookie/API。 |
| R5 | 告警、通知、备份恢复、数据保留/清理、财务 typed config、审计完善、部署只读 | 各域副作用/step-up/SSRF/恢复演练/审计留痕分别验收。 |
| R6+ | TOTP/Passkey/API tokens、审批/Shell 独立安全产品 | challenge、防重放、权限分权、远程执行副作用和取消边界单独批准验收。 |
| UI 适配 | 每一后端契约通过后再映射旧 VO；最后统一前端 adapter/浏览器 E2E | UI 不决定领域 DTO；单独验收真实 Rust + Agent + Echo 浏览器闭环。 |

R0/R1 起先 Rust 领域/服务/查询，再按阶段适配 UI；API 契约可先供 UI review，但不能先用 mock server 冒充后端。前端 `api.md` 不据以要求 Rust 暴露所有旧方法。最终验收必须分后端协议/CLI 或 test client 测试，与真实 browser + Server + Agent + Echo 测试两层；运行接口尚未实现前不得标为通过。

### 13.2 旧前端 Mock 方法的处置矩阵

| 旧 Mock 语义/字段 | 目标 v1 | 阶段/处置 |
| --- | --- | --- |
| `serverId`、`servers.*` 主机集合 | `agentId`、`agent.list/get` | R2；adapter 转字段，不能把 Agent 叫 Server。 |
| 表单直接 `agent.register` | `enrollment.create/list/get/revoke` + 真实 Agent Noise 注册 | R2；仅 enrollment 管理凭据不代表 Agent 已上线。 |
| `agent.update` 混合状态/费用/告警/主题字段 | `agent.updateMetadata`、`job.*`、`plugin.runtime.*` 等独立写模型 | R2/R4/R5；拒绝全量混合 patch。 |
| `agent.updateConfig` | Job、Plus runtime 分域 | R2/R4；CAS、schema 与敏感配置语义。 |
| `task.dispatch`、`task.approve` | 首版不提供；未来 `execution.*` 独立提案 | R6+；无任意命令映射。 |
| `metrics.*` 示例/统计 | `metrics.latest/history` | R3；只读真实入库 report 与绑定。 |
| `probe` / `wss` | Job task kind 的 ICMP/TCP/HTTP/UDP | R2；不伪称 wss 支持。 |
| `plugin.*` 任意安装或 JS config | `plugin.list/schema.get/plugin.runtime.*` | R4；仅已安装 Plus、描述符可信，不执行描述符代码。 |
| 主题/组题 UI 自由模板 | `theme.*` + `site.*` + 专用隔离 renderer | R4；主题为用户上传完整 HTML/CSS/JS 静态包，非管理 UI 插件。 |
| `alert.*` | `alert.rule.*` 与 `alert.event.*` | R5；规则与事件生命周期分离。 |
| `notification.*` | `notification.channel.*` / delivery logs | R5；秘密 write-only、出站 SSRF 防护。 |
| `backup.*`、存储清理 | `backup.*`、`storage.*`、`data.delete.*` | R5；restore 维护模式 + step-up，删除先预览确认。 |
| 任意财务 config key | `finance.config.*` typed allowlist | R5；不透传系统配置。 |
| `deployment.switch` | `deployment.status.get` | 只读；编译/嵌入模式不可由 POST 切换。 |
| WebShell / 脚本仓库 / 审批 | 远期 `execution.*` 草案 | R6+；不是 Job，不在首版 method catalog。 |
| `auth.*` localStorage 模拟身份 | REST Cookie session + 服务端授权 | R1；禁止客户端构造 admin。 |

## 14. 验收测试矩阵

以下是实施时应新增并独立运行的场景，不表示当前已经通过。协议测试应直接验证 Rust Server DTO/服务/数据库；浏览器测试另验真实 UI 与网络，不以 Mock 单测替代。

### 14.1 契约、权限和身份

- 每个 RPC 请求/响应 schema 正向、缺失字段、未知字段、未知 enum、越界数值、超深 JSON、重复键与超长内容。
- HTTP/RPC 未认证、viewer/operator/admin 权限矩阵；资源不可见与 NOT_FOUND 策略；CSRF、Origin、跨站 login、限流、Cookie flags、会话 TTL/吊销、WS 握手后撤权。
- `requestId` 不等于 `clientMutationId`；并发相同键/相同 hash 仅一项写入，同键不同 hash 冲突；旧 CAS 拒绝且无局部变更；幂等回执过期不自动再签发。
- session/token/enrollment/Plus secret 不出现在 list/get、错误、日志、审计、URL、Referer 或 WS；step-up actor/target/input hash 不匹配、过期、重复使用都拒绝。

### 14.2 Agent、Job、operation 和 report

- enrollment 创建不造成在线 Agent；真实 Noise 注册成功后才出现 Agent；撤销/过期/消费状态准确。
- Job create/update/enable/delete 使用正确双 CAS；并发改 catalog 不覆盖其他 Job；interval/once/六段 cron + 时区/无效 misfire 校验。
- Agent 离线为 waitingAgent；策略拒绝为 blocked；不支持能力为 CAPABILITY_UNSUPPORTED；空过滤目录 ACK 不确认 applied；迟到旧 ACK 不覆盖新 revision。
- Server 在 DB commit 后、Agent notify 前崩溃可恢复；超时 unknown 不提示失败回滚；应用配置后无 TaskRun 不显示 running；Job 删除不会伪造报告/立即擦除历史。
- 报告/事件分页 cursor 绑定筛选；原始报告/进程/Socket 权限隔离和脱敏；Agent/Server 重启后状态重新观测。

### 14.3 指标与 WS

- 真实 0 CPU/0 Bps 保留；无采集 job 显示 warmingUp/notConfigured；离线与 stale 区分；null 不是 0。
- `sampledAtMs` 只代表真实起始时刻，`receivedAtMs` 分开；同指标不同组采样时刻不强行对齐；旧 revision/乱序报告不覆盖新投影，累计计数 reset 不出负速率，空桶 null。
- history 时间窗/1000 点边界、cursor 筛选隔离；权限变化使缓存失效；敏感 Process/Socket 不出现在 summary/全量广播。
- WS 只允许控制方法；旧 epoch/sequence 拒收、重连快照水位正确、取消确认、慢客户端有界队列与 resync；WS 写请求被拒，HTTP 写失败不得自动 WS/HTTP 双投。

### 14.4 主题与公开站点安全

- ZIP slip、盘符/UNC/绝对路径、规范化重复/大小写碰撞、NUL、链接、加密压缩包、压缩炸弹、entry/深度超限、MIME 欺骗、可执行文件和损坏 manifest 均拒绝且暂存清理。
- 上传正文相同 key 哈希重试返回同 upload；不同内容同 key 冲突；校验状态真实可查；未完成操作重启可恢复；未发布/私有 theme asset 不能匿名读取。
- 主题 iframe 无管理 Cookie、无同源/forms/popups/top navigation；CSP 阻断 connect/eval/Service Worker/未授权外部资源；nonce/source/MessageChannel 校验阻止恶意 postMessage 与导航后旧 channel。
- bridge 不支持任意 URL/RPC；仅 scope agent/metric allowlist；无 raw report、private IP、进程、管理 DTO、secret；撤 lease、退出、修改 site policy 立即阻断后续访问并清缓存。
- site activate/rollback 原子 CAS；publish 不自动 activate/public；公开撤销阻止新请求并说明不能召回已发到浏览器的数据；无安全 origin 配置时不降级同源预览。

### 14.5 后续业务与真实端到端

- 告警规则窗口/cooldown/mute 与事件 ack/resolve 生命周期分别验证；通知 URL SSRF/重定向/DNS 变化防护，日志不泄密。
- 备份 checksum/恢复演练、维护模式、step-up、HTTP 不覆盖活动 DB、data delete preview 过期/CAS/真实执行/审计；财务拒绝未知 key；deployment 只读。
- API token 一次显示/吊销；TOTP/Passkey challenge actor/origin/过期/重放；Shell 在未单独批准前返回 unsupported，不能进入 Job。
- 最终 E2E 关闭 Mock，真实浏览器、Rust Server、真实 Agent、Echo Plus 与 SQLite 覆盖登录、接入、CPU/内存/Probe Job 创建修改停用、历史、权限拒绝、断线/重启、Plus runtime/Worker ACK、主题恶意包和隔离测试。测试资源/凭据必须清理；证据关联 agent/job/revision/operation。

## 15. 设计决策与未定部署选项

**用户已确认的范围**：Web API 全量规划；Rust 优先、先模型/服务/查询再前端 VO；核心管理和用户主题纳入本设计；告警、通知、备份等后续接口完整规划；首版任务仅采集/Probe/已安装 Plus；Shell/审批后置。主题为用户上传完整 HTML/CSS/JS 页面包，需独立预览及安全隔离。

**本文推荐但仍需实施前确认的设计决定**：普通管理统一 HTTP JSON-RPC 2.0；WS 仅订阅控制/通知；admin/operator/viewer 三角色；主题隔离 origin/CSP 与 sandbox 部署方式；上传限额、session TTL、幂等保留至少 24h、历史默认上限 7d/1000 点；R4 公共 site broker 字段细节。这些是可审查的 proposed 值，不是当前配置或已经批准的实现行为。

**实施前必须由部署/安全验收确定的参数**：管理与主题资源的独立 site/origin、TLS/CORS allowlist、管理员 bootstrap 操作、会话 absolute/idle TTL、秘密加密密钥注入/轮换/备份排除、报告/审计留存期、部署规模与限流/容量、公共站点数据发布责任和告警通知 egress allowlist。未配置必要主题隔离 origin 时功能应关闭，而非安全降级。

接口契约通过评审不自动代表实施完成，也不证明 Agent/Server 已具备所列能力。每一阶段需分别批准 migration、兼容策略、安全边界和自动化验收；未到阶段的方法在运行部署中须明确 unsupported/planned，绝不提供成功空壳或 Mock 回退。
