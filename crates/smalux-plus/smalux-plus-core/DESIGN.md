# Smalux Plus Core 设计总结

## 1. 目标

`smalux-plus-core` 是 Smalux Plus 插件系统的公共契约层。它定义插件身份、版本、平台、任务能力、动态参数 Schema、Worker IPC 帧、任务 SDK 和错误模型，让 Agent、Server 与具体 Plus 插件共享同一套概念。

当前实现只使用独立 Worker 可执行文件。Agent 不加载插件代码，双方只通过 stdin/stdout 上的 Protobuf 帧交互；网络、Noise、数据库与调度器不进入该 crate。

## 2. 分层

```text
smalux-plus-core       公共类型、Manifest、参数 Schema、Worker IPC、任务 SDK 和错误
        |
        +-- Agent host  本地发现、校验、启动 Worker、执行、取消和排空
        +-- Plus SDK    插件开发者注册 PlusTask 并运行 Worker
        +-- Repository  TUF 元数据、下载、校验和版本选择
        +-- Server      期望版本、分发命令和 Agent 状态对账
        +-- Rustic      具体业务插件，独立于 Agent 核心
```

核心层不依赖 Agent、Server、Protocol 或某个业务插件，避免循环依赖和实现细节泄漏。

## 3. 插件描述

每个插件通过 `PluginManifest` 描述：

- `plugin_id`：稳定身份，例如 `smalux.plus.echo`。
- `display_name`：仅用于展示，不参与身份判断。
- `version`：插件自身版本。
- `platform`：Windows、Linux 或 macOS。
- `protocol_version`：Worker stdin/stdout IPC 主版本；当前为 2。
- `schema_file`：参数 Schema Bundle 在版本目录中的相对路径。
- `task_types`：一个 Worker 可以提供多个任务类型。

任务执行前必须检查插件处于 active 状态，并且清单声明了请求的 `task_type`。任务不能因为缺少插件而隐式安装原生代码，应直接返回 `PluginUnavailable` 类错误。

### 3.1 参数 Schema

每个 `schema.pb` 保存 `FileDescriptorSet` 与声明式字段提示。前者是参数和结果二进制格式的权威来源，后者只决定管理端采用文本、数字、下拉、可输入下拉、开关或密码等控件。Schema 不携带网页代码，Server 也不加载插件前端。

Agent 清单只上报 Schema SHA-256。Server 先查询最多 256 项的解析缓存，再查询数据库；只有两者都缺失时才请求完整 Bundle。数据库中的 Bundle 不可变，同一插件版本不能绑定不同 Schema，参数契约变化必须提升插件版本。

```text
Agent Inventory(hash)
    -> Server memory
    -> Server database
    -> miss: SchemaQuery(hash)
    <- Agent SchemaResponse(hash, bundle)
    -> validate -> persist -> cache
```

## 4. Worker IPC 原则

每个插件版本是一个独立可执行文件。Agent 启动后，先发送 `Hello` 并验证 `plugin_id` 和协议版本，再发送 `Initialize` 传递当前会话的运行时配置、配置格式版本与并发上限；只有收到第二个 `Ready` 才会将 Worker 标记为可执行。

```text
Agent stdin -> [4-byte big-endian length | WorkerFrame protobuf] -> Worker
Agent stdout <- [4-byte big-endian length | WorkerFrame protobuf] <- Worker
```

`stdout` 只能写 WorkerFrame，日志必须写 `stderr`。每个 `ExecuteTask` 有唯一 request_id，并且只产生一个最终 `TaskResult` 或 `ErrorResponse`；不发送 Started、Pong 或独立 Cancelled 中间帧。取消时 Agent 发送 `CancelTask`，Worker 通过 `CancellationToken` 协作结束任务，最终返回 `TaskResult(Cancelled)`。stdin EOF 也会触发同样的任务取消和插件收尾。单帧上限为 8 MiB。

## 5. 生命周期

正式 Worker 管理器按以下状态工作：

```text
发现 -> 校验 Manifest -> 启动 Worker -> Hello -> Initialize -> execute
                                                    |
                                      新快照 -> draining -> shutdown -> 启动新 Worker
```

热更新采用安全排空：旧 Worker 停止接收新任务，等待正在执行的任务结束；超时则终止本次切换并保留旧 Worker。当前版本已经实现会话内启动、替换、取消确认和退出回收。

当前实现已经具备以下生命周期保证：

- `PluginWorkerClient::spawn_and_initialize` 完成 Hello、Initialize 两阶段确认后才暴露 Worker；
- 取消会等待 Worker 返回已经结束的 `TaskResult(Cancelled)`，超出 shutdown timeout 才强制终止；
- Plus Task panic 会转换为带原始 `request_id` 的失败结果，不会让 Worker IPC 读循环卡住；
- `InvalidConfig` 使用独立状态返回，Agent 将其作为永久错误停用 Job，而不是重试相同参数；
- Worker 结果帧写失败会尽量返回小型 `ErrorResponse`，避免 Agent 无期限等待；
- Agent 退出时先停止 Scheduler，再调用 `PluginManager::shutdown_all`，最后排空结果出口；
- Worker 异常退出会进入有限次数、有限时间窗口的指数退避重启，达到阈值后暂停并通知 Server。

版本目录至少保留 current、previous、staged 三个版本，以便失败时回滚。回滚只能选择当前可信仓库仍发布、未撤销且不低于安全下限的版本。

## 6. Agent 与 Server 调用流程

```text
Server 保存 desired(plugin_id, version)
    |
Agent 建立会话并上报 observed inventory
    |
Server 对账：缺失 -> Stage 命令；已准备 -> Activate 命令
    |
Agent 下载并校验插件，执行 stage/activate
    |
Agent 回报结果和新的 inventory
```

Agent 默认关闭远程插件管理。启用后仍需满足总开关和 `plugin_id` 白名单；Server 不能远程修改仓库地址、信任根或白名单。

## 7. 仓库与安全

后续默认使用独立的 `smalux-plugins` GitHub Pages 仓库。TUF root 公钥随 Agent 发布，私钥永不进入代码仓库。Server 命令只携带插件 ID 和版本，不接受 Agent 任意指定下载 URL、哈希或公钥。

TUF 校验负责防止篡改、回滚、过期和混合元数据。下载文件还必须匹配清单中的平台、Worker 协议版本、大小和摘要。

## 8. 错误处理

- 清单损坏：拒绝 stage。
- Worker 协议版本不兼容：拒绝 activate，保留当前版本。
- 任务类型未声明：拒绝 execute。
- 执行超时：结束本次任务并保留插件进程状态。
- reload 排空超时：保留旧版本，不强制卸载。
- 版本被撤销：停止新任务，排空并卸载，向 Server 报告。

## 9. 后续实现顺序

1. 接入手工安装的 CLI 查看，以及下载、大小和签名校验接口。
2. 接入 TUF 仓库和 Agent 本地远程管理开关。
3. Server 增加持久化的 desired/observed 状态和重连后的自动对账。

## 10. 当前边界

本模块不负责：连接建立、Noise/TLS、数据库、调度器、REST/gRPC 路由、插件下载、插件签名验证和业务 Secret 持久化。Worker 进程降低了宿主进程内代码执行风险，但它不是操作系统级沙箱；资源隔离、权限收缩和容器化需要由部署层另行配置。
