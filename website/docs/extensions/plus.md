---
title: Plus 模块
description: 将备份等可选、有副作用能力隔离为独立扩展。
---

# Plus 模块

`smalux-plus` 用于不应增加 Agent 核心耦合的可选能力。Plus 以独立 Worker 进程运行，
Agent 负责发现和调度，Server 只保存能力、参数 Schema 和任务结果，不安装插件二进制。

## 组件边界

```text
smalux-protocol
    配置、结果、错误和能力契约

smalux-plus-core
    Manifest、参数 Schema、Worker IPC 和插件开发接口

smalux-plus-echo / smalux-plus-rustic
    插件自己的 Protobuf 参数、执行逻辑和结果

smalux-agent TaskFactory
    把 Proto 分支映射到启用的 Plus 工厂

smalux-agent RemoteJobController / Scheduler
    不理解 Rustic 业务细节
```

Plus crate 不处理 gRPC Session，Protocol crate 不访问具体插件，Scheduler 不解释插件参数。

## 插件包结构

每个安装版本必须提供 Worker、Manifest 和参数 Schema：

```text
plugins/
└── smalux.plus.echo/
    └── 0.1.0/
        ├── plugin.json
        ├── schema.pb
        └── smalux-plus-echo-worker.exe
```

`plugin.json` 的 `schema_file` 必须是版本目录内的相对路径。Agent 在启动 Worker 前读取
`schema.pb`，校验插件 ID、版本、Task kind 和 SHA-256；路径越界、Schema 损坏或清单不一致
都会导致该插件版本不进入本地清单。

## 参数 Schema

`schema.pb` 是 `PluginSchemaBundle` 的 Protobuf 编码，由两部分组成：

- `FileDescriptorSet`：参数与结果消息的真实 Protobuf 类型；
- 声明式 UI 提示：字段名称、控件、默认值、单位、静态选项和基础验证规则。

第一版控件包括 `AUTO`、`TEXT`、`TEXTAREA`、`NUMBER`、`SELECT`、`COMBOBOX`、
`TOGGLE` 和 `PASSWORD`。`SELECT` 只能选择静态选项；`COMBOBOX` 可以选择预设值，也可以
手动填写。第一版不查询 Agent 的实时网卡、磁盘等动态选项，也不执行插件提供的 JavaScript、
HTML 或 React 页面。

单次 Job 的参数仍放在 `PluginTaskConfig.task_config` 中，并按 Schema 指定的配置消息编码。
Agent 不解释该字节，只将其交给对应 Worker：

```text
Server 表单/配置
  -> 根据 FileDescriptorSet 编码 task_config
  -> PluginTaskConfig
  -> Agent PluginManager
  -> Worker ExecuteTask.config
  -> PlusTask::execute
```

### Echo 示例参数

`smalux.plus.echo` 用一个没有外部副作用的 Worker 演示完整参数链路：

| 字段 | 控件 | 示例 | 执行效果 |
| --- | --- | --- | --- |
| `message` | 文本 | `hello` | 原样写入结果消息 |
| `delay_millis` | 数字 | `500` | 返回前等待 500 毫秒，可被取消 |
| `fail` | 开关 | `false` | 设置为 `true` 时返回示例失败 |
| `mode` | 下拉 | `ECHO_MODE_NORMAL` | 写入结果，演示枚举选项 |
| `target` | 可输入下拉 | `default` | 写入结果，演示预设值或自定义文本 |

Worker 返回的 payload 是 `EchoTaskResult`，包含 `message`、完成时间、`mode` 和
`target`，可以同时检查参数编码、Worker 执行和结果解码。

## AgentContext 与插件持久化

Worker 初始化时会获得只读 `AgentContext`：Agent 版本、操作系统、架构、可选的 Agent ID 摘要、
数据目录、配置目录、插件目录和当前插件专属数据目录。Agent ID 在认证状态尚未同步到插件管理器时
可以为空；插件不能把它当作必填身份凭据。插件的业务状态由插件自己保存：

```text
<data_dir>/plus/<plugin_id>/<plugin_version>/
```

Agent 不会把 Token、私钥、Noise 状态或 Server 凭据注入插件。Echo 会在自己的目录中维护
`execution-count.txt`，用来演示插件持久化不需要 Agent 理解业务内容。

## Schema 上报流程

Agent 重连时不会重复上传完整描述符：

```text
Agent -> Server: Inventory(plugin_id, version, task_kinds, schema_hash)
Server: 内存缓存 -> 数据库
Server -> Agent: SchemaQuery(仅缺失的 hash)
Agent -> Server: SchemaResponse(hash, schema.pb)
Server: 校验 hash/身份/版本/Task -> 写数据库 -> 放入内存缓存
Server -> Agent: PluginRuntimeSnapshot
```

数据库以 `schema_hash` 作为内容地址，同时约束同一个 `(plugin_id, plugin_version)` 只能绑定
一个 Schema。参数契约改变时必须提升插件版本。Server 进程内最多缓存 256 个已解析 Schema；
超过后仍然持久化，只是不继续扩大内存缓存。Server 重启后可直接从数据库恢复，不要求 Agent
重新上传。

## 运行配置更新

Agent 已经运行某个插件时，收到新的 `PluginRuntimeSnapshot` 会按插件 ID 和版本比较：

- 配置字节和并发数完全相同：复用现有 Worker，不重启；
- 配置或并发数发生变化：先启动并完成新 Worker 的 Hello/Initialize，成功后切换映射，
  再向旧 Worker 发送 Shutdown 并回收进程；
- 新 Worker 启动失败：保留旧 Worker，返回拒绝 ACK，当前任务继续使用旧配置；
- 插件从快照移除：发送 Shutdown 并回收对应 Worker。

快照 revision 只是运行配置版本，不能单独导致 Worker 重启。Server 重连后可以重复下发
同一快照，Agent 会按上述规则幂等处理。

### Worker 崩溃和 Job 暂停

Agent 会周期检查 Worker 子进程和 stdout reader。异常退出会按本地配置的失败窗口和重启次数
进行后台恢复，默认在 `10m` 内失败 `3` 次后进入 `paused`，停止该 Worker，并发送
`AgentPluginSync.pause_notice`。配置替换和主动 Shutdown 不计入崩溃次数。

Worker 的 Hello、Initialize 和 Execute 都受 Agent 本地超时保护；Worker 不响应时 Agent 会发送
Cancel，仍不退出则回收子进程。配置替换会先等待优雅 Shutdown，超时后才强制终止。

Initialize 会把共享 `runtime_config` 交给每个 `PlusTask::initialize`。所有 Task 初始化成功后
Worker 才发送 Ready；Agent 下发的并发上限与插件自身上限取较小值，实际并发会在 Worker Ready
中确认。Worker 在 Initialize 前收到 Execute 或重复 Initialize 会拒绝协议。

Server 按 `agent_id + plugin_id + version` 保存暂停状态，只从该 Agent 的 Job 快照中移除对应
插件任务，其他 Agent 仍可使用同一插件。暂停通知得到 `pause_acknowledgement` 后不再重复发送；
断线重连会重发未确认通知。只有更高的 `PluginRuntimeSnapshot.revision` 才能解除暂停并重新
启动 Worker，Agent 不会自动重试已经失败的 Plus Task。

Schema 不允许包含 Secret。第一版所有普通运行参数统一使用 Server 下发的 `config`，只在
Noise 会话、Agent 内存和当前 Worker 中存在。需要凭据的插件暂时使用稳定资源引用；后续再由
专用安全存储接口提供最小权限凭据。

## 使用资源引用

远程 Proto 只应携带稳定引用：

```proto
message RusticBackupTaskConfig {
  string repository_id = 1;
  string source_id = 2;
}
```

Agent 使用 `repository_id` 和 `source_id` 查询本地配置及安全存储。不要把仓库密码、云访问 Token、完整
shell 命令或任意路径直接放进 Job Proto。

## 有副作用任务

备份、更新、脚本和修复与只读采集不同，接入统一 Job 前必须回答：

- Server 是否允许远程创建和 `RunNow`；
- Agent 本地允许访问哪些目录、仓库和凭据；
- 重复命令是否安全，持久化幂等键是什么；
- Agent 重启后如何恢复 running/succeeded/failed 状态；
- 断联后继续多久，何时暂停；
- timeout 或 cancel 是否真的能终止底层进程；
- 日志、结果和审计保留多久；
- 如何防止任务耗尽磁盘、带宽和进程资源。

`RemoteJobController` 的 `command_id` 缓存是有限的进程内幂等窗口，不能替代备份操作的持久化幂等。Plus Task
必须保存业务 run ID 和最终状态，使 Agent 重启后仍能识别已开始或已完成的操作。

## Feature 与分发

可选 Plus 能力可使用 Cargo feature 或不同 Agent 发行物控制，但协议字段不能因 feature 改变编号。
Server 必须依据 Agent 上报的 capability 下发任务；未启用的 Agent 返回明确不支持错误。

当前实现固定使用独立 Worker 子进程，不把第三方动态库加载进 Agent 地址空间。Echo 的
`build.rs` 从 `proto/echo.proto` 同时生成 Rust 类型和临时 Schema；仓库中的 `schema.pb` 是发布
侧车文件，测试会检查它与当前 `.proto` 生成结果完全一致，Schema 过期会直接导致测试失败。

## 推荐生命周期

```text
validate -> authorized -> queued -> running -> succeeded/failed/cancelled
                         \-> recovering（进程重启后）
```

每次状态转换都应使用稳定业务 run ID 持久化。`authorized` 表示本地策略允许远程请求，不等于底层命令已
开始；`recovering` 必须查询实际外部状态或以可证明幂等的方式恢复，不能盲目重新执行。

## 本地授权边界

Server Job 只能引用 Agent 预先配置的资源 ID。Agent 本地策略决定该资源允许的操作、目录范围、最大
并发、带宽、运行窗口和凭据。这样即使 Server 账户被误用，也不能通过 Proto 任意扩大到 Agent 文件系统
或执行任意命令。

对于更新、脚本等更高风险能力，建议独立 capability 和显式本地开关，并让默认状态为关闭。只读采集与
有副作用 Plus Task 不应共享同一宽泛权限。
