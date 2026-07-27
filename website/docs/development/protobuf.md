---
title: Protobuf 开发
description: Proto 文件组织、代码生成和向后兼容规则。
---

# Protobuf 开发

## 文件组织

```text
proto/smalux/agent/v1/
├── transport.proto        # gRPC service 与外层 Frame
├── messages.proto         # 安全业务 envelope、注册和控制消息
├── errors.proto           # 稳定错误枚举
├── job/
│   ├── definition.proto
│   ├── schedule.proto
│   ├── command.proto
│   ├── status.proto
│   └── event.proto
└── task/
    ├── common.proto
    ├── selection.proto
    └── 按领域拆分的配置与结果
```

所有文件使用同一个 package：

```proto
package smalux.agent.v1;
```

Rust 中统一通过 `smalux_protocol::agent::v1` 引用，业务 crate 不依赖生成文件的物理路径。

## 构建流程

`smalux-protocol/build.rs`：

1. 使用 `protoc-bin-vendored` 获取当前平台 `protoc`；
2. 收集 `proto/smalux/agent/v1/` 下需要编译的文件；
3. 通过 `tonic-prost-build` 生成消息和 gRPC Client/Server；
4. 把结果写入 Cargo `OUT_DIR`；
5. `src/lib.rs` 使用 `tonic::include_proto!` 暴露模块。

修改 Proto 后运行：

```powershell
cargo check -p smalux-protocol
cargo test -p smalux-protocol --all-targets
```

## 兼容规则

已发布字段必须遵守：

- 不更改已有字段编号；
- 不把旧编号分配给不同含义；
- 不改变字段类型到 wire 不兼容类型；
- 删除字段后使用 `reserved` 保留编号和字段名；
- 枚举零值只表示 unspecified，业务层显式拒绝或使用文档定义的默认策略；
- 新增能力优先增加新字段或 `oneof` 分支，旧端会把未知字段保留/忽略；
- 稳定错误恢复依据枚举 code，不匹配人类可读 message。

## 配置和结果成对

每个固定 Task 都应同时定义配置和结果：

```proto
message ExampleTaskConfig {
  string resource_id = 1;
}

message ExampleResult {
  bool succeeded = 1;
}
```

然后分别加入 `TaskDefinition.oneof task` 和 `TaskResult.oneof result`。字段号在各自 oneof 中保持稳定，
并为 round-trip、缺失配置和 Agent 不支持能力补测试。

## 不应放进 Proto 的内容

- Rust 类型名、动态库路径或任意可执行代码；
- shell 命令和未经约束的脚本；
- 仓库密码、Token、私钥等秘密；
- Scheduler 已统一提供的触发、超时、重试和队列字段；
- 只能由本地环境决定的绝对路径或权限策略。

Proto 配置应使用稳定业务 ID 引用本地资源，Agent 再根据本地授权和安全存储解析。
