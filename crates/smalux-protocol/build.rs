//! 根据版本化 Protobuf schema 生成 Agent/Server 共用的 gRPC 交互类型。

use std::path::{Path, PathBuf};

/// 递归收集目录中的 `.proto` 文件，并保持稳定排序避免无意义构建差异。
fn collect_proto_files(directory: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_proto_files(&path, files)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "proto")
        {
            files.push(path);
        }
    }
    Ok(())
}

/// 配置 vendored protoc，并在任意 v1 schema 变化时重新生成代码。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;
    let proto_root = std::path::PathBuf::from("proto");
    let mut grpc_config = prost_build::Config::new();
    grpc_config.protoc_executable(protoc);
    // 这些 oneof 分支显著大于同组其他分支；使用 Box 避免每个枚举值都占用最大分支栈空间。
    grpc_config.boxed(".smalux.agent.v1.TaskResult.result.system");
    grpc_config.boxed(".smalux.agent.v1.JobCommand.action.upsert");
    grpc_config.boxed(".smalux.agent.v1.SecureMessage.body.task_report");
    grpc_config.boxed(".smalux.agent.v1.SecureMessage.body.job_command_result");
    let proto_v1 = proto_root.join("smalux/agent/v1");
    let mut grpc_protos = Vec::new();
    collect_proto_files(&proto_v1, &mut grpc_protos)?;
    grpc_protos.sort();
    tonic_prost_build::configure()
        // 空 service 会让 Tonic 生成单分支 match；具体 RPC 未确定前仅抑制生成代码的 lint。
        .server_mod_attribute(
            "smalux.agent.v1",
            "#[allow(clippy::match_single_binding, clippy::mixed_attributes_style)]",
        )
        .compile_with_config(grpc_config, &grpc_protos, &[proto_root, protoc_include])?;

    // 逐个监听文件，避免 Windows/IDE 增量构建继续使用旧的 OUT_DIR 生成代码。
    // 目录监听保留用于捕获新增或删除的 proto 文件。
    println!("cargo:rerun-if-changed={}", proto_v1.display());
    for proto in &grpc_protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    Ok(())
}
