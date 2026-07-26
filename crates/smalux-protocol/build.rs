//! 根据版本化 Protobuf schema 生成 Agent/Server 共用的 gRPC 交互类型。

/// 配置 vendored protoc，并在交互 schema 变化时重新生成代码。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;
    let proto_root = std::path::PathBuf::from("proto");
    let mut grpc_config = prost_build::Config::new();
    grpc_config.protoc_executable(protoc);
    let grpc_protos = [
        std::path::PathBuf::from("proto/smalux/agent/v1/errors.proto"),
        std::path::PathBuf::from("proto/smalux/agent/v1/messages.proto"),
        std::path::PathBuf::from("proto/smalux/agent/v1/transport.proto"),
    ];
    tonic_prost_build::configure()
        // 空 service 会让 Tonic 生成单分支 match；具体 RPC 未确定前仅抑制生成代码的 lint。
        .server_mod_attribute(
            "smalux.agent.v1",
            "#[allow(clippy::match_single_binding, clippy::mixed_attributes_style)]",
        )
        .compile_with_config(grpc_config, &grpc_protos, &[proto_root, protoc_include])?;

    println!("cargo:rerun-if-changed=proto/smalux/agent/v1/transport.proto");
    println!("cargo:rerun-if-changed=proto/smalux/agent/v1/messages.proto");
    println!("cargo:rerun-if-changed=proto/smalux/agent/v1/errors.proto");
    Ok(())
}
