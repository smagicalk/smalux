//! 根据 Protobuf schema 生成核心模型和仅供示例使用的 gRPC Transport。

/// 配置 vendored protoc，并声明 schema 变化时重新生成代码。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;
    let proto_root = std::path::PathBuf::from("proto");

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc.clone());
    config.enable_type_names();
    config.compile_protos(
        &["proto/smalux/protocol/v1/core.proto"],
        &[proto_root.clone(), protoc_include.clone()],
    )?;

    // gRPC 仅是传输示例，生成文件只由 grpc_echo example 引用。
    let mut grpc_config = prost_build::Config::new();
    grpc_config.protoc_executable(protoc);
    let grpc_proto = std::path::PathBuf::from("proto/smalux/example/grpc/v1/transport.proto");
    tonic_prost_build::configure()
        .extern_path(".smalux.protocol.v1", "::smalux_protocol")
        .compile_with_config(grpc_config, &[grpc_proto], &[proto_root, protoc_include])?;

    println!("cargo:rerun-if-changed=proto/smalux/protocol/v1/core.proto");
    println!("cargo:rerun-if-changed=proto/smalux/example/grpc/v1/transport.proto");
    Ok(())
}
