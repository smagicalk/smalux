//! Agent 与 Server 共用的版本化 gRPC 交互定义。
//!
//! 除版本化 Protobuf 类型外，本 crate 还提供 Noise XXpsk3/IK 状态机、加密会话、
//! Tonic 双向流适配和密钥轮换状态方法。持久化、授权和业务处理仍由调用方负责。

pub mod noise;
pub mod tonic_transport;

/// Agent 交互协议。
pub mod agent {
    /// Agent 交互协议 v1；类型由 `proto/smalux/agent/v1/*.proto` 构建时生成。
    #[allow(non_camel_case_types)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/smalux.agent.v1.rs"));
    }
}
