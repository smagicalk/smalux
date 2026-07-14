//! Noise 安全实现入口。
//!
//! Provider 负责长期静态身份、模式配置和远端公钥校验；Session 负责单连接握手、
//! 受保护 record、分片、rekey 与失败关闭。调用方只通过本模块导出的公共类型使用实现。

mod provider;
mod record;
mod session;

pub use provider::{
    AllowUnknownRemoteKey, CallbackRemoteKeyVerifier, NoiseKeypair, NoiseProvider,
    NoiseProviderBuilder, NoiseRemoteKeyVerifier, PinnedRemoteKey,
};
pub use record::NOISE_RECORD_PLAINTEXT_BYTES;
pub use session::{NOISE_IK_SCHEME, NOISE_REKEY_INTERVAL_RECORDS, NOISE_XX_SCHEME};
