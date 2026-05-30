//! rustls 证书校验适配。
//!
//! `UnsafeNoCertificateVerification` 仅用于连接自签名或测试环境服务端，生产环境应优先使用默认校验。

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ConfigBuilder, DigitallySignedStruct, Error, SignatureScheme};
use std::fmt::{Debug, Formatter};

/// 创建带显式 crypto provider 的 ClientConfig builder。
///
/// workspace 级 feature 合并可能同时启用 rustls 的多个 provider，不能依赖自动推断。
pub(crate) fn client_config_builder()
-> Result<ConfigBuilder<ClientConfig, rustls::WantsVerifier>, Error> {
    ClientConfig::builder_with_provider(rustls::crypto::aws_lc_rs::default_provider().into())
        .with_safe_default_protocol_versions()
}

/// 跳过服务端证书校验的 verifier。
///
/// 这个类型会无条件接受服务端证书和握手签名，只能在明确允许不安全 TLS 的场景使用。
pub(crate) struct UnsafeNoCertificateVerification {
    /// 当前 rustls crypto provider 支持的签名算法。
    supported_schemes: Vec<SignatureScheme>,
}

impl UnsafeNoCertificateVerification {
    /// 根据 ClientConfig builder 当前 provider 创建不安全 verifier。
    pub(crate) fn from_client_config_builder(
        builder: &rustls::ConfigBuilder<ClientConfig, rustls::WantsVerifier>,
    ) -> Self {
        Self {
            supported_schemes: builder
                .crypto_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

impl Debug for UnsafeNoCertificateVerification {
    /// 避免 Debug 输出暴露无意义的内部结构。
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnsafeNoCertificateVerification")
            .field("supported_scheme_count", &self.supported_schemes.len())
            .finish()
    }
}

impl rustls::client::danger::ServerCertVerifier for UnsafeNoCertificateVerification {
    /// 直接接受服务端证书。
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    /// 直接接受 TLS 1.2 握手签名。
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    /// 直接接受 TLS 1.3 握手签名。
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    /// 返回 rustls 支持验证流程需要枚举的签名算法。
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_schemes.clone()
    }
}

#[cfg(test)]
mod tests {
    //! 不安全 rustls verifier 的最小行为测试。

    use super::{UnsafeNoCertificateVerification, client_config_builder};
    use rustls::client::danger::ServerCertVerifier;

    /// verifier 应复用当前 crypto provider 的签名算法列表。
    #[test]
    fn unsafe_verifier_uses_provider_supported_schemes() {
        let builder = client_config_builder().unwrap();
        let expected = builder
            .crypto_provider()
            .signature_verification_algorithms
            .supported_schemes();
        let verifier = UnsafeNoCertificateVerification::from_client_config_builder(&builder);

        assert!(!verifier.supported_verify_schemes().is_empty());
        assert_eq!(verifier.supported_verify_schemes(), expected);
    }

    /// Debug 输出只暴露数量，不打印算法列表细节。
    #[test]
    fn debug_output_only_contains_scheme_count() {
        let builder = client_config_builder().unwrap();
        let verifier = UnsafeNoCertificateVerification::from_client_config_builder(&builder);
        let debug = format!("{verifier:?}");

        assert!(debug.contains("supported_scheme_count"));
        assert!(!debug.contains("ECDSA_NISTP256_SHA256"));
    }
}
