//! Hello 校验、Server 选择规则和不可变协商结果。
//!
//! 协商结果会规范化为安全层必须认证的 transcript，避免版本、能力、安全方案和
//! Frame 上限在握手前被静默篡改。

use std::collections::HashSet;

use prost::Message;

use crate::{
    ClientHello, CodecLimits, Error, NegotiationTranscript, ProtocolVersion, Result,
    SecurityContext, SecurityRole, ServerHello, VersionRange,
};

/// 单次 Hello 最多声明的 capability 数量。
pub const MAX_CAPABILITIES: usize = 128;
/// capability 或安全方案名称的最大 UTF-8 字节数。
pub const MAX_CAPABILITY_NAME_BYTES: usize = 128;
/// Client 最多声明的安全方案数量。
pub const MAX_SECURITY_SCHEMES: usize = 32;
const NONCE_BYTES: usize = 32;
const SESSION_ID_BYTES: usize = 16;

/// Server 用于选择协商结果的本地策略。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiationPolicy {
    /// Server 支持的 Major/Minor 范围。
    pub supported_versions: Vec<VersionRange>,
    /// Server 支持的能力，顺序也是返回顺序。
    pub supported_capabilities: Vec<String>,
    /// Server 强制要求 Client 支持的能力。
    pub required_capabilities: Vec<String>,
    /// Server 允许的安全方案，按优先级从高到低排序。
    pub security_schemes: Vec<String>,
    /// Server 本地允许的最大单帧大小。
    pub codec_limits: CodecLimits,
}

/// Hello 完成后不可变的会话参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedParameters {
    version: ProtocolVersion,
    capabilities: Vec<String>,
    security_scheme: String,
    max_frame_bytes: usize,
    session_id: Vec<u8>,
    transcript: Vec<u8>,
}

impl NegotiatedParameters {
    /// 返回协商后的精确协议版本。
    pub const fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// 返回双方最终启用的能力集合。
    pub fn capabilities(&self) -> &[String] {
        &self.capabilities
    }

    /// 返回 Server 选择的安全方案名称。
    pub fn security_scheme(&self) -> &str {
        &self.security_scheme
    }

    /// 返回协商后的单帧上限。
    pub const fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// 返回本次连接的 16 字节会话标识。
    pub fn session_id(&self) -> &[u8] {
        &self.session_id
    }

    /// 返回必须由安全实现认证的规范化协商 transcript。
    pub fn transcript(&self) -> &[u8] {
        &self.transcript
    }

    /// 为具体安全实现生成只读的单连接上下文。
    pub fn security_context(&self, role: SecurityRole) -> SecurityContext {
        SecurityContext::new(
            role,
            self.security_scheme.clone(),
            self.version,
            self.capabilities.clone(),
            self.max_frame_bytes,
            self.session_id.clone(),
            self.transcript.clone(),
        )
    }
}

/// 校验 ClientHello，并按 Server 本地策略生成唯一协商结果。
pub fn negotiate_server_hello(
    client: &ClientHello,
    policy: &NegotiationPolicy,
    session_id: Vec<u8>,
    server_nonce: Vec<u8>,
) -> Result<ServerHello> {
    validate_client_hello(client)?;
    validate_policy(policy)?;
    validate_exact_bytes("server_hello.session_id", &session_id, SESSION_ID_BYTES)?;
    validate_exact_bytes("server_hello.nonce", &server_nonce, NONCE_BYTES)?;

    let selected_version =
        select_version(&client.supported_versions, &policy.supported_versions)
            .ok_or_else(|| Error::Negotiation("no common protocol version".to_owned()))?;
    let client_capabilities: HashSet<&str> = client
        .supported_capabilities
        .iter()
        .map(String::as_str)
        .collect();
    let server_capabilities: HashSet<&str> = policy
        .supported_capabilities
        .iter()
        .map(String::as_str)
        .collect();

    for required in &client.required_capabilities {
        if !server_capabilities.contains(required.as_str()) {
            return Err(Error::Negotiation(format!(
                "server does not support required client capability {required}"
            )));
        }
    }
    for required in &policy.required_capabilities {
        if !client_capabilities.contains(required.as_str()) {
            return Err(Error::Negotiation(format!(
                "client does not support required server capability {required}"
            )));
        }
    }

    let selected_capabilities = policy
        .supported_capabilities
        .iter()
        .filter(|name| client_capabilities.contains(name.as_str()))
        .cloned()
        .collect();
    let selected_security_scheme = policy
        .security_schemes
        .iter()
        .find(|scheme| client.supported_security_schemes.contains(scheme))
        .cloned()
        .ok_or_else(|| Error::Negotiation("no common security scheme".to_owned()))?;

    Ok(ServerHello {
        selected_version: Some(selected_version),
        selected_capabilities,
        selected_security_scheme,
        max_frame_bytes: client
            .max_frame_bytes
            .min(policy.codec_limits.max_frame_bytes() as u32),
        session_id,
        nonce: server_nonce,
    })
}

pub(crate) fn validate_client_hello(hello: &ClientHello) -> Result<()> {
    validate_version_ranges("client_hello.supported_versions", &hello.supported_versions)?;
    validate_names(
        "client_hello.supported_capabilities",
        &hello.supported_capabilities,
        MAX_CAPABILITIES,
    )?;
    validate_names(
        "client_hello.required_capabilities",
        &hello.required_capabilities,
        MAX_CAPABILITIES,
    )?;
    let supported: HashSet<&str> = hello
        .supported_capabilities
        .iter()
        .map(String::as_str)
        .collect();
    if let Some(missing) = hello
        .required_capabilities
        .iter()
        .find(|required| !supported.contains(required.as_str()))
    {
        return Err(Error::InvalidField {
            field: "client_hello.required_capabilities",
            detail: format!("required capability {missing} is not listed as supported"),
        });
    }
    validate_names(
        "client_hello.supported_security_schemes",
        &hello.supported_security_schemes,
        MAX_SECURITY_SCHEMES,
    )?;
    if hello.supported_security_schemes.is_empty() {
        return Err(Error::InvalidField {
            field: "client_hello.supported_security_schemes",
            detail: "at least one security scheme is required".to_owned(),
        });
    }
    validate_frame_bytes("client_hello.max_frame_bytes", hello.max_frame_bytes)?;
    validate_exact_bytes("client_hello.nonce", &hello.nonce, NONCE_BYTES)
}

pub(crate) fn validate_server_selection(
    client: &ClientHello,
    server: &ServerHello,
    local_limits: CodecLimits,
) -> Result<NegotiatedParameters> {
    let version = server.selected_version.ok_or(Error::InvalidField {
        field: "server_hello.selected_version",
        detail: "version is required".to_owned(),
    })?;
    if !client.supported_versions.iter().any(|range| {
        range.major == version.major
            && range.min_minor <= version.minor
            && version.minor <= range.max_minor
    }) {
        return Err(Error::Negotiation(
            "server selected an unsupported protocol version".to_owned(),
        ));
    }
    validate_names(
        "server_hello.selected_capabilities",
        &server.selected_capabilities,
        MAX_CAPABILITIES,
    )?;
    if server
        .selected_capabilities
        .iter()
        .any(|name| !client.supported_capabilities.contains(name))
    {
        return Err(Error::Negotiation(
            "server selected an unsupported capability".to_owned(),
        ));
    }
    if client
        .required_capabilities
        .iter()
        .any(|name| !server.selected_capabilities.contains(name))
    {
        return Err(Error::Negotiation(
            "server omitted a required client capability".to_owned(),
        ));
    }
    validate_name(
        "server_hello.selected_security_scheme",
        &server.selected_security_scheme,
    )?;
    if !client
        .supported_security_schemes
        .contains(&server.selected_security_scheme)
    {
        return Err(Error::Negotiation(
            "server selected an unsupported security scheme".to_owned(),
        ));
    }
    validate_frame_bytes("server_hello.max_frame_bytes", server.max_frame_bytes)?;
    let max_allowed = client
        .max_frame_bytes
        .min(local_limits.max_frame_bytes() as u32);
    if server.max_frame_bytes > max_allowed {
        return Err(Error::Negotiation(
            "server raised the client frame limit".to_owned(),
        ));
    }
    validate_exact_bytes(
        "server_hello.session_id",
        &server.session_id,
        SESSION_ID_BYTES,
    )?;
    validate_exact_bytes("server_hello.nonce", &server.nonce, NONCE_BYTES)?;

    let transcript = NegotiationTranscript {
        version: Some(version),
        capabilities: server.selected_capabilities.clone(),
        security_scheme: server.selected_security_scheme.clone(),
        max_frame_bytes: server.max_frame_bytes,
        session_id: server.session_id.clone(),
        client_nonce: client.nonce.clone(),
        server_nonce: server.nonce.clone(),
    }
    .encode_to_vec();

    Ok(NegotiatedParameters {
        version,
        capabilities: server.selected_capabilities.clone(),
        security_scheme: server.selected_security_scheme.clone(),
        max_frame_bytes: server.max_frame_bytes as usize,
        session_id: server.session_id.clone(),
        transcript,
    })
}

fn validate_policy(policy: &NegotiationPolicy) -> Result<()> {
    validate_version_ranges("policy.supported_versions", &policy.supported_versions)?;
    validate_names(
        "policy.supported_capabilities",
        &policy.supported_capabilities,
        MAX_CAPABILITIES,
    )?;
    validate_names(
        "policy.required_capabilities",
        &policy.required_capabilities,
        MAX_CAPABILITIES,
    )?;
    let supported: HashSet<&str> = policy
        .supported_capabilities
        .iter()
        .map(String::as_str)
        .collect();
    if let Some(missing) = policy
        .required_capabilities
        .iter()
        .find(|required| !supported.contains(required.as_str()))
    {
        return Err(Error::InvalidField {
            field: "policy.required_capabilities",
            detail: format!("required capability {missing} is not listed as supported"),
        });
    }
    validate_names(
        "policy.security_schemes",
        &policy.security_schemes,
        MAX_SECURITY_SCHEMES,
    )?;
    if policy.security_schemes.is_empty() {
        return Err(Error::InvalidField {
            field: "policy.security_schemes",
            detail: "at least one security scheme is required".to_owned(),
        });
    }
    Ok(())
}

fn select_version(client: &[VersionRange], server: &[VersionRange]) -> Option<ProtocolVersion> {
    client
        .iter()
        .flat_map(|client_range| {
            server.iter().filter_map(move |server_range| {
                if client_range.major != server_range.major {
                    return None;
                }
                let min_minor = client_range.min_minor.max(server_range.min_minor);
                let max_minor = client_range.max_minor.min(server_range.max_minor);
                (min_minor <= max_minor).then_some(ProtocolVersion {
                    major: client_range.major,
                    minor: max_minor,
                })
            })
        })
        .max_by_key(|version| (version.major, version.minor))
}

fn validate_version_ranges(field: &'static str, ranges: &[VersionRange]) -> Result<()> {
    if ranges.is_empty() {
        return Err(Error::InvalidField {
            field,
            detail: "at least one version range is required".to_owned(),
        });
    }
    let mut majors = HashSet::new();
    for range in ranges {
        if range.major == 0 || range.min_minor > range.max_minor || !majors.insert(range.major) {
            return Err(Error::InvalidField {
                field,
                detail: "major must be non-zero, unique, and minor range must be ordered"
                    .to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_names(field: &'static str, names: &[String], max_count: usize) -> Result<()> {
    if names.len() > max_count {
        return Err(Error::InvalidField {
            field,
            detail: format!("count must not exceed {max_count}"),
        });
    }
    let mut unique = HashSet::new();
    for name in names {
        validate_name(field, name)?;
        if !unique.insert(name) {
            return Err(Error::InvalidField {
                field,
                detail: format!("duplicate name {name}"),
            });
        }
    }
    Ok(())
}

fn validate_name(field: &'static str, name: &str) -> Result<()> {
    let mut bytes = name.bytes();
    let first = bytes.next();
    let valid = first.is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'/' | b'-')
        });
    if !valid || name.len() > MAX_CAPABILITY_NAME_BYTES {
        return Err(Error::InvalidField {
            field,
            detail: format!("invalid namespaced name {name}"),
        });
    }
    Ok(())
}

fn validate_frame_bytes(field: &'static str, value: u32) -> Result<()> {
    CodecLimits::new(value as usize)
        .map(|_| ())
        .map_err(|error| Error::InvalidField {
            field,
            detail: error.to_string(),
        })
}

pub(crate) fn validate_exact_bytes(
    field: &'static str,
    value: &[u8],
    expected: usize,
) -> Result<()> {
    if value.len() != expected {
        return Err(Error::InvalidField {
            field,
            detail: format!("expected {expected} bytes, received {}", value.len()),
        });
    }
    Ok(())
}
