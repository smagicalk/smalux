//! WebSocket 握手 request 构建和 URL 工具。

use super::auth::WebSocketAuth;
use super::config::WebSocketConfig;
use anyhow::anyhow;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::{Uri, header::AUTHORIZATION};

/// Bearer 认证 header 值前缀。
const BEARER_AUTH_PREFIX: &str = "Bearer ";

/// 根据配置生成最终握手请求。
pub(crate) fn build_connect_request(config: &WebSocketConfig) -> anyhow::Result<Request> {
    let url = build_connect_url(config)?;
    let mut request = url.as_str().into_client_request()?;

    if let WebSocketAuth::BearerToken { token } = &config.auth {
        if token.is_empty() {
            anyhow::bail!("bearer token is empty");
        }
        request.headers_mut().insert(
            AUTHORIZATION,
            format!("{BEARER_AUTH_PREFIX}{token}").parse()?,
        );
    }

    Ok(request)
}

/// 合并基础 URL、普通 query 参数和 query token，生成最终连接 URL。
pub(crate) fn build_connect_url(config: &WebSocketConfig) -> anyhow::Result<String> {
    let uri: Uri = config.url.parse()?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| anyhow!("websocket url missing scheme"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| anyhow!("websocket url missing authority"))?;
    let path = if uri.path().is_empty() {
        "/"
    } else {
        uri.path()
    };
    let mut query_parts = Vec::new();

    if let Some(query) = uri.query().filter(|query| !query.is_empty()) {
        query_parts.push(query.to_string());
    }

    for (key, value) in &config.query {
        query_parts.push(encode_query_pair(key, value));
    }

    if let WebSocketAuth::QueryToken { param, token } = &config.auth {
        if param.is_empty() {
            anyhow::bail!("query token parameter name is empty");
        }
        if token.is_empty() {
            anyhow::bail!("query token is empty");
        }
        query_parts.push(encode_query_pair(param, token));
    }

    let query = query_parts.join("&");
    validate_sensitive_query_duplicates(&query)?;
    let path_and_query = if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{query}")
    };
    let uri = Uri::builder()
        .scheme(scheme)
        .authority(authority.as_str())
        .path_and_query(path_and_query)
        .build()?;

    Ok(uri.to_string())
}

/// 使用标准 form-url-encoded 规则编码 query 键值。
fn encode_query_pair(key: &str, value: &str) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    serializer.append_pair(key, value);
    serializer.finish()
}

/// 检查敏感 query 参数是否重复，避免 token 解析歧义。
fn validate_sensitive_query_duplicates(query: &str) -> anyhow::Result<()> {
    let mut seen_sensitive_keys = std::collections::HashSet::new();

    for (key, _value) in form_urlencoded::parse(query.as_bytes()) {
        let normalized = key.to_ascii_lowercase();
        if is_sensitive_query_key(&normalized) && !seen_sensitive_keys.insert(normalized.clone()) {
            anyhow::bail!("duplicate sensitive query parameter: {normalized}");
        }
    }

    Ok(())
}

/// 判断 query/header 名称是否属于敏感凭证字段。
fn is_sensitive_query_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "token" | "access_token" | "api_key" | "key" | "secret" | "signature" | "authorization"
    )
}

/// 脱敏 URL 中的敏感 query 参数，避免日志泄漏凭证。
pub(crate) fn redact_url(url: &str) -> String {
    let Some((base, query_and_fragment)) = url.split_once('?') else {
        return url.to_string();
    };
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, ""), |(query, fragment)| {
            (query, fragment)
        });
    let redacted_query = redact_query(query);

    if fragment.is_empty() {
        format!("{base}?{redacted_query}")
    } else {
        format!("{base}?{redacted_query}#{fragment}")
    }
}

/// 脱敏 query 字符串中的敏感参数值。
fn redact_query(query: &str) -> String {
    query
        .split('&')
        .map(|pair| {
            let Some((key, _value)) = pair.split_once('=') else {
                return pair.to_string();
            };
            if is_sensitive_query_key(key) {
                format!("{key}=<redacted>")
            } else {
                pair.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}
