//! HTTP 导出 transport。
//!
//! 当前用于 Komari basic info；后续也可以承载其他短请求导出。

use serde::Serialize;
use std::time::Duration;

/// HTTP 请求超时时间。
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// HTTP 响应体日志最大字符数，避免第三方服务返回异常大内容刷爆日志。
const HTTP_RESPONSE_LOG_BODY_LIMIT: usize = 2048;

/// HTTP transport 配置。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct HttpConfig {
    /// 是否跳过 TLS 证书校验。
    pub(crate) unsafe_cert: bool,
    /// 单次 HTTP 请求超时时间。
    request_timeout: Duration,
}

impl Default for HttpConfig {
    /// 默认使用正常 TLS 校验和固定请求超时。
    fn default() -> Self {
        Self {
            unsafe_cert: false,
            request_timeout: HTTP_REQUEST_TIMEOUT,
        }
    }
}

impl HttpConfig {
    /// 设置是否跳过 TLS 证书校验。
    pub(crate) fn with_unsafe_cert(mut self, unsafe_cert: bool) -> Self {
        self.unsafe_cert = unsafe_cert;
        self
    }
}

/// HTTP method。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum HttpMethod {
    /// POST 请求。
    Post,
}

impl HttpMethod {
    /// 转换为 reqwest method。
    fn as_reqwest(self) -> reqwest::Method {
        match self {
            Self::Post => reqwest::Method::POST,
        }
    }

    /// 转换为日志里的方法名。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Post => "POST",
        }
    }
}

/// HTTP transport client。
#[derive(Debug, Clone)]
pub(crate) struct HttpClient {
    /// reqwest client 会复用连接池。
    client: reqwest::Client,
}

impl HttpClient {
    /// 创建 HTTP client。
    pub(crate) fn new(config: HttpConfig) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(config.request_timeout);
        if config.unsafe_cert {
            // unsafe_cert 只用于测试或自签名环境，会跳过服务端证书校验。
            tracing::warn!(
                "unsafe_cert enabled; HTTP TLS server certificate verification is disabled"
            );
            builder = builder.danger_accept_invalid_certs(true);
        }

        Ok(Self {
            client: builder.build()?,
        })
    }

    /// 发送 JSON 请求，并要求返回 2xx。
    pub(crate) async fn send_json<T>(
        &self,
        method: HttpMethod,
        url: &str,
        body: &T,
    ) -> anyhow::Result<()>
    where
        T: Serialize + ?Sized,
    {
        let response = self
            .client
            .request(method.as_reqwest(), url)
            .json(body)
            .send()
            .await?;
        let status = response.status();
        let text = response
            .text()
            .await
            .unwrap_or_else(|error| format!("<failed to read response body: {error}>"));
        let log_body = format_http_response_body_for_log(&text);

        if !status.is_success() {
            tracing::warn!(
                method = method.as_str(),
                url = %redact_http_url(url),
                status = %status,
                body = %log_body,
                "http export response failed"
            );
            anyhow::bail!("http export request failed: status={status}, body={text}");
        }

        tracing::info!(
            method = method.as_str(),
            url = %redact_http_url(url),
            status = %status,
            body = %log_body,
            "http export response received"
        );

        Ok(())
    }
}

/// 脱敏 HTTP URL，避免 token 等认证信息进入日志。
fn redact_http_url(raw_url: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(raw_url) else {
        return "<invalid-url>".to_string();
    };

    if !url.username().is_empty() {
        let _ = url.set_username("<redacted>");
    }
    if url.password().is_some() {
        let _ = url.set_password(Some("<redacted>"));
    }

    let query_pairs = url
        .query_pairs()
        .map(|(key, value)| {
            let value = if is_sensitive_query_key(&key) {
                "<redacted>".to_string()
            } else {
                value.into_owned()
            };
            (key.into_owned(), value)
        })
        .collect::<Vec<_>>();

    if !query_pairs.is_empty() {
        let mut serializer = url.query_pairs_mut();
        serializer.clear();
        for (key, value) in query_pairs {
            serializer.append_pair(&key, &value);
        }
    }

    url.to_string()
}

/// 判断 query key 是否是敏感字段。
fn is_sensitive_query_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "token"
            | "access_token"
            | "auth"
            | "authorization"
            | "key"
            | "api_key"
            | "password"
            | "secret"
    )
}

/// 格式化 HTTP 响应体日志，超长时截断。
fn format_http_response_body_for_log(body: &str) -> String {
    let mut chars = body.chars();
    let preview = chars
        .by_ref()
        .take(HTTP_RESPONSE_LOG_BODY_LIMIT)
        .collect::<String>();

    if chars.next().is_some() {
        format!("{preview}...<truncated>")
    } else {
        preview
    }
}

#[cfg(test)]
mod tests {
    //! HTTP transport 辅助逻辑测试。

    use super::{HTTP_RESPONSE_LOG_BODY_LIMIT, format_http_response_body_for_log, redact_http_url};

    /// 验证 HTTP URL 日志会脱敏认证 query。
    #[test]
    fn redact_http_url_masks_sensitive_query_values() {
        let redacted = redact_http_url(
            "https://user:pass@example.com/api/clients/report?token=secret&name=node",
        );

        assert_eq!(
            redacted,
            "https://%3Credacted%3E:%3Credacted%3E@example.com/api/clients/report?token=%3Credacted%3E&name=node"
        );
    }

    /// 验证 HTTP 响应体日志会截断超长内容。
    #[test]
    fn format_http_response_body_for_log_truncates_long_body() {
        let body = "a".repeat(HTTP_RESPONSE_LOG_BODY_LIMIT + 1);
        let formatted = format_http_response_body_for_log(&body);

        assert!(formatted.ends_with("...<truncated>"));
        assert!(formatted.len() > HTTP_RESPONSE_LOG_BODY_LIMIT);
    }
}
