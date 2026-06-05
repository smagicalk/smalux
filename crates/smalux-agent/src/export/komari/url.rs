//! Komari URL 构造和 query 规则。
//!
//! URL 规则集中放在这里，后续 report、basic info、terminal 都复用同一套 token
//! 合并和重复敏感 query 校验逻辑。

use crate::config::model::{ExportAuthMode, ExportConfig};
use crate::export::{ExportEndpointScheme, build_export_endpoint};
use std::collections::BTreeSet;

/// Komari report endpoint 的完整默认路径。
const KOMARI_REPORT_PATH: &str = "/api/clients/report";
/// Komari basic info endpoint 的完整默认路径。
const KOMARI_BASIC_INFO_PATH: &str = "/api/clients/uploadBasicInfo";
/// Komari terminal endpoint 的完整默认路径。
const KOMARI_TERMINAL_PATH: &str = "/api/clients/terminal";
/// Komari task result endpoint 的完整默认路径。
const KOMARI_TASK_RESULT_PATH: &str = "/api/clients/task/result";

/// 构造 Komari WebSocket 实时 report URL，不主动追加认证 query。
pub(super) fn komari_report_websocket_url(config: &ExportConfig) -> anyhow::Result<String> {
    build_export_endpoint(
        &config.base_url,
        ExportEndpointScheme::WebSocket,
        KOMARI_REPORT_PATH,
    )
}

/// 构造 Komari basic info URL。
pub(super) fn komari_basic_info_url(config: &ExportConfig) -> anyhow::Result<String> {
    let mut url = reqwest::Url::parse(&build_export_endpoint(
        &config.base_url,
        ExportEndpointScheme::Http,
        KOMARI_BASIC_INFO_PATH,
    )?)?;
    append_export_query(&mut url, config)?;
    Ok(url.to_string())
}

/// 构造 Komari task result HTTP URL。
pub(super) fn komari_task_result_url(config: &ExportConfig) -> anyhow::Result<String> {
    let mut url = reqwest::Url::parse(&build_export_endpoint(
        &config.base_url,
        ExportEndpointScheme::Http,
        KOMARI_TASK_RESULT_PATH,
    )?)?;
    append_export_query(&mut url, config)?;
    Ok(url.to_string())
}

/// 构造 Komari terminal WebSocket URL。
pub(super) fn komari_terminal_url(
    config: &ExportConfig,
    request_id: &str,
) -> anyhow::Result<String> {
    if request_id.trim().is_empty() {
        anyhow::bail!("komari terminal request_id cannot be empty");
    }

    let mut url = reqwest::Url::parse(&build_export_endpoint(
        &config.base_url,
        ExportEndpointScheme::WebSocket,
        KOMARI_TERMINAL_PATH,
    )?)?;
    url.query_pairs_mut().append_pair("id", request_id);
    append_export_query(&mut url, config)?;
    Ok(url.to_string())
}

/// 脱敏 Komari URL 中的敏感 query 参数，便于日志排查实际 endpoint。
pub(super) fn redact_komari_url(url: &str) -> String {
    let Some((base, query_and_fragment)) = url.split_once('?') else {
        return url.to_string();
    };
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, ""), |(query, fragment)| {
            (query, fragment)
        });
    let redacted_query = query
        .split('&')
        .map(|pair| {
            let Some((key, _value)) = pair.split_once('=') else {
                return pair.to_string();
            };
            if is_sensitive_query_key(&key.to_ascii_lowercase()) {
                format!("{key}=<redacted>")
            } else {
                pair.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("&");

    if fragment.is_empty() {
        format!("{base}?{redacted_query}")
    } else {
        format!("{base}?{redacted_query}#{fragment}")
    }
}

/// 合并通用 query 和 query token。
fn append_export_query(url: &mut reqwest::Url, config: &ExportConfig) -> anyhow::Result<()> {
    for (key, value) in &config.query {
        url.query_pairs_mut().append_pair(key, value);
    }

    match config.auth_mode {
        ExportAuthMode::None => {}
        ExportAuthMode::Query => {
            let token = config
                .token
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("export.token is required for komari query auth"))?;
            url.query_pairs_mut()
                .append_pair(&config.query_token_param, token);
        }
        ExportAuthMode::Bearer => {
            anyhow::bail!("komari export does not support bearer auth")
        }
    }

    validate_sensitive_query_duplicates(url)?;
    Ok(())
}

/// 检查敏感 query 参数是否重复，避免 token 解析歧义。
fn validate_sensitive_query_duplicates(url: &reqwest::Url) -> anyhow::Result<()> {
    let mut seen = BTreeSet::new();
    for (key, _value) in url.query_pairs() {
        let normalized = key.to_ascii_lowercase();
        if is_sensitive_query_key(&normalized) && !seen.insert(normalized.clone()) {
            anyhow::bail!("duplicate sensitive query parameter: {normalized}");
        }
    }

    Ok(())
}

/// 判断 query 名称是否属于敏感凭证字段。
fn is_sensitive_query_key(key: &str) -> bool {
    matches!(
        key,
        "token" | "access_token" | "api_key" | "apikey" | "key" | "secret" | "signature"
    ) || key.ends_with("_token")
        || key.ends_with("_key")
}

#[cfg(test)]
mod tests {
    //! Komari URL 构造测试。

    use super::*;

    /// 构造 Komari 测试配置。
    fn komari_config(base_url: &str) -> ExportConfig {
        ExportConfig {
            base_url: base_url.to_string(),
            auth_mode: ExportAuthMode::Query,
            token: Some("secret-token".to_string()),
            ..ExportConfig::default()
        }
    }

    /// 验证 base_url 根地址可以推导 WebSocket report URL。
    #[test]
    fn komari_websocket_report_url_is_derived_from_base_url() {
        let url = komari_report_websocket_url(&komari_config("https://example.com")).unwrap();

        assert_eq!(url, "wss://example.com/api/clients/report");
    }

    /// 验证 basic info URL 可以从 base_url 根地址推导。
    #[test]
    fn komari_basic_info_url_is_derived_from_base_url() {
        let url = komari_basic_info_url(&komari_config("https://example.com")).unwrap();

        assert_eq!(
            url,
            "https://example.com/api/clients/uploadBasicInfo?token=secret-token"
        );
    }

    /// 验证 task result URL 可以从 base_url 根地址推导。
    #[test]
    fn komari_task_result_url_is_derived_from_base_url() {
        let url = komari_task_result_url(&komari_config("https://example.com")).unwrap();

        assert_eq!(
            url,
            "https://example.com/api/clients/task/result?token=secret-token"
        );
    }

    /// 验证 terminal URL 会从 base_url 根地址推导并带上 request id。
    #[test]
    fn komari_terminal_url_is_derived_from_base_url() {
        let url = komari_terminal_url(&komari_config("https://example.com"), "term-1").unwrap();

        assert_eq!(
            url,
            "wss://example.com/api/clients/terminal?id=term-1&token=secret-token"
        );
    }

    /// 验证 base URL 不能包含 path。
    #[test]
    fn komari_base_url_rejects_path() {
        let error =
            komari_report_websocket_url(&komari_config("https://example.com/api/clients/report"))
                .unwrap_err();

        assert!(error.to_string().contains("path"));
    }

    /// 验证 URL 日志会脱敏 token。
    #[test]
    fn redact_komari_url_masks_sensitive_query_values() {
        let redacted = redact_komari_url(
            "https://example.com/api/clients/uploadBasicInfo?token=secret&region=local",
        );

        assert_eq!(
            redacted,
            "https://example.com/api/clients/uploadBasicInfo?token=<redacted>&region=local"
        );
    }

    /// 验证重复敏感 query 会快速失败，避免 token 歧义。
    #[test]
    fn komari_url_rejects_duplicate_sensitive_query() {
        let mut config = komari_config("https://example.com");
        config
            .query
            .insert("token".to_string(), "from-query".to_string());

        let error = komari_basic_info_url(&config).unwrap_err();

        assert!(error.to_string().contains("duplicate sensitive query"));
    }
}
