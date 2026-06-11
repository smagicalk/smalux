//! 通用工具模块。
//!
//! 当前为空，后续仅放多个 crate 共享且不属于具体领域的工具函数。

/// 日志和调试输出脱敏工具。
pub mod redact {
    use serde_json::Value;

    /// 脱敏后文本。
    #[derive(Debug, Clone, Eq, PartialEq)]
    pub struct RedactedText {
        /// 脱敏后的文本内容。
        pub value: String,
        /// 是否发生过脱敏。
        pub redacted: bool,
    }

    /// 脱敏后 JSON。
    #[derive(Debug, Clone, PartialEq)]
    pub struct RedactedJson {
        /// 脱敏后的 JSON 值。
        pub value: Value,
        /// 是否发生过脱敏。
        pub redacted: bool,
    }

    /// 递归脱敏 JSON 值，返回是否发生过脱敏。
    pub fn redact_sensitive_json_value(value: &mut Value) -> bool {
        match value {
            Value::Object(map) => {
                let shell_stream_data = map
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| matches!(kind, "input" | "output"))
                    && map.contains_key("data");
                let komari_task_result = map.contains_key("task_id") && map.contains_key("result");
                let mut changed = false;

                for (key, value) in map.iter_mut() {
                    if is_sensitive_key(key)
                        || (shell_stream_data && key == "data")
                        || (komari_task_result && key == "result")
                    {
                        *value = Value::String("<redacted>".to_string());
                        changed = true;
                    } else {
                        changed |= redact_sensitive_json_value(value);
                    }
                }

                changed
            }
            Value::Array(values) => {
                let mut changed = false;
                for value in values {
                    changed |= redact_sensitive_json_value(value);
                }
                changed
            }
            _ => false,
        }
    }

    /// 克隆并脱敏 JSON 值，不修改调用者持有的原值。
    pub fn redact_sensitive_json(value: &Value) -> RedactedJson {
        let mut redacted = value.clone();
        let changed = redact_sensitive_json_value(&mut redacted);
        RedactedJson {
            value: redacted,
            redacted: changed,
        }
    }

    /// 尝试把文本当 JSON 脱敏；不是 JSON 时返回 None。
    pub fn redact_sensitive_json_text(text: &str) -> Option<RedactedText> {
        let value = serde_json::from_str::<Value>(text).ok()?;
        let redacted = redact_sensitive_json(&value);
        let value = serde_json::to_string(&redacted.value).ok()?;

        Some(RedactedText {
            value,
            redacted: redacted.redacted,
        })
    }

    /// 尝试把 UTF-8 字节当 JSON 脱敏；不是 UTF-8 或 JSON 时返回 None。
    pub fn redact_sensitive_json_bytes(bytes: &[u8]) -> Option<RedactedText> {
        let text = std::str::from_utf8(bytes).ok()?;
        redact_sensitive_json_text(text)
    }

    /// 对非 JSON 文本做轻量关键词脱敏。
    pub fn redact_sensitive_text(text: &str) -> RedactedText {
        let mut output = String::with_capacity(text.len());
        let mut index = 0;
        let mut changed = false;

        while index < text.len() {
            if let Some(match_info) = find_sensitive_assignment(text, index) {
                output.push_str(&text[index..match_info.value_start]);
                output.push_str("<redacted>");
                index = match_info.value_end;
                changed = true;
                continue;
            }

            let Some(ch) = text[index..].chars().next() else {
                break;
            };
            output.push(ch);
            index += ch.len_utf8();
        }

        RedactedText {
            value: output,
            redacted: changed,
        }
    }

    /// 判断字段名是否是敏感字段。
    pub fn is_sensitive_key(key: &str) -> bool {
        let normalized = normalize_sensitive_key(key);
        matches!(
            normalized.as_str(),
            "token"
                | "accesstoken"
                | "refreshtoken"
                | "idtoken"
                | "authorization"
                | "auth"
                | "password"
                | "passwd"
                | "secret"
                | "clientsecret"
                | "apikey"
                | "privatekey"
                | "psk"
                | "command"
                | "stdout"
                | "stderr"
        )
    }

    /// 归一化敏感字段名，兼容 snake_case、kebab-case、点号和大小写差异。
    pub fn normalize_sensitive_key(key: &str) -> String {
        key.chars()
            .filter(|ch| !matches!(ch, '_' | '-' | '.'))
            .flat_map(char::to_lowercase)
            .collect()
    }

    /// 文本中匹配到的敏感 key-value 片段。
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    struct SensitiveAssignment {
        /// value 起始位置。
        value_start: usize,
        /// value 结束位置。
        value_end: usize,
    }

    /// 查找当前位置是否是敏感 key-value 片段。
    fn find_sensitive_assignment(text: &str, start: usize) -> Option<SensitiveAssignment> {
        if !is_text_key_boundary_before(text, start) {
            return None;
        }

        for key in SENSITIVE_TEXT_KEYS {
            let key_end = start + key.len();
            if key_end > text.len() || !text[start..key_end].eq_ignore_ascii_case(key) {
                continue;
            }

            let mut cursor = key_end;
            while cursor < text.len() {
                let ch = text[cursor..].chars().next()?;
                if !ch.is_ascii_whitespace() {
                    break;
                }
                cursor += ch.len_utf8();
            }

            let separator = text[cursor..].chars().next()?;
            if !matches!(separator, '=' | ':') {
                continue;
            }
            cursor += separator.len_utf8();

            while cursor < text.len() {
                let ch = text[cursor..].chars().next()?;
                if !ch.is_ascii_whitespace() {
                    break;
                }
                cursor += ch.len_utf8();
            }

            let value_end = find_text_value_end(text, cursor, separator);
            return Some(SensitiveAssignment {
                value_start: cursor,
                value_end,
            });
        }

        None
    }

    /// 文本脱敏使用的敏感 key，保持小写 ASCII，方便不分大小写匹配。
    const SENSITIVE_TEXT_KEYS: &[&str] = &[
        "token",
        "access_token",
        "refresh_token",
        "id_token",
        "authorization",
        "auth",
        "password",
        "passwd",
        "secret",
        "client_secret",
        "api_key",
        "private_key",
        "psk",
        "command",
        "stdout",
        "stderr",
    ];

    /// 判断当前位置前面是否是字段边界，避免匹配到普通单词中间。
    fn is_text_key_boundary_before(text: &str, start: usize) -> bool {
        if start == 0 {
            return true;
        }

        text[..start]
            .chars()
            .next_back()
            .is_none_or(|ch| !matches!(ch, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '-'))
    }

    /// 查找文本 key-value 的 value 结束位置。
    fn find_text_value_end(text: &str, start: usize, separator: char) -> usize {
        let mut cursor = start;
        let quote = text[start..]
            .chars()
            .next()
            .filter(|ch| matches!(ch, '"' | '\''));

        if let Some(quote) = quote {
            cursor += quote.len_utf8();
            let mut escaped = false;
            while cursor < text.len() {
                let Some(ch) = text[cursor..].chars().next() else {
                    break;
                };
                cursor += ch.len_utf8();
                if escaped {
                    escaped = false;
                    continue;
                }
                if ch == '\\' {
                    escaped = true;
                    continue;
                }
                if ch == quote {
                    break;
                }
            }
            return cursor;
        }

        while cursor < text.len() {
            let Some(ch) = text[cursor..].chars().next() else {
                break;
            };
            if matches!(ch, '\r' | '\n') || (separator == '=' && matches!(ch, '&' | ';' | ',')) {
                break;
            }
            if separator == '=' && ch.is_ascii_whitespace() {
                break;
            }
            cursor += ch.len_utf8();
        }

        cursor
    }

    #[cfg(test)]
    mod tests {
        //! 日志脱敏工具测试。

        use super::*;

        /// 验证敏感字段名会被归一化识别。
        #[test]
        fn is_sensitive_key_accepts_common_variants() {
            assert!(is_sensitive_key("access_token"));
            assert!(is_sensitive_key("API-Key"));
            assert!(is_sensitive_key("private.key"));
            assert!(!is_sensitive_key("agent_id"));
        }

        /// 验证 JSON 脱敏会递归处理敏感字段。
        #[test]
        fn redact_sensitive_json_redacts_nested_fields() {
            let body = serde_json::json!({
                "token": "secret-token",
                "nested": {
                    "api_key": "secret-key",
                    "normal": "visible"
                },
                "stdout": "command output"
            });

            let redacted = redact_sensitive_json(&body);

            assert!(redacted.redacted);
            assert_eq!(redacted.value["token"], "<redacted>");
            assert_eq!(redacted.value["nested"]["api_key"], "<redacted>");
            assert_eq!(redacted.value["nested"]["normal"], "visible");
            assert_eq!(redacted.value["stdout"], "<redacted>");
        }

        /// 验证 JSON 数组中所有元素都会被处理，而不是遇到第一个命中就短路。
        #[test]
        fn redact_sensitive_json_redacts_all_array_items() {
            let body = serde_json::json!([
                { "token": "secret-1" },
                { "password": "secret-2" }
            ]);

            let redacted = redact_sensitive_json(&body);

            assert_eq!(redacted.value[0]["token"], "<redacted>");
            assert_eq!(redacted.value[1]["password"], "<redacted>");
        }

        /// 验证 shell stream 的 data 字段会按上下文脱敏。
        #[test]
        fn redact_sensitive_json_redacts_shell_stream_data() {
            let body = serde_json::json!({
                "type": "output",
                "session_id": "shell-1",
                "data": "base64-output"
            });

            let redacted = redact_sensitive_json(&body);

            assert!(redacted.redacted);
            assert_eq!(redacted.value["data"], "<redacted>");
        }

        /// 验证 Komari task result 会按上下文脱敏。
        #[test]
        fn redact_sensitive_json_redacts_komari_task_result() {
            let body = serde_json::json!({
                "task_id": "task-1",
                "result": "command output"
            });

            let redacted = redact_sensitive_json(&body);

            assert!(redacted.redacted);
            assert_eq!(redacted.value["result"], "<redacted>");
        }

        /// 验证 JSON 文本可以直接脱敏成字符串。
        #[test]
        fn redact_sensitive_json_text_redacts_json_string() {
            let redacted =
                redact_sensitive_json_text(r#"{ "authorization": "Bearer secret", "value": 1 }"#)
                    .unwrap();

            assert!(redacted.redacted);
            assert!(redacted.value.contains(r#""authorization":"<redacted>""#));
            assert!(!redacted.value.contains("Bearer secret"));
        }

        /// 验证非 JSON 文本会按 key-value 关键词脱敏。
        #[test]
        fn redact_sensitive_text_redacts_sensitive_assignments() {
            let redacted = redact_sensitive_text("token=secret&name=node command: whoami");

            assert!(redacted.redacted);
            assert!(redacted.value.contains("token=<redacted>"));
            assert!(redacted.value.contains("name=node"));
            assert!(redacted.value.contains("command: <redacted>"));
            assert!(!redacted.value.contains("secret"));
            assert!(!redacted.value.contains("whoami"));
        }

        /// 验证普通单词中间不会误匹配敏感 key。
        #[test]
        fn redact_sensitive_text_avoids_word_middle_matches() {
            let redacted = redact_sensitive_text("mytoken=visible token=secret");

            assert!(redacted.redacted);
            assert!(redacted.value.contains("mytoken=visible"));
            assert!(redacted.value.contains("token=<redacted>"));
        }

        /// 验证带转义引号的值会整体脱敏，而不是在转义处提前截断。
        #[test]
        fn redact_sensitive_text_handles_escaped_quotes() {
            let redacted = redact_sensitive_text(r#"token=\"abc\\\"def\" other=value"#);

            assert!(redacted.redacted);
            assert_eq!(redacted.value, r#"token=<redacted> other=value"#);
            assert!(!redacted.value.contains("abc"));
            assert!(!redacted.value.contains("def"));
        }
    }
}

/// 通用校验工具。
pub mod validate {
    use std::time::Duration;

    /// 校验字符串去除首尾空白后非空。
    pub fn ensure_non_empty(name: &str, value: &str) -> anyhow::Result<()> {
        if value.trim().is_empty() {
            anyhow::bail!("{name} cannot be empty");
        }
        Ok(())
    }

    /// 校验时间间隔不低于指定最小值。
    pub fn ensure_interval_at_least(
        name: &str,
        value: Duration,
        min: Duration,
    ) -> anyhow::Result<()> {
        if value < min {
            anyhow::bail!("{name} must be at least {:?}", min);
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        //! 通用校验工具测试。

        use super::*;

        /// 验证空白字符串会被拒绝。
        #[test]
        fn ensure_non_empty_rejects_blank_string() {
            let error = ensure_non_empty("field", "  ").unwrap_err();

            assert!(error.to_string().contains("field cannot be empty"));
        }

        /// 验证非空字符串可以通过校验。
        #[test]
        fn ensure_non_empty_accepts_non_blank_string() {
            ensure_non_empty("field", "value").unwrap();
        }

        /// 验证低于最小值的时间间隔会被拒绝。
        #[test]
        fn ensure_interval_at_least_rejects_too_small_value() {
            let error = ensure_interval_at_least(
                "interval",
                Duration::from_millis(1),
                Duration::from_millis(100),
            )
            .unwrap_err();

            assert!(error.to_string().contains("interval must be at least"));
        }

        /// 验证满足最小值的时间间隔可以通过校验。
        #[test]
        fn ensure_interval_at_least_accepts_minimum_value() {
            ensure_interval_at_least(
                "interval",
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .unwrap();
        }
    }
}
