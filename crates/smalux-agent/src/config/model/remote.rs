//! 远程能力动态运行配置。
//!
//! 远程执行能力是否启用仍由 CLI-only `ServiceOptions` 控制；这里的配置只描述
//! 能力开启后的运行限制，允许 server 通过 `config_patch` 动态调整。

use super::super::defaults::{
    DEFAULT_REMOTE_PROBE_GLOBAL_MIN_INTERVAL, DEFAULT_REMOTE_PROBE_TARGET_MIN_INTERVAL,
    DEFAULT_REMOTE_PROBE_TIMEOUT, DEFAULT_REMOTE_SHELL_IDLE_TIMEOUT,
    DEFAULT_REMOTE_SHELL_MAX_SESSIONS, DEFAULT_REMOTE_SHELL_SESSION_TIMEOUT,
    DEFAULT_REMOTE_TASK_MAX_CONCURRENT, DEFAULT_REMOTE_TASK_MAX_STDERR_BYTES,
    DEFAULT_REMOTE_TASK_MAX_STDOUT_BYTES, DEFAULT_REMOTE_TASK_TIMEOUT,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::time::Duration;

/// 远程探测最小超时。
const MIN_REMOTE_PROBE_TIMEOUT: Duration = Duration::from_millis(100);
/// 远程探测最大超时。
const MAX_REMOTE_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// 全局远程探测最小启动间隔下限。
const MIN_REMOTE_PROBE_GLOBAL_INTERVAL: Duration = Duration::from_millis(200);
/// 全局远程探测最小启动间隔上限。
const MAX_REMOTE_PROBE_GLOBAL_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// 同一目标远程探测最小启动间隔下限。
const MIN_REMOTE_PROBE_TARGET_INTERVAL: Duration = Duration::from_secs(1);
/// 同一目标远程探测最小启动间隔上限。
const MAX_REMOTE_PROBE_TARGET_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// 远程交互式 shell 运行限制。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RemoteShellConfig {
    /// 最大并发 shell 会话数。
    pub max_sessions: usize,
    /// 会话无输入输出后的空闲超时。
    #[serde(with = "humantime_serde")]
    pub idle_timeout: Duration,
    /// 单个会话最长运行时间。
    #[serde(with = "humantime_serde")]
    pub session_timeout: Duration,
    /// 自定义 shell 程序；为空时按平台选择默认 shell。
    pub program: Option<String>,
}

impl Default for RemoteShellConfig {
    /// 默认使用保守会话限制。
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_REMOTE_SHELL_MAX_SESSIONS,
            idle_timeout: DEFAULT_REMOTE_SHELL_IDLE_TIMEOUT,
            session_timeout: DEFAULT_REMOTE_SHELL_SESSION_TIMEOUT,
            program: None,
        }
    }
}

impl RemoteShellConfig {
    /// 校验远程 shell 动态运行限制。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.max_sessions == 0 {
            anyhow::bail!("remote_shell.max_sessions must be greater than 0");
        }
        if self.idle_timeout.is_zero() {
            anyhow::bail!("remote_shell.idle_timeout must be greater than 0");
        }
        if self.session_timeout.is_zero() {
            anyhow::bail!("remote_shell.session_timeout must be greater than 0");
        }
        if let Some(program) = &self.program
            && program.trim().is_empty()
        {
            anyhow::bail!("remote_shell.program cannot be empty");
        }

        Ok(())
    }

    /// 返回当前实际使用的 shell 程序。
    pub(crate) fn program_or_default(&self) -> &str {
        self.program
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(default_shell_program())
    }
}

/// 远程交互式 shell 配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteShellConfigPatch {
    /// 最大并发 shell 会话数。
    pub max_sessions: Option<usize>,
    /// 会话无输入输出后的空闲超时。
    #[serde(default, with = "humantime_serde")]
    pub idle_timeout: Option<Duration>,
    /// 单个会话最长运行时间。
    #[serde(default, with = "humantime_serde")]
    pub session_timeout: Option<Duration>,
    /// 自定义 shell 程序；传 `null` 表示清空为平台默认 shell。
    #[serde(default, deserialize_with = "deserialize_optional_string_patch")]
    pub program: Option<Option<String>>,
}

impl RemoteShellConfigPatch {
    /// 应用远程 shell 配置 patch。
    pub(crate) fn apply_to(&self, config: &mut RemoteShellConfig) {
        if let Some(max_sessions) = self.max_sessions {
            config.max_sessions = max_sessions;
        }
        if let Some(idle_timeout) = self.idle_timeout {
            config.idle_timeout = idle_timeout;
        }
        if let Some(session_timeout) = self.session_timeout {
            config.session_timeout = session_timeout;
        }
        if let Some(program) = self.program.clone() {
            config.program = program;
        }
    }
}

/// 远程非交互任务运行限制。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RemoteTaskConfig {
    /// 最大并发任务数。
    pub max_concurrent: usize,
    /// 单个任务最大运行时间。
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// stdout 最大回传字节数。
    pub max_stdout_bytes: usize,
    /// stderr 最大回传字节数。
    pub max_stderr_bytes: usize,
}

impl Default for RemoteTaskConfig {
    /// 默认只允许一个远程任务并发执行。
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_REMOTE_TASK_MAX_CONCURRENT,
            timeout: DEFAULT_REMOTE_TASK_TIMEOUT,
            max_stdout_bytes: DEFAULT_REMOTE_TASK_MAX_STDOUT_BYTES,
            max_stderr_bytes: DEFAULT_REMOTE_TASK_MAX_STDERR_BYTES,
        }
    }
}

impl RemoteTaskConfig {
    /// 校验远程任务动态运行限制。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.max_concurrent == 0 {
            anyhow::bail!("remote_task.max_concurrent must be greater than 0");
        }
        if self.timeout.is_zero() {
            anyhow::bail!("remote_task.timeout must be greater than 0");
        }
        if self.max_stdout_bytes == 0 {
            anyhow::bail!("remote_task.max_stdout_bytes must be greater than 0");
        }
        if self.max_stderr_bytes == 0 {
            anyhow::bail!("remote_task.max_stderr_bytes must be greater than 0");
        }

        Ok(())
    }
}

/// 远程非交互任务配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteTaskConfigPatch {
    /// 最大并发任务数。
    pub max_concurrent: Option<usize>,
    /// 单个任务最大运行时间。
    #[serde(default, with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// stdout 最大回传字节数。
    pub max_stdout_bytes: Option<usize>,
    /// stderr 最大回传字节数。
    pub max_stderr_bytes: Option<usize>,
}

impl RemoteTaskConfigPatch {
    /// 应用远程任务配置 patch。
    pub(crate) fn apply_to(&self, config: &mut RemoteTaskConfig) {
        if let Some(max_concurrent) = self.max_concurrent {
            config.max_concurrent = max_concurrent;
        }
        if let Some(timeout) = self.timeout {
            config.timeout = timeout;
        }
        if let Some(max_stdout_bytes) = self.max_stdout_bytes {
            config.max_stdout_bytes = max_stdout_bytes;
        }
        if let Some(max_stderr_bytes) = self.max_stderr_bytes {
            config.max_stderr_bytes = max_stderr_bytes;
        }
    }
}

/// 远程网络探测运行限制。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RemoteProbeConfig {
    /// 是否启用远程探测；默认关闭，server 可通过动态配置打开。
    pub enabled: bool,
    /// 单次探测超时时间。
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// 任意两个探测启动之间的最小间隔。
    #[serde(with = "humantime_serde")]
    pub global_min_interval: Duration,
    /// 同一目标重复探测的最小间隔。
    #[serde(with = "humantime_serde")]
    pub target_min_interval: Duration,
}

impl Default for RemoteProbeConfig {
    /// 默认关闭远程探测，只保留保守运行限制。
    fn default() -> Self {
        Self {
            enabled: false,
            timeout: DEFAULT_REMOTE_PROBE_TIMEOUT,
            global_min_interval: DEFAULT_REMOTE_PROBE_GLOBAL_MIN_INTERVAL,
            target_min_interval: DEFAULT_REMOTE_PROBE_TARGET_MIN_INTERVAL,
        }
    }
}

impl RemoteProbeConfig {
    /// 校验远程探测动态运行限制。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        ensure_duration_range(
            "remote_probe.timeout",
            self.timeout,
            MIN_REMOTE_PROBE_TIMEOUT,
            MAX_REMOTE_PROBE_TIMEOUT,
        )?;
        ensure_duration_range(
            "remote_probe.global_min_interval",
            self.global_min_interval,
            MIN_REMOTE_PROBE_GLOBAL_INTERVAL,
            MAX_REMOTE_PROBE_GLOBAL_INTERVAL,
        )?;
        ensure_duration_range(
            "remote_probe.target_min_interval",
            self.target_min_interval,
            MIN_REMOTE_PROBE_TARGET_INTERVAL,
            MAX_REMOTE_PROBE_TARGET_INTERVAL,
        )
    }
}

/// 远程网络探测配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemoteProbeConfigPatch {
    /// 是否启用远程探测。
    pub enabled: Option<bool>,
    /// 单次探测超时时间。
    #[serde(default, with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// 任意两个探测启动之间的最小间隔。
    #[serde(default, with = "humantime_serde")]
    pub global_min_interval: Option<Duration>,
    /// 同一目标重复探测的最小间隔。
    #[serde(default, with = "humantime_serde")]
    pub target_min_interval: Option<Duration>,
}

impl RemoteProbeConfigPatch {
    /// 应用远程探测配置 patch。
    pub(crate) fn apply_to(&self, config: &mut RemoteProbeConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(timeout) = self.timeout {
            config.timeout = timeout;
        }
        if let Some(global_min_interval) = self.global_min_interval {
            config.global_min_interval = global_min_interval;
        }
        if let Some(target_min_interval) = self.target_min_interval {
            config.target_min_interval = target_min_interval;
        }
    }
}

/// 按平台选择默认 shell 程序。
fn default_shell_program() -> &'static str {
    if cfg!(windows) {
        "powershell.exe"
    } else {
        "/bin/sh"
    }
}

/// 反序列化可清空字符串 patch：缺省表示不修改，`null` 表示清空，字符串表示覆盖。
fn deserialize_optional_string_patch<'de, D>(
    deserializer: D,
) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

/// 校验时间值在允许范围内。
fn ensure_duration_range(
    name: &str,
    value: Duration,
    min: Duration,
    max: Duration,
) -> anyhow::Result<()> {
    if value < min {
        anyhow::bail!("{name} must be at least {min:?}");
    }
    if value > max {
        anyhow::bail!("{name} must be at most {max:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! 远程能力动态配置测试。

    use super::*;

    /// 验证远程 shell 默认限制可用。
    #[test]
    fn remote_shell_config_defaults_are_valid() {
        let config = RemoteShellConfig::default();

        assert_eq!(config.max_sessions, 1);
        assert!(!config.program_or_default().is_empty());
        config.validate().unwrap();
    }

    /// 验证无效远程 shell 限制会被拒绝。
    #[test]
    fn remote_shell_config_rejects_invalid_values() {
        let max_error = RemoteShellConfig {
            max_sessions: 0,
            ..RemoteShellConfig::default()
        }
        .validate()
        .unwrap_err();
        let idle_error = RemoteShellConfig {
            idle_timeout: Duration::ZERO,
            ..RemoteShellConfig::default()
        }
        .validate()
        .unwrap_err();
        let program_error = RemoteShellConfig {
            program: Some(" ".to_string()),
            ..RemoteShellConfig::default()
        }
        .validate()
        .unwrap_err();

        assert!(max_error.to_string().contains("max_sessions"));
        assert!(idle_error.to_string().contains("idle_timeout"));
        assert!(program_error.to_string().contains("program"));
    }

    /// 验证 shell 程序 patch 可以区分缺省、清空和覆盖。
    #[test]
    fn remote_shell_program_patch_distinguishes_missing_null_and_value() {
        let missing: RemoteShellConfigPatch = serde_json::from_str("{}").unwrap();
        let cleared: RemoteShellConfigPatch =
            serde_json::from_str(r#"{ "program": null }"#).unwrap();
        let replaced: RemoteShellConfigPatch =
            serde_json::from_str(r#"{ "program": "powershell.exe" }"#).unwrap();

        assert_eq!(missing.program, None);
        assert_eq!(cleared.program, Some(None));
        assert_eq!(
            replaced.program.as_ref().unwrap().as_deref(),
            Some("powershell.exe")
        );
    }

    /// 验证远程任务默认限制可用。
    #[test]
    fn remote_task_config_defaults_are_valid() {
        let config = RemoteTaskConfig::default();

        assert_eq!(config.max_concurrent, 1);
        assert_eq!(config.timeout, DEFAULT_REMOTE_TASK_TIMEOUT);
        assert_eq!(
            config.max_stdout_bytes,
            DEFAULT_REMOTE_TASK_MAX_STDOUT_BYTES
        );
        assert_eq!(
            config.max_stderr_bytes,
            DEFAULT_REMOTE_TASK_MAX_STDERR_BYTES
        );
        config.validate().unwrap();
    }

    /// 验证远程任务并发数必须大于 0。
    #[test]
    fn remote_task_config_rejects_zero_max_concurrent() {
        let error = RemoteTaskConfig {
            max_concurrent: 0,
            ..RemoteTaskConfig::default()
        }
        .validate()
        .unwrap_err();

        assert!(error.to_string().contains("max_concurrent"));
    }

    /// 验证远程任务输出和超时限制必须大于 0。
    #[test]
    fn remote_task_config_rejects_zero_limits() {
        let timeout_error = RemoteTaskConfig {
            timeout: Duration::ZERO,
            ..RemoteTaskConfig::default()
        }
        .validate()
        .unwrap_err();
        let stdout_error = RemoteTaskConfig {
            max_stdout_bytes: 0,
            ..RemoteTaskConfig::default()
        }
        .validate()
        .unwrap_err();
        let stderr_error = RemoteTaskConfig {
            max_stderr_bytes: 0,
            ..RemoteTaskConfig::default()
        }
        .validate()
        .unwrap_err();

        assert!(timeout_error.to_string().contains("timeout"));
        assert!(stdout_error.to_string().contains("max_stdout_bytes"));
        assert!(stderr_error.to_string().contains("max_stderr_bytes"));
    }

    /// 验证远程探测默认关闭且限制可用。
    #[test]
    fn remote_probe_config_defaults_are_valid() {
        let config = RemoteProbeConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.timeout, DEFAULT_REMOTE_PROBE_TIMEOUT);
        assert_eq!(
            config.global_min_interval,
            DEFAULT_REMOTE_PROBE_GLOBAL_MIN_INTERVAL
        );
        assert_eq!(
            config.target_min_interval,
            DEFAULT_REMOTE_PROBE_TARGET_MIN_INTERVAL
        );
        config.validate().unwrap();
    }

    /// 验证远程探测 patch 支持部分更新。
    #[test]
    fn remote_probe_patch_updates_partial_fields() {
        let mut config = RemoteProbeConfig::default();
        RemoteProbeConfigPatch {
            enabled: Some(true),
            timeout: Some(Duration::from_secs(2)),
            ..RemoteProbeConfigPatch::default()
        }
        .apply_to(&mut config);

        assert!(config.enabled);
        assert_eq!(config.timeout, Duration::from_secs(2));
        assert_eq!(
            config.global_min_interval,
            DEFAULT_REMOTE_PROBE_GLOBAL_MIN_INTERVAL
        );
    }

    /// 验证远程探测超时范围会被校验。
    #[test]
    fn remote_probe_config_rejects_timeout_out_of_range() {
        let small_error = RemoteProbeConfig {
            timeout: MIN_REMOTE_PROBE_TIMEOUT - Duration::from_millis(1),
            ..RemoteProbeConfig::default()
        }
        .validate()
        .unwrap_err();
        let large_error = RemoteProbeConfig {
            timeout: MAX_REMOTE_PROBE_TIMEOUT + Duration::from_millis(1),
            ..RemoteProbeConfig::default()
        }
        .validate()
        .unwrap_err();

        assert!(small_error.to_string().contains("remote_probe.timeout"));
        assert!(large_error.to_string().contains("remote_probe.timeout"));
    }

    /// 验证远程探测频率限制范围会被校验。
    #[test]
    fn remote_probe_config_rejects_interval_out_of_range() {
        let global_error = RemoteProbeConfig {
            global_min_interval: MIN_REMOTE_PROBE_GLOBAL_INTERVAL - Duration::from_millis(1),
            ..RemoteProbeConfig::default()
        }
        .validate()
        .unwrap_err();
        let target_error = RemoteProbeConfig {
            target_min_interval: MIN_REMOTE_PROBE_TARGET_INTERVAL - Duration::from_millis(1),
            ..RemoteProbeConfig::default()
        }
        .validate()
        .unwrap_err();

        assert!(
            global_error
                .to_string()
                .contains("remote_probe.global_min_interval")
        );
        assert!(
            target_error
                .to_string()
                .contains("remote_probe.target_min_interval")
        );
    }
}
