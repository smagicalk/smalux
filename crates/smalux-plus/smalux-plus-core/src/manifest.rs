//! 插件清单模型。
//!
//! 清单描述“插件是什么以及能做什么”，不描述插件如何加载。实际发布时清单会由
//! 仓库元数据签名，Agent 只接受已信任仓库中与清单匹配的二进制文件。

/// 插件版本，采用简单的三段式数字表示，避免公共契约依赖第三方 semver 类型。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct PluginVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl PluginVersion {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl std::fmt::Display for PluginVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// 插件支持的目标平台。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginPlatform {
    Windows,
    Linux,
    Macos,
}

/// 插件清单的最小公共表示。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PluginManifest {
    pub plugin_id: String,
    pub display_name: String,
    pub version: PluginVersion,
    pub platform: PluginPlatform,
    /// 相对插件版本目录的 Worker 入口文件名，禁止使用绝对路径或父目录跳转。
    pub entrypoint: String,
    /// Worker IPC 协议主版本；不同主版本不能直接启动。
    pub protocol_version: u32,
    /// 相对插件版本目录的参数 Schema 文件名。
    pub schema_file: String,
    pub task_types: Vec<String>,
}

impl PluginManifest {
    /// 判断插件是否声明了指定任务类型。
    pub fn supports_task(&self, task_type: &str) -> bool {
        self.task_types.iter().any(|item| item == task_type)
    }

    /// 校验清单中的公共身份字段，避免错误信息拖到 Worker 启动后才暴露。
    pub fn validate(&self) -> Result<(), crate::PluginError> {
        if !is_safe_component(&self.plugin_id) {
            return Err(crate::PluginError::InvalidManifest(
                "plugin_id must be a safe 1..=256 byte identifier".to_owned(),
            ));
        }
        if self.display_name.is_empty()
            || self.display_name.len() > 256
            || has_control_character(&self.display_name)
        {
            return Err(crate::PluginError::InvalidManifest(
                "display_name must contain 1..=256 bytes and no control characters".to_owned(),
            ));
        }
        if self.entrypoint.is_empty()
            || has_control_character(&self.entrypoint)
            || std::path::Path::new(&self.entrypoint).is_absolute()
            || self.entrypoint.split(['/', '\\']).any(|part| part == "..")
        {
            return Err(crate::PluginError::InvalidManifest(
                "entrypoint must be a relative path without parent traversal".to_owned(),
            ));
        }
        if self.protocol_version == 0 {
            return Err(crate::PluginError::InvalidManifest(
                "protocol_version must be greater than zero".to_owned(),
            ));
        }
        if self.schema_file.is_empty()
            || has_control_character(&self.schema_file)
            || std::path::Path::new(&self.schema_file).is_absolute()
            || self.schema_file.split(['/', '\\']).any(|part| part == "..")
        {
            return Err(crate::PluginError::InvalidManifest(
                "schema_file must be a relative path without parent traversal".to_owned(),
            ));
        }
        if self.task_types.is_empty()
            || self.task_types.iter().any(|kind| {
                kind.is_empty()
                    || kind.len() > 256
                    || kind.chars().any(|character| character.is_control())
            })
            || self.task_types.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(crate::PluginError::InvalidManifest(
                "task_types must be sorted, unique and non-empty".to_owned(),
            ));
        }
        Ok(())
    }
}

/// 插件身份会进入目录和日志字段，只允许稳定的 ASCII 路径组件。
fn is_safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn has_control_character(value: &str) -> bool {
    value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> PluginManifest {
        PluginManifest {
            plugin_id: "smalux.plus.test".to_owned(),
            display_name: "Test".to_owned(),
            version: PluginVersion::new(1, 0, 0),
            platform: PluginPlatform::Windows,
            entrypoint: "worker.exe".to_owned(),
            protocol_version: 1,
            schema_file: "schema.pb".to_owned(),
            task_types: vec!["smalux.plus.test.v1".to_owned()],
        }
    }

    #[test]
    fn validation_rejects_path_traversal_identity() {
        let mut value = manifest();
        value.plugin_id = "../outside".to_owned();
        assert!(value.validate().is_err());
    }

    #[test]
    fn validation_accepts_stable_task_names() {
        assert!(manifest().validate().is_ok());
    }

    #[test]
    fn validation_rejects_control_characters_in_display_and_paths() {
        let mut value = manifest();
        value.display_name = "Test\nPlugin".to_owned();
        assert!(value.validate().is_err());

        let mut value = manifest();
        value.entrypoint = "worker\0.exe".to_owned();
        assert!(value.validate().is_err());

        let mut value = manifest();
        value.schema_file = "schema\t.pb".to_owned();
        assert!(value.validate().is_err());
    }
}
