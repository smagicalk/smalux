//! 本地插件目录的只读发现与 Manifest 校验。

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use smalux_plus_core::{
    PluginManifest, PluginPlatform, PluginSchemaBundle, PluginVersion, SchemaHash,
    protocol::WORKER_PROTOCOL_VERSION,
};
use smalux_protocol::agent::v1::{AgentPluginInventory, PluginInventoryEntry};

/// 发现或校验手工安装插件时的稳定错误。
#[derive(Debug, thiserror::Error)]
pub enum PluginCatalogError {
    #[error("failed to read plugin directory {path}: {source}")]
    ReadDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read plugin manifest {path}: {source}")]
    ReadManifest {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid plugin manifest {path}: {source}")]
    ParseManifest {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("plugin manifest {path} is invalid: {message}")]
    InvalidManifest { path: PathBuf, message: String },
    #[error("failed to read plugin schema {path}: {source}")]
    ReadSchema {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid plugin schema {path}: {message}")]
    InvalidSchema { path: PathBuf, message: String },
    #[error("duplicate plugin {plugin_id} version {version}")]
    Duplicate { plugin_id: String, version: String },
}

/// 一个已经发现且通过基础校验的本地插件版本。
#[derive(Clone, Debug)]
pub struct InstalledPlugin {
    /// Manifest 所在的版本目录；Worker 入口只能相对此目录解析。
    pub directory: PathBuf,
    /// 已解析的插件公开身份和能力。
    pub manifest: PluginManifest,
    /// 已校验的 Schema 原始字节和内容哈希。
    pub schema_bytes: Vec<u8>,
    pub schema_hash: SchemaHash,
}

impl InstalledPlugin {
    /// 版本目录内经 Manifest 限制后的 Worker 入口路径。
    pub fn entrypoint(&self) -> PathBuf {
        self.directory.join(&self.manifest.entrypoint)
    }

    /// 返回校验后的 Schema Bundle。
    pub fn schema(&self) -> Result<PluginSchemaBundle, PluginCatalogError> {
        PluginSchemaBundle::decode_checked(&self.schema_bytes).map_err(|error| {
            PluginCatalogError::InvalidSchema {
                path: self.directory.join(&self.manifest.schema_file),
                message: error.to_string(),
            }
        })
    }

    /// Server inventory 所需的公开、无 Secret 描述。
    fn to_protocol(&self) -> PluginInventoryEntry {
        let mut task_kinds = self.manifest.task_types.clone();
        task_kinds.sort_unstable();
        PluginInventoryEntry {
            plugin_id: self.manifest.plugin_id.clone(),
            version: self.manifest.version.to_string(),
            task_kinds,
            schema_hash: self.schema_hash.to_vec(),
            schema_format_version: smalux_plus_core::schema::SCHEMA_FORMAT_VERSION,
        }
    }
}

/// 已发现插件的确定性快照，以 `(plugin_id, version)` 作为唯一键。
#[derive(Clone, Debug, Default)]
pub struct PluginCatalog {
    root: PathBuf,
    plugins: BTreeMap<(String, PluginVersion), InstalledPlugin>,
}

impl PluginCatalog {
    /// 从 `<root>/<plugin_id>/<version>/plugin.json` 发现手工安装的插件。
    ///
    /// 不存在的目录被视为没有插件，方便首次启动；其他读取错误必须显式返回，避免把
    /// 权限或磁盘错误误报为“Server 没有下发插件”。
    pub fn discover(root: impl Into<PathBuf>) -> Result<Self, PluginCatalogError> {
        let root = root.into();
        if !root.exists() {
            return Ok(Self {
                root,
                plugins: BTreeMap::new(),
            });
        }
        let mut plugins = BTreeMap::new();
        for plugin_entry in
            fs::read_dir(&root).map_err(|source| PluginCatalogError::ReadDirectory {
                path: root.clone(),
                source,
            })?
        {
            let plugin_entry =
                plugin_entry.map_err(|source| PluginCatalogError::ReadDirectory {
                    path: root.clone(),
                    source,
                })?;
            if !plugin_entry.path().is_dir() {
                continue;
            }
            for version_entry in fs::read_dir(plugin_entry.path()).map_err(|source| {
                PluginCatalogError::ReadDirectory {
                    path: plugin_entry.path(),
                    source,
                }
            })? {
                let version_entry =
                    version_entry.map_err(|source| PluginCatalogError::ReadDirectory {
                        path: plugin_entry.path(),
                        source,
                    })?;
                let directory = version_entry.path();
                if !directory.is_dir() {
                    continue;
                }
                let manifest_path = directory.join("plugin.json");
                if !manifest_path.is_file() {
                    continue;
                }
                let bytes = fs::read(&manifest_path).map_err(|source| {
                    PluginCatalogError::ReadManifest {
                        path: manifest_path.clone(),
                        source,
                    }
                })?;
                let manifest: PluginManifest =
                    serde_json::from_slice(&bytes).map_err(|source| {
                        PluginCatalogError::ParseManifest {
                            path: manifest_path.clone(),
                            source,
                        }
                    })?;
                manifest
                    .validate()
                    .map_err(|error| PluginCatalogError::InvalidManifest {
                        path: manifest_path.clone(),
                        message: error.to_string(),
                    })?;
                validate_worker_manifest(&directory, &manifest).map_err(|message| {
                    PluginCatalogError::InvalidManifest {
                        path: manifest_path.clone(),
                        message,
                    }
                })?;
                let schema_path = directory.join(&manifest.schema_file);
                let schema_bytes =
                    fs::read(&schema_path).map_err(|source| PluginCatalogError::ReadSchema {
                        path: schema_path.clone(),
                        source,
                    })?;
                let schema =
                    PluginSchemaBundle::decode_checked(&schema_bytes).map_err(|error| {
                        PluginCatalogError::InvalidSchema {
                            path: schema_path.clone(),
                            message: error.to_string(),
                        }
                    })?;
                let manifest_tasks = manifest
                    .task_types
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                let schema_tasks = schema
                    .tasks
                    .iter()
                    .map(|task| task.task_kind.as_str())
                    .collect::<Vec<_>>();
                if schema.plugin_id != manifest.plugin_id
                    || schema.plugin_version != manifest.version.to_string()
                    || schema_tasks != manifest_tasks
                {
                    return Err(PluginCatalogError::InvalidSchema {
                        path: schema_path,
                        message: "schema identity or task kinds do not match manifest".to_owned(),
                    });
                }
                let schema_hash =
                    schema
                        .hash()
                        .map_err(|error| PluginCatalogError::InvalidSchema {
                            path: schema_path,
                            message: error.to_string(),
                        })?;
                let key = (manifest.plugin_id.clone(), manifest.version);
                let plugin = InstalledPlugin {
                    directory,
                    manifest,
                    schema_bytes,
                    schema_hash,
                };
                if plugins.insert(key.clone(), plugin).is_some() {
                    return Err(PluginCatalogError::Duplicate {
                        plugin_id: key.0,
                        version: key.1.to_string(),
                    });
                }
            }
        }
        Ok(Self { root, plugins })
    }

    /// 插件目录根路径；只供诊断和本地管理展示使用。
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 按稳定身份和版本查找一个已安装插件。
    pub fn get(&self, plugin_id: &str, version: PluginVersion) -> Option<&InstalledPlugin> {
        self.plugins.get(&(plugin_id.to_owned(), version))
    }

    /// 返回确定性排序后的已安装插件集合。
    pub fn plugins(&self) -> impl Iterator<Item = &InstalledPlugin> {
        self.plugins.values()
    }

    /// 按 Server 请求的内容 Hash 查找已经在发现阶段校验过的 Schema 原始字节。
    pub fn schema_bytes(&self, hash: &[u8]) -> Option<&[u8]> {
        self.plugins()
            .find(|plugin| plugin.schema_hash.as_slice() == hash)
            .map(|plugin| plugin.schema_bytes.as_slice())
    }

    /// 构造发送给 Server 的完整公开 inventory。
    pub fn inventory(&self) -> AgentPluginInventory {
        AgentPluginInventory {
            revision: 1,
            plugins: self.plugins().map(InstalledPlugin::to_protocol).collect(),
        }
    }
}

fn validate_worker_manifest(directory: &Path, manifest: &PluginManifest) -> Result<(), String> {
    if manifest.platform != current_platform() {
        return Err(format!(
            "plugin platform {:?} does not match current platform {:?}",
            manifest.platform,
            current_platform()
        ));
    }
    if manifest.protocol_version != WORKER_PROTOCOL_VERSION {
        return Err(format!(
            "plugin Worker protocol version {} does not match Agent version {}",
            manifest.protocol_version, WORKER_PROTOCOL_VERSION
        ));
    }
    let directory = fs::canonicalize(directory)
        .map_err(|error| format!("failed to canonicalize plugin directory: {error}"))?;
    let entrypoint = directory.join(&manifest.entrypoint);
    let metadata = fs::metadata(&entrypoint)
        .map_err(|error| format!("failed to read Worker entrypoint: {error}"))?;
    if !metadata.is_file() {
        return Err("Worker entrypoint must be a regular file".to_owned());
    }
    let entrypoint = fs::canonicalize(&entrypoint)
        .map_err(|error| format!("failed to canonicalize Worker entrypoint: {error}"))?;
    if !entrypoint.starts_with(&directory) {
        return Err("Worker entrypoint resolves outside its plugin version directory".to_owned());
    }
    Ok(())
}

fn current_platform() -> PluginPlatform {
    #[cfg(target_os = "windows")]
    {
        PluginPlatform::Windows
    }
    #[cfg(target_os = "linux")]
    {
        PluginPlatform::Linux
    }
    #[cfg(target_os = "macos")]
    {
        PluginPlatform::Macos
    }
}

#[cfg(test)]
mod tests {
    use super::{PluginCatalog, current_platform};
    use prost::Message;
    use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
    use smalux_plus_core::{
        PluginPlatform, PluginSchemaBundle, PluginTaskSchema, SCHEMA_FORMAT_VERSION,
    };
    use std::fs;

    fn schema(plugin_id: &str, version: &str, task_kind: &str) -> Vec<u8> {
        let descriptor_set = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("plugin.proto".to_owned()),
                message_type: vec![DescriptorProto {
                    name: Some("Config".to_owned()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
        .encode_to_vec();
        PluginSchemaBundle {
            format_version: SCHEMA_FORMAT_VERSION,
            plugin_id: plugin_id.to_owned(),
            plugin_version: version.to_owned(),
            descriptor_set,
            tasks: vec![PluginTaskSchema {
                task_kind: task_kind.to_owned(),
                schema_version: 1,
                config_message: "Config".to_owned(),
                ..Default::default()
            }],
        }
        .encode_checked()
        .unwrap()
    }

    fn platform_name() -> &'static str {
        match current_platform() {
            PluginPlatform::Windows => "windows",
            PluginPlatform::Linux => "linux",
            PluginPlatform::Macos => "macos",
        }
    }

    #[test]
    fn discover_returns_a_sorted_public_inventory() {
        let root = std::env::temp_dir().join(format!(
            "smalux-plugin-catalog-test-{}",
            uuid::Uuid::new_v4()
        ));
        let first = root.join("z").join("0.1.0");
        let second = root.join("a").join("0.2.0");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(
            first.join("plugin.json"),
            format!(
                r#"{{"plugin_id":"z","display_name":"Z","version":{{"major":0,"minor":1,"patch":0}},"abi_version":1,"platform":"{}","entrypoint":"worker.exe","protocol_version":1,"schema_file":"schema.pb","task_types":["smalux.z.v1"]}}"#,
                platform_name()
            ),
        )
        .unwrap();
        fs::write(first.join("schema.pb"), schema("z", "0.1.0", "smalux.z.v1")).unwrap();
        fs::write(first.join("worker.exe"), []).unwrap();
        fs::write(
            second.join("plugin.json"),
            format!(
                r#"{{"plugin_id":"a","display_name":"A","version":{{"major":0,"minor":2,"patch":0}},"abi_version":1,"platform":"{}","entrypoint":"worker.exe","protocol_version":1,"schema_file":"schema.pb","task_types":["smalux.a.v1"]}}"#,
                platform_name()
            ),
        )
        .unwrap();
        fs::write(
            second.join("schema.pb"),
            schema("a", "0.2.0", "smalux.a.v1"),
        )
        .unwrap();
        fs::write(second.join("worker.exe"), []).unwrap();

        let inventory = PluginCatalog::discover(&root).unwrap().inventory();

        assert_eq!(inventory.plugins.len(), 2);
        assert_eq!(inventory.plugins[0].plugin_id, "a");
        assert_eq!(inventory.plugins[1].plugin_id, "z");
        fs::remove_dir_all(root).unwrap();
    }
}
