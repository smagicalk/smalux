//! Agent 认证状态的版本化 JSON 文件存储适配器。

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use smalux_protocol::noise::{NoiseIdentity, NoisePublicKey};
use tokio::{fs, io::AsyncWriteExt};
use tracing::{debug, warn};

use super::{AgentStateStore, PersistedAgentState};

const STATE_FORMAT_VERSION: u32 = 1;

/// 默认 JSON 文件状态存储。
pub struct FileAgentStateStore {
    path: PathBuf,
}

impl FileAgentStateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn new_path(&self) -> PathBuf {
        self.path.with_extension("json.new")
    }

    fn backup_path(&self) -> PathBuf {
        self.path.with_extension("json.bak")
    }
}

#[async_trait]
impl AgentStateStore for FileAgentStateStore {
    async fn load(&self) -> anyhow::Result<Option<PersistedAgentState>> {
        let (bytes, loaded_path) = match fs::read(&self.path).await {
            Ok(bytes) => (bytes, self.path.clone()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let backup_path = self.backup_path();
                match fs::read(&backup_path).await {
                    Ok(bytes) => {
                        warn!(path = %self.path.display(), "recovering Agent state from backup");
                        (bytes, backup_path)
                    }
                    Err(backup_error) if backup_error.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(backup_error) => return Err(backup_error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        };
        if !private_file_is_secure(&loaded_path)? {
            warn!(
                path = %loaded_path.display(),
                "Agent identity file permissions allow access beyond the owning account"
            );
        }
        let document: StateDocument = serde_json::from_slice(&bytes)?;
        if document.version != STATE_FORMAT_VERSION {
            anyhow::bail!("unsupported Agent state version {}", document.version);
        }
        Ok(Some(document.state.try_into()?))
    }

    async fn save(&self, state: &PersistedAgentState) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let document = StateDocument {
            version: STATE_FORMAT_VERSION,
            state: StoredAgentState::from(state),
        };
        let bytes = serde_json::to_vec_pretty(&document)?;
        let new_path = self.new_path();
        let backup_path = self.backup_path();

        // 先完整写入并 sync 临时文件，再替换主文件，避免进程异常中断留下半个 JSON。
        let mut file = create_private_file(&new_path).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        drop(file);

        let _ = fs::remove_file(&backup_path).await;
        if fs::try_exists(&self.path).await? {
            fs::rename(&self.path, &backup_path).await?;
        }
        if let Err(error) = fs::rename(&new_path, &self.path).await {
            if fs::try_exists(&backup_path).await? {
                let _ = fs::rename(&backup_path, &self.path).await;
            }
            return Err(error.into());
        }
        let _ = fs::remove_file(&backup_path).await;
        debug!(path = %self.path.display(), stage = ?state.stage(), "saved Agent authentication state");
        Ok(())
    }

    async fn clear(&self) -> anyhow::Result<()> {
        for path in [&self.path, &self.new_path(), &self.backup_path()] {
            match fs::remove_file(path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
async fn create_private_file(path: &Path) -> std::io::Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true).mode(0o600);
    options.open(path).await
}

#[cfg(unix)]
fn private_file_is_secure(path: &Path) -> std::io::Result<bool> {
    use std::os::unix::fs::PermissionsExt;

    Ok(std::fs::metadata(path)?.permissions().mode() & 0o077 == 0)
}

#[cfg(windows)]
async fn create_private_file(path: &Path) -> std::io::Result<fs::File> {
    let file = fs::File::create(path).await?;
    apply_private_file_security(path)?;
    Ok(file)
}

#[cfg(windows)]
fn apply_private_file_security(path: &Path) -> std::io::Result<()> {
    use std::{ffi::c_void, os::windows::ffi::OsStrExt, ptr};

    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::{
            Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW,
            DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, SetFileSecurityW,
        },
    };

    const SDDL_REVISION_1: u32 = 1;
    // 禁止继承目录 ACL，只允许 System、Administrators 和文件所有者完全控制。
    const PRIVATE_FILE_DACL: &str = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)";

    let descriptor_text = std::ffi::OsStr::new(PRIVATE_FILE_DACL)
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let mut descriptor: *mut c_void = ptr::null_mut();
    // SAFETY: descriptor_text 是 NUL 结尾的有效 SDDL；成功时 descriptor 由 LocalFree 释放。
    let converted = unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            descriptor_text.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            ptr::null_mut(),
        )
    };
    if converted == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let wide_path = path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    // SAFETY: wide_path 是 NUL 结尾的现有文件路径，descriptor 在调用期间保持有效。
    let secured = unsafe {
        SetFileSecurityW(
            wide_path.as_ptr(),
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            descriptor,
        )
    };
    // SAFETY: descriptor 由 ConvertStringSecurityDescriptor... 分配且只释放一次。
    unsafe { LocalFree(descriptor) };
    if secured == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
fn private_file_is_secure(path: &Path) -> std::io::Result<bool> {
    use std::{ffi::c_void, os::windows::ffi::OsStrExt, ptr};

    use windows_sys::Win32::{
        Foundation::{ERROR_SUCCESS, LocalFree},
        Security::{
            Authorization::{
                ConvertSecurityDescriptorToStringSecurityDescriptorW, GetNamedSecurityInfoW,
                SE_FILE_OBJECT,
            },
            DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        },
    };

    const SDDL_REVISION_1: u32 = 1;
    let wide_path = path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: wide_path 是 NUL 结尾路径；只请求返回完整描述符，其余可选输出为空。
    let result = unsafe {
        GetNamedSecurityInfoW(
            wide_path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if result != ERROR_SUCCESS {
        return Err(std::io::Error::from_raw_os_error(result as i32));
    }
    let mut text = ptr::null_mut();
    let mut text_len = 0;
    // SAFETY: descriptor 来自 GetNamedSecurityInfoW；text 成功时由 LocalFree 释放。
    let converted = unsafe {
        ConvertSecurityDescriptorToStringSecurityDescriptorW(
            descriptor,
            SDDL_REVISION_1,
            DACL_SECURITY_INFORMATION,
            &mut text,
            &mut text_len,
        )
    };
    let security = if converted == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        // SAFETY: text 指向 text_len 个有效 UTF-16 code unit。
        let value = String::from_utf16_lossy(unsafe {
            std::slice::from_raw_parts(text, text_len as usize)
        });
        // Protected DACL 不会继承目录权限；拒绝常见的宽泛身份。
        Ok(value.starts_with("D:P")
            && ![";;;WD", ";;;AN", ";;;AU", ";;;BU"]
                .iter()
                .any(|trustee| value.contains(trustee)))
    };
    // SAFETY: 两个指针分别由对应 Windows API 分配且只释放一次。
    unsafe {
        if !text.is_null() {
            LocalFree(text.cast::<c_void>());
        }
        LocalFree(descriptor);
    }
    security
}

/// 顶层文档保留显式版本，后续可在不改变领域模型的前提下迁移磁盘格式。
#[derive(Serialize, Deserialize)]
struct StateDocument {
    version: u32,
    state: StoredAgentState,
}

/// 只用于 JSON 编解码的 DTO，不向业务层暴露可变字节数组。
#[derive(Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
enum StoredAgentState {
    IdentityPrepared {
        private_key: Vec<u8>,
        public_key: Vec<u8>,
    },
    RegistrationPending {
        agent_id: String,
        private_key: Vec<u8>,
        public_key: Vec<u8>,
        server_public_keys: Vec<Vec<u8>>,
        registration_id: Vec<u8>,
    },
    Registered {
        agent_id: String,
        private_key: Vec<u8>,
        public_key: Vec<u8>,
        server_public_keys: Vec<Vec<u8>>,
        registration_id: Vec<u8>,
    },
}

impl From<&PersistedAgentState> for StoredAgentState {
    fn from(state: &PersistedAgentState) -> Self {
        let keys = |identity: &NoiseIdentity| {
            (
                identity.export_private_key().as_bytes().to_vec(),
                identity.public_key().as_bytes().to_vec(),
            )
        };
        match state {
            PersistedAgentState::IdentityPrepared { identity } => {
                let (private_key, public_key) = keys(identity);
                Self::IdentityPrepared {
                    private_key,
                    public_key,
                }
            }
            PersistedAgentState::RegistrationPending {
                agent_id,
                identity,
                server_public_keys,
                registration_id,
            } => {
                let (private_key, public_key) = keys(identity);
                Self::RegistrationPending {
                    agent_id: agent_id.clone(),
                    private_key,
                    public_key,
                    server_public_keys: server_public_keys
                        .iter()
                        .map(|key| key.as_bytes().to_vec())
                        .collect(),
                    registration_id: registration_id.to_vec(),
                }
            }
            PersistedAgentState::Registered {
                agent_id,
                identity,
                server_public_keys,
                registration_id,
            } => {
                let (private_key, public_key) = keys(identity);
                Self::Registered {
                    agent_id: agent_id.clone(),
                    private_key,
                    public_key,
                    server_public_keys: server_public_keys
                        .iter()
                        .map(|key| key.as_bytes().to_vec())
                        .collect(),
                    registration_id: registration_id.to_vec(),
                }
            }
        }
    }
}

impl TryFrom<StoredAgentState> for PersistedAgentState {
    type Error = anyhow::Error;

    fn try_from(state: StoredAgentState) -> Result<Self, Self::Error> {
        let identity = |private_key: Vec<u8>, public_key: Vec<u8>| {
            NoiseIdentity::from_parts(&private_key, &public_key).map_err(anyhow::Error::from)
        };
        let server_keys = |values: Vec<Vec<u8>>| -> anyhow::Result<Vec<NoisePublicKey>> {
            let keys = values
                .into_iter()
                .map(|value| NoisePublicKey::from_bytes(&value).map_err(anyhow::Error::from))
                .collect::<Result<Vec<_>, _>>()?;
            if keys.is_empty() {
                anyhow::bail!("registered Agent state must contain a Server public key");
            }
            Ok(keys)
        };
        let registration_id = |value: Vec<u8>| -> anyhow::Result<[u8; 16]> {
            value
                .try_into()
                .map_err(|_| anyhow::anyhow!("registration ID must contain 16 bytes"))
        };
        match state {
            StoredAgentState::IdentityPrepared {
                private_key,
                public_key,
            } => Ok(Self::IdentityPrepared {
                identity: identity(private_key, public_key)?,
            }),
            StoredAgentState::RegistrationPending {
                agent_id,
                private_key,
                public_key,
                server_public_keys,
                registration_id: stored_registration_id,
            } => Ok(Self::RegistrationPending {
                agent_id,
                identity: identity(private_key, public_key)?,
                server_public_keys: server_keys(server_public_keys)?,
                registration_id: registration_id(stored_registration_id)?,
            }),
            StoredAgentState::Registered {
                agent_id,
                private_key,
                public_key,
                server_public_keys,
                registration_id: stored_registration_id,
            } => Ok(Self::Registered {
                agent_id,
                identity: identity(private_key, public_key)?,
                server_public_keys: server_keys(server_public_keys)?,
                registration_id: registration_id(stored_registration_id)?,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use smalux_protocol::noise::NoiseIdentity;
    use uuid::Uuid;

    use super::{super::AgentStateStore, FileAgentStateStore};
    use crate::client::{PersistedAgentState, RegistrationStage};

    #[tokio::test]
    async fn saved_identity_file_is_private_to_the_agent_owner() {
        let directory = std::env::temp_dir().join(format!("smalux-agent-state-{}", Uuid::new_v4()));
        let store = FileAgentStateStore::new(directory.join("identity.json"));
        let state = PersistedAgentState::identity_prepared(NoiseIdentity::generate().unwrap());

        store.save(&state).await.unwrap();

        assert!(super::private_file_is_secure(store.path()).unwrap());
        store.clear().await.unwrap();
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn file_store_round_trips_each_registration_stage() {
        let directory = std::env::temp_dir().join(format!("smalux-agent-state-{}", Uuid::new_v4()));
        let store = Arc::new(FileAgentStateStore::new(directory.join("identity.json")));
        let identity = NoiseIdentity::generate().unwrap();

        let prepared = PersistedAgentState::identity_prepared(identity.clone());
        store.save(&prepared).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.stage(), RegistrationStage::IdentityPrepared);
        assert_eq!(loaded.identity().public_key(), identity.public_key());

        let server = NoiseIdentity::generate().unwrap();
        let pending = PersistedAgentState::registration_pending(
            "agent-1".to_owned(),
            identity.clone(),
            server.public_key(),
            [7; 16],
        );
        store.save(&pending).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.stage(), RegistrationStage::RegistrationPending);
        assert_eq!(loaded.agent_id(), Some("agent-1"));
        assert_eq!(loaded.registration_id(), Some([7; 16]));

        let registered = PersistedAgentState::registered_from_pending(&loaded).unwrap();
        store.save(&registered).await.unwrap();
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.stage(), RegistrationStage::Registered);
        assert_eq!(loaded.server_public_keys(), &[server.public_key()]);

        store.clear().await.unwrap();
        assert!(store.load().await.unwrap().is_none());
        let _ = tokio::fs::remove_dir_all(directory).await;
    }

    #[tokio::test]
    async fn file_store_recovers_a_complete_backup_when_primary_is_missing() {
        let directory = std::env::temp_dir().join(format!("smalux-agent-state-{}", Uuid::new_v4()));
        let store = FileAgentStateStore::new(directory.join("identity.json"));
        let state = PersistedAgentState::identity_prepared(NoiseIdentity::generate().unwrap());
        store.save(&state).await.unwrap();
        tokio::fs::rename(store.path(), store.backup_path())
            .await
            .unwrap();

        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded.stage(), RegistrationStage::IdentityPrepared);

        store.clear().await.unwrap();
        let _ = tokio::fs::remove_dir_all(directory).await;
    }
}
