//! Smalux 的配置与本地资源目录。
//!
//! default 只保存网络等静态默认值；paths 负责解析应用目录。
//! 目录解析和目录创建分开，调用方可以在读取配置时不产生文件，也可以在启动阶段
//! 显式调用 paths::AppDirectories::ensure_all 创建所需目录。

pub mod default;

/// 跨平台的应用资源目录解析。
pub mod paths {
    use std::{
        env, fs, io,
        path::{Path, PathBuf},
    };

    use thiserror::Error;
    use tracing::{debug, error, warn};

    /// 应用在用户目录和系统目录中的默认名称。
    pub const APPLICATION_NAME: &str = "smalux";
    /// 设置后会覆盖平台默认目录，并在该根目录下使用 config/data/cache 三个目录。
    pub const HOME_ENV: &str = "SMALUX_HOME";

    /// 解析用户主目录失败时返回的稳定错误。
    #[derive(Debug, Error, PartialEq, Eq)]
    pub enum DirectoryError {
        #[error("unable to determine the user home directory; set {HOME_ENV} explicitly")]
        HomeDirectoryUnavailable,
    }

    /// Server、Agent 和其他进程共享的本地资源目录集合。
    ///
    /// 该结构只保存路径，不会在构造时创建目录。这样配置检查、单元测试和只读命令
    /// 不会因为查询目录而改变磁盘状态。
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct AppDirectories {
        config_dir: PathBuf,
        data_dir: PathBuf,
        cache_dir: PathBuf,
    }

    impl AppDirectories {
        /// 按当前平台和环境变量解析目录。
        ///
        /// 设置 SMALUX_HOME 时使用：
        ///
        /// <SMALUX_HOME>/config
        /// <SMALUX_HOME>/data
        /// <SMALUX_HOME>/cache
        ///
        /// 未设置时，Unix 使用 XDG_CONFIG_HOME、XDG_DATA_HOME 和 XDG_CACHE_HOME；
        /// Windows 使用 APPDATA 和 LOCALAPPDATA。环境变量只影响路径计算，
        /// 不会自动创建目录。
        pub fn discover() -> Result<Self, DirectoryError> {
            if let Some(root) = env_path(HOME_ENV) {
                let directories = Self::from_root(root);
                debug!(
                    config_dir = %directories.config_dir.display(),
                    data_dir = %directories.data_dir.display(),
                    cache_dir = %directories.cache_dir.display(),
                    "using SMALUX_HOME application directories"
                );
                return Ok(directories);
            }

            let home = home_dir().ok_or_else(|| {
                warn!(
                    "unable to determine the user home directory while discovering application directories"
                );
                DirectoryError::HomeDirectoryUnavailable
            })?;

            #[cfg(windows)]
            {
                let config_base = env_path("APPDATA").unwrap_or_else(|| home.clone());
                let data_base = env_path("LOCALAPPDATA").unwrap_or_else(|| home.clone());
                let config_dir = config_base.join(APPLICATION_NAME);
                let data_dir = data_base.join(APPLICATION_NAME);
                let cache_dir = data_dir.join("cache");
                let directories = Self::new(config_dir, data_dir, cache_dir);
                debug!(
                    config_dir = %directories.config_dir.display(),
                    data_dir = %directories.data_dir.display(),
                    cache_dir = %directories.cache_dir.display(),
                    "discovered Windows application directories"
                );
                Ok(directories)
            }

            #[cfg(not(windows))]
            {
                let config_base =
                    env_path("XDG_CONFIG_HOME").unwrap_or_else(|| home.join(".config"));
                let data_base =
                    env_path("XDG_DATA_HOME").unwrap_or_else(|| home.join(".local/share"));
                let cache_base = env_path("XDG_CACHE_HOME").unwrap_or_else(|| home.join(".cache"));
                let config_dir = config_base.join(APPLICATION_NAME);
                let data_dir = data_base.join(APPLICATION_NAME);
                let cache_dir = cache_base.join(APPLICATION_NAME);
                let directories = Self::new(config_dir, data_dir, cache_dir);
                debug!(
                    config_dir = %directories.config_dir.display(),
                    data_dir = %directories.data_dir.display(),
                    cache_dir = %directories.cache_dir.display(),
                    "discovered Unix application directories"
                );
                Ok(directories)
            }
        }

        /// 从指定根目录构造一套隔离布局，适合示例、测试和多实例运行。
        pub fn from_root(root: impl Into<PathBuf>) -> Self {
            let root = root.into();
            debug!(root = %root.display(), "building isolated application directories");
            Self::new(root.join("config"), root.join("data"), root.join("cache"))
        }

        fn new(config_dir: PathBuf, data_dir: PathBuf, cache_dir: PathBuf) -> Self {
            Self {
                config_dir,
                data_dir,
                cache_dir,
            }
        }

        /// 配置文件目录，例如 config.toml。
        pub fn config_dir(&self) -> &Path {
            &self.config_dir
        }

        /// 长期业务数据目录，例如 Server 注册表或 Agent 身份。
        pub fn data_dir(&self) -> &Path {
            &self.data_dir
        }

        /// 可删除的缓存目录，不应保存唯一身份或不可恢复数据。
        pub fn cache_dir(&self) -> &Path {
            &self.cache_dir
        }

        /// 日志目录统一放在 data 下，日志文件仍应通过滚动策略定期清理。
        pub fn logs_dir(&self) -> PathBuf {
            self.data_dir.join("logs")
        }

        /// 一次性创建全部公共目录；不会创建业务子目录或写入默认配置。
        pub fn ensure_all(&self) -> io::Result<()> {
            let logs_dir = self.logs_dir();
            debug!(
                config_dir = %self.config_dir.display(),
                data_dir = %self.data_dir.display(),
                cache_dir = %self.cache_dir.display(),
                logs_dir = %logs_dir.display(),
                "creating common application directories"
            );
            for directory in [
                self.config_dir(),
                self.data_dir(),
                self.cache_dir(),
                logs_dir.as_path(),
            ] {
                if let Err(error) = fs::create_dir_all(directory) {
                    error!(
                        directory = %directory.display(),
                        error = %error,
                        "failed to create application directory"
                    );
                    return Err(error);
                }
            }
            debug!("common application directories are ready");
            Ok(())
        }
    }

    /// 获取平台默认的配置目录。
    pub fn config_dir() -> Result<PathBuf, DirectoryError> {
        Ok(AppDirectories::discover()?.config_dir().to_owned())
    }

    /// 获取平台默认的长期数据目录。
    pub fn data_dir() -> Result<PathBuf, DirectoryError> {
        Ok(AppDirectories::discover()?.data_dir().to_owned())
    }

    /// 获取平台默认的缓存目录。
    pub fn cache_dir() -> Result<PathBuf, DirectoryError> {
        Ok(AppDirectories::discover()?.cache_dir().to_owned())
    }

    /// 获取平台默认的日志目录。
    pub fn logs_dir() -> Result<PathBuf, DirectoryError> {
        Ok(AppDirectories::discover()?.logs_dir())
    }

    fn env_path(name: &str) -> Option<PathBuf> {
        env::var_os(name)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }

    #[cfg(windows)]
    fn home_dir() -> Option<PathBuf> {
        env_path("USERPROFILE").or_else(|| env_path("HOME"))
    }

    #[cfg(not(windows))]
    fn home_dir() -> Option<PathBuf> {
        env_path("HOME")
    }

    #[cfg(test)]
    mod tests {
        use super::AppDirectories;
        use std::fs;

        #[test]
        fn from_root_separates_resource_categories() {
            let root = std::path::PathBuf::from("test-root");
            let directories = AppDirectories::from_root(&root);

            assert_eq!(directories.config_dir(), root.join("config").as_path());
            assert_eq!(directories.data_dir(), root.join("data").as_path());
            assert_eq!(directories.cache_dir(), root.join("cache").as_path());
            assert_eq!(directories.logs_dir(), root.join("data/logs"));
        }

        #[test]
        fn ensure_all_creates_only_the_common_layout() {
            let root = std::env::temp_dir()
                .join(format!("smalux-core-directories-{}", std::process::id()));
            if root.exists() {
                fs::remove_dir_all(&root).unwrap();
            }
            let directories = AppDirectories::from_root(&root);

            directories.ensure_all().unwrap();

            assert!(directories.config_dir().is_dir());
            assert!(directories.data_dir().is_dir());
            assert!(directories.cache_dir().is_dir());
            assert!(directories.logs_dir().is_dir());
            assert!(!root.join("server").exists());
            assert!(!root.join("state").exists());
            assert!(!root.join("runtime").exists());
            fs::remove_dir_all(root).unwrap();
        }
    }
}

// 常用目录函数在 config 模块再导出一层，调用方可以直接使用
// `smalux_core::config::config_dir()`，同时仍可访问完整的 `config::paths` API。
pub use paths::{AppDirectories, DirectoryError, cache_dir, config_dir, data_dir, logs_dir};
