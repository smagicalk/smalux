//! Agent 连接状态摘要的规范化编码和摘要计算。
//!
//! 摘要不是认证材料，也不能替代完整 Protobuf 快照。它只回答一个问题：Server 和
//! Agent 是否已经持有同一份远程 Job 目录或 Plus runtime。两端都通过本模块计算，
//! 避免各自拼接字符串造成排序和字段遗漏差异。

use blake2::{Blake2s256, Digest};
use prost::Message;

use crate::agent::v1::{PluginRuntimeSnapshot, ReplaceAllJobs};

/// BLAKE2s-256 摘要的固定字节数。
pub const DIGEST_LENGTH: usize = 32;

/// 对规范化后的远程 Job 目录计算 BLAKE2s-256。
pub fn catalog_digest(catalog: &ReplaceAllJobs) -> [u8; DIGEST_LENGTH] {
    let mut canonical = catalog.clone();
    canonical
        .jobs
        .sort_unstable_by(|left, right| left.job_id.cmp(&right.job_id));
    digest_message(&canonical)
}

/// 对规范化后的 Plugin runtime 快照计算 BLAKE2s-256。
pub fn runtime_digest(snapshot: &PluginRuntimeSnapshot) -> [u8; DIGEST_LENGTH] {
    let mut canonical = snapshot.clone();
    canonical.plugins.sort_unstable_by(|left, right| {
        (&left.plugin_id, &left.version).cmp(&(&right.plugin_id, &right.version))
    });
    digest_message(&canonical)
}

/// 对任意 Prost 消息做无额外前缀的 BLAKE2s-256 编码摘要。
fn digest_message<T: Message>(message: &T) -> [u8; DIGEST_LENGTH] {
    Blake2s256::digest(message.encode_to_vec()).into()
}

/// 判断摘要字段是否为空或完整的 BLAKE2s-256。
pub fn valid_digest(bytes: &[u8]) -> bool {
    bytes.is_empty() || bytes.len() == DIGEST_LENGTH
}

#[cfg(test)]
mod tests {
    use super::{catalog_digest, runtime_digest, valid_digest};
    use crate::agent::v1::{
        JobDefinition, PluginRuntimeConfig, PluginRuntimeSnapshot, ReplaceAllJobs,
    };

    #[test]
    fn catalog_digest_is_independent_of_job_order() {
        let first = ReplaceAllJobs {
            catalog_revision: 3,
            jobs: vec![
                JobDefinition {
                    job_id: vec![2],
                    ..Default::default()
                },
                JobDefinition {
                    job_id: vec![1],
                    ..Default::default()
                },
            ],
        };
        let second = ReplaceAllJobs {
            catalog_revision: 3,
            jobs: first.jobs.iter().rev().cloned().collect(),
        };
        assert_eq!(catalog_digest(&first), catalog_digest(&second));
    }

    #[test]
    fn runtime_digest_is_independent_of_plugin_order() {
        let first = PluginRuntimeSnapshot {
            revision: 4,
            plugins: vec![
                PluginRuntimeConfig {
                    plugin_id: "b".into(),
                    version: "1".into(),
                    ..Default::default()
                },
                PluginRuntimeConfig {
                    plugin_id: "a".into(),
                    version: "1".into(),
                    ..Default::default()
                },
            ],
        };
        let second = PluginRuntimeSnapshot {
            revision: 4,
            plugins: first.plugins.iter().rev().cloned().collect(),
        };
        assert_eq!(runtime_digest(&first), runtime_digest(&second));
    }

    #[test]
    fn digest_validation_accepts_only_empty_or_32_bytes() {
        assert!(valid_digest(&[]));
        assert!(valid_digest(&[0; 32]));
        assert!(!valid_digest(&[0; 31]));
        assert!(!valid_digest(&[0; 33]));
    }
}
