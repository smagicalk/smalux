//! Smalux Noise record 容器的分片、长度校验和发送侧加密。
//!
//! 该容器解决 snow 单条消息 65,535 字节上限，不取代 WSS、gRPC 或 length-delimited
//! Frame 的外层消息边界。接收侧只有在本模块完成边界校验后才会尝试 AEAD 解密。

use snow::TransportState;

use crate::{Error, Result};

/// 单个 Noise record 使用的最大明文分片：60 KiB。
pub const NOISE_RECORD_PLAINTEXT_BYTES: usize = 60 * 1024;
/// ChaChaPoly 附加到每个 Noise transport record 的认证标签字节数。
pub(super) const AEAD_TAG_BYTES: usize = 16;
/// Smalux Noise record 容器的版本化魔数。
pub(super) const RECORD_HEADER: &[u8; 4] = b"SNR1";
/// 为 ProtectedPayload 和外层 Frame 的 Protobuf tag/length 预留的保守空间。
pub(super) const OUTER_PROTOBUF_OVERHEAD: usize = 16;

/// 每个 record 前置的大端 u16 密文长度字节数。
const RECORD_LENGTH_BYTES: usize = 2;
/// Noise 单条 transport 消息允许的最大总字节数。
const NOISE_MAX_RECORD_BYTES: usize = 65_535;

/// 加密单个明文分片、追加长度前缀，并在达到阈值后更新发送密钥。
pub(super) fn encrypt_record(
    transport: &mut TransportState,
    plaintext: &[u8],
    output: &mut Vec<u8>,
    outgoing_records: &mut u64,
    rekey_interval_records: u64,
) -> Result<()> {
    let mut ciphertext = vec![0_u8; plaintext.len() + AEAD_TAG_BYTES];
    let written = transport
        .write_message(plaintext, &mut ciphertext)
        .map_err(|error| record_error("encrypt transport record", error))?;
    ciphertext.truncate(written);
    let length = u16::try_from(written)
        .map_err(|_| Error::Security("Noise record exceeds u16 length".to_owned()))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(&ciphertext);
    *outgoing_records = outgoing_records
        .checked_add(1)
        .ok_or_else(|| Error::Security("Noise outgoing record counter overflow".to_owned()))?;
    if (*outgoing_records).is_multiple_of(rekey_interval_records) {
        transport.rekey_outgoing();
    }
    Ok(())
}

/// 只切分和校验容器边界，不在认证成功前解释任何明文。
pub(super) fn parse_records(ciphertext: &[u8], max_plaintext_bytes: usize) -> Result<Vec<&[u8]>> {
    if !ciphertext.starts_with(RECORD_HEADER) {
        return Err(Error::Security(
            "Noise protected payload has an invalid record header".to_owned(),
        ));
    }
    let mut records = Vec::new();
    let mut offset = RECORD_HEADER.len();
    let mut plaintext_bytes = 0_usize;
    while offset < ciphertext.len() {
        if ciphertext.len() - offset < RECORD_LENGTH_BYTES {
            return Err(Error::Security(
                "Noise protected payload has a truncated record length".to_owned(),
            ));
        }
        let record_len = usize::from(u16::from_be_bytes([
            ciphertext[offset],
            ciphertext[offset + 1],
        ]));
        offset += RECORD_LENGTH_BYTES;
        if !(AEAD_TAG_BYTES..=NOISE_MAX_RECORD_BYTES).contains(&record_len) {
            return Err(Error::Security(format!(
                "Noise record length {record_len} is invalid"
            )));
        }
        let end = offset
            .checked_add(record_len)
            .ok_or_else(|| Error::Security("Noise record length overflow".to_owned()))?;
        if end > ciphertext.len() {
            return Err(Error::Security(
                "Noise protected payload contains a truncated record".to_owned(),
            ));
        }
        plaintext_bytes = plaintext_bytes
            .checked_add(record_len - AEAD_TAG_BYTES)
            .ok_or_else(|| Error::Security("Noise plaintext length overflow".to_owned()))?;
        if plaintext_bytes > max_plaintext_bytes {
            return Err(Error::Security(
                "Noise protected payload exceeds the secure plaintext limit".to_owned(),
            ));
        }
        records.push(&ciphertext[offset..end]);
        offset = end;
    }
    if records.is_empty() {
        return Err(Error::Security(
            "Noise protected payload does not contain a record".to_owned(),
        ));
    }
    Ok(records)
}

/// 计算指定明文在分片、长度前缀和 AEAD 标签后的精确容器大小。
pub(super) fn encrypted_container_len(plaintext_len: usize) -> usize {
    let records = if plaintext_len == 0 {
        1
    } else {
        plaintext_len.div_ceil(NOISE_RECORD_PLAINTEXT_BYTES)
    };
    RECORD_HEADER.len() + plaintext_len + records * (RECORD_LENGTH_BYTES + AEAD_TAG_BYTES)
}

/// 用二分查找反推不会突破协商 Frame 上限的最大明文长度。
pub(super) fn max_plaintext_for_frame(max_frame_bytes: usize) -> usize {
    let available = max_frame_bytes.saturating_sub(OUTER_PROTOBUF_OVERHEAD);
    let mut low = 0_usize;
    let mut high = available;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if encrypted_container_len(middle) <= available {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

/// 为 record 加密错误补充稳定操作上下文。
fn record_error(stage: &'static str, error: impl std::fmt::Display) -> Error {
    Error::Security(format!("Noise {stage} failed: {error}"))
}
