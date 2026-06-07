//! Smalux 自有协议二进制 wire packet。
//!
//! wire packet 只负责 transport 层路由和承载 payload，不负责认证。安全模式下真正
//! 可信的数据在 Noise 解密后的业务 payload 内，外层 header 不能作为安全依据。

/// 二进制包魔数。
const WIRE_MAGIC: &[u8; 4] = b"SMX1";
/// 当前 wire 版本。
const WIRE_VERSION: u8 = 1;
/// 固定包头长度。
const WIRE_HEADER_LEN: usize = 4 + 1 + 1 + 2 + 16 + 8 + 4;
/// payload 最大长度，避免异常输入导致内存暴涨。
const MAX_WIRE_PAYLOAD_LEN: usize = 1024 * 1024;

/// wire codec 错误。
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// 包长度不足。
    #[error("wire packet is too short")]
    PacketTooShort,
    /// 魔数不匹配。
    #[error("wire packet magic mismatch")]
    InvalidMagic,
    /// wire 版本不支持。
    #[error("unsupported wire packet version: {0}")]
    UnsupportedVersion(u8),
    /// packet kind 不支持。
    #[error("unsupported wire packet kind: {0}")]
    UnsupportedKind(u8),
    /// payload 长度字段不匹配。
    #[error("wire packet payload length mismatch")]
    PayloadLengthMismatch,
    /// payload 超过上限。
    #[error("wire packet payload is too large")]
    PayloadTooLarge,
}

/// Smalux wire packet 类型。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum WirePacketKind {
    /// 未加密业务数据，主要用于开发和联调。
    PlainData = 1,
    /// 握手前的 hello。
    Hello = 2,
    /// Noise 握手数据。
    Handshake = 3,
    /// 加密业务数据。
    SecureData = 4,
    /// 关闭或错误说明。
    Close = 5,
}

impl TryFrom<u8> for WirePacketKind {
    // wire kind 转换失败时统一返回 wire codec 错误。
    type Error = WireError;

    /// 从 wire kind 转换为稳定枚举。
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::PlainData),
            2 => Ok(Self::Hello),
            3 => Ok(Self::Handshake),
            4 => Ok(Self::SecureData),
            5 => Ok(Self::Close),
            other => Err(WireError::UnsupportedKind(other)),
        }
    }
}

impl WirePacketKind {
    /// 返回稳定日志名称。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlainData => "plain_data",
            Self::Hello => "hello",
            Self::Handshake => "handshake",
            Self::SecureData => "secure_data",
            Self::Close => "close",
        }
    }
}

/// Smalux 二进制 wire packet。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WirePacket {
    /// packet 类型。
    pub kind: WirePacketKind,
    /// 预留 flags。
    pub flags: u16,
    /// 连接或 stream session id。
    pub session_id: [u8; 16],
    /// packet 序号。
    pub sequence: u64,
    /// packet payload。
    pub payload: Vec<u8>,
}

impl WirePacket {
    /// 创建 packet。
    pub fn new(
        kind: WirePacketKind,
        session_id: [u8; 16],
        sequence: u64,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            flags: 0,
            session_id,
            sequence,
            payload,
        }
    }

    /// 创建明文业务 packet。
    pub fn plain_data(session_id: [u8; 16], sequence: u64, payload: Vec<u8>) -> Self {
        Self::new(WirePacketKind::PlainData, session_id, sequence, payload)
    }

    /// 创建加密业务 packet。
    pub fn secure_data(session_id: [u8; 16], sequence: u64, payload: Vec<u8>) -> Self {
        Self::new(WirePacketKind::SecureData, session_id, sequence, payload)
    }
}

/// 编码 wire packet。
pub fn encode_wire_packet(packet: &WirePacket) -> Result<Vec<u8>, WireError> {
    if packet.payload.len() > MAX_WIRE_PAYLOAD_LEN {
        return Err(WireError::PayloadTooLarge);
    }

    // wire header 固定使用 big-endian，方便 server 用任意语言按字节序直接解析。
    // 这里不写校验和，完整性由 TLS 或 secure_psk 的 AEAD 负责。
    let mut out = Vec::with_capacity(WIRE_HEADER_LEN + packet.payload.len());
    out.extend_from_slice(WIRE_MAGIC);
    out.push(WIRE_VERSION);
    out.push(packet.kind as u8);
    out.extend_from_slice(&packet.flags.to_be_bytes());
    out.extend_from_slice(&packet.session_id);
    out.extend_from_slice(&packet.sequence.to_be_bytes());
    out.extend_from_slice(&(packet.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&packet.payload);
    Ok(out)
}

/// 解码 wire packet。
pub fn decode_wire_packet(input: &[u8]) -> Result<WirePacket, WireError> {
    if input.len() < WIRE_HEADER_LEN {
        return Err(WireError::PacketTooShort);
    }
    if &input[0..4] != WIRE_MAGIC {
        return Err(WireError::InvalidMagic);
    }
    let version = input[4];
    if version != WIRE_VERSION {
        return Err(WireError::UnsupportedVersion(version));
    }

    let kind = WirePacketKind::try_from(input[5])?;
    let flags = u16::from_be_bytes([input[6], input[7]]);
    let mut session_id = [0u8; 16];
    session_id.copy_from_slice(&input[8..24]);
    let sequence = u64::from_be_bytes(input[24..32].try_into().expect("fixed slice length"));
    let payload_len = u32::from_be_bytes(input[32..36].try_into().expect("fixed slice length"));
    let payload_len = payload_len as usize;
    // 先校验声明长度，再切 payload，避免畸形包导致越界或大内存分配。
    if payload_len > MAX_WIRE_PAYLOAD_LEN {
        return Err(WireError::PayloadTooLarge);
    }
    if input.len() != WIRE_HEADER_LEN + payload_len {
        return Err(WireError::PayloadLengthMismatch);
    }

    Ok(WirePacket {
        kind,
        flags,
        session_id,
        sequence,
        payload: input[WIRE_HEADER_LEN..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    //! wire packet 测试。

    use super::*;

    /// 验证 wire packet 可以往返。
    #[test]
    fn wire_packet_roundtrips_binary() {
        let packet = WirePacket::plain_data([1u8; 16], 7, b"hello".to_vec());

        let encoded = encode_wire_packet(&packet).unwrap();
        let decoded = decode_wire_packet(&encoded).unwrap();

        assert_eq!(decoded, packet);
    }

    /// 验证长度不足会被拒绝。
    #[test]
    fn wire_packet_rejects_short_packet() {
        let error = decode_wire_packet(b"BAD").unwrap_err();

        assert!(matches!(error, WireError::PacketTooShort));
    }
}
