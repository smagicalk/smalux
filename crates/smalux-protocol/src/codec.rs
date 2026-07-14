//! ClientFrame/ServerFrame 原始编解码、流式长度分帧和 Any 辅助函数。
//!
//! 本模块只处理字节边界、规范 varint 和本地大小限制，不校验 Hello、sequence 或
//! 会话消息顺序；这些语义由 `ProtocolSession` 负责。

use bytes::{Buf, BytesMut};
use prost::{Message, Name};
use prost_types::Any;

use crate::{ClientFrame, Error, Result, ServerFrame};

/// 默认允许的单个 Protobuf Frame 大小：1 MiB。
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
/// 可配置的最小单帧限制：4 KiB。
pub const MIN_FRAME_BYTES: usize = 4 * 1024;
/// 协议实现允许配置的绝对单帧上限：16 MiB。
pub const ABSOLUTE_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Codec 的本地资源限制，远端协商不能提高该值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecLimits {
    max_frame_bytes: usize,
}

impl CodecLimits {
    /// 创建经过范围校验的本地帧限制。
    pub fn new(max_frame_bytes: usize) -> Result<Self> {
        if !(MIN_FRAME_BYTES..=ABSOLUTE_MAX_FRAME_BYTES).contains(&max_frame_bytes) {
            return Err(Error::InvalidFrameLimit {
                value: max_frame_bytes,
                min: MIN_FRAME_BYTES,
                max: ABSOLUTE_MAX_FRAME_BYTES,
            });
        }
        Ok(Self { max_frame_bytes })
    }

    /// 返回当前允许的最大 Frame 字节数，不包含 length-delimited 前缀。
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

/// ClientFrame 和 ServerFrame 的传输无关编解码器。
#[derive(Debug, Clone)]
pub struct FrameCodec {
    limits: CodecLimits,
}

impl FrameCodec {
    /// 使用已经校验的本地限制创建 Codec。
    pub fn new(limits: CodecLimits) -> Result<Self> {
        CodecLimits::new(limits.max_frame_bytes())?;
        Ok(Self { limits })
    }

    /// 返回当前 Codec 的本地资源限制。
    pub const fn limits(&self) -> CodecLimits {
        self.limits
    }

    /// 将 ClientFrame 编码为不带长度前缀的 Protobuf。
    pub fn encode_client(&self, frame: &ClientFrame) -> Result<Vec<u8>> {
        ensure_client_body(frame)?;
        self.encode_raw(frame)
    }

    /// 从一条完整的原始消息解码 ClientFrame。
    pub fn decode_client(&self, bytes: &[u8]) -> Result<ClientFrame> {
        let frame: ClientFrame = self.decode_raw(bytes)?;
        ensure_client_body(&frame)?;
        Ok(frame)
    }

    /// 将 ServerFrame 编码为不带长度前缀的 Protobuf。
    pub fn encode_server(&self, frame: &ServerFrame) -> Result<Vec<u8>> {
        ensure_server_body(frame)?;
        self.encode_raw(frame)
    }

    /// 从一条完整的原始消息解码 ServerFrame。
    pub fn decode_server(&self, bytes: &[u8]) -> Result<ServerFrame> {
        let frame: ServerFrame = self.decode_raw(bytes)?;
        ensure_server_body(&frame)?;
        Ok(frame)
    }

    /// 将 ClientFrame 编码为 Protobuf varint 长度前缀格式。
    pub fn encode_client_delimited(&self, frame: &ClientFrame) -> Result<Vec<u8>> {
        ensure_client_body(frame)?;
        self.encode_delimited(frame)
    }

    /// 从可累积的字节缓冲区尝试读取一个 ClientFrame。
    ///
    /// 数据不完整时返回 `Ok(None)`，且不会消费缓冲区。
    pub fn decode_client_delimited(&self, buffer: &mut BytesMut) -> Result<Option<ClientFrame>> {
        let Some(payload) = self.take_delimited(buffer)? else {
            return Ok(None);
        };
        self.decode_client(&payload).map(Some)
    }

    /// 将 ServerFrame 编码为 Protobuf varint 长度前缀格式。
    pub fn encode_server_delimited(&self, frame: &ServerFrame) -> Result<Vec<u8>> {
        ensure_server_body(frame)?;
        self.encode_delimited(frame)
    }

    /// 从可累积的字节缓冲区尝试读取一个 ServerFrame。
    pub fn decode_server_delimited(&self, buffer: &mut BytesMut) -> Result<Option<ServerFrame>> {
        let Some(payload) = self.take_delimited(buffer)? else {
            return Ok(None);
        };
        self.decode_server(&payload).map(Some)
    }

    fn encode_raw<M: Message>(&self, message: &M) -> Result<Vec<u8>> {
        self.ensure_size(message.encoded_len())?;
        let mut output = Vec::with_capacity(message.encoded_len());
        message.encode(&mut output)?;
        Ok(output)
    }

    fn decode_raw<M: Message + Default>(&self, bytes: &[u8]) -> Result<M> {
        self.ensure_size(bytes.len())?;
        Ok(M::decode(bytes)?)
    }

    fn encode_delimited<M: Message>(&self, message: &M) -> Result<Vec<u8>> {
        self.ensure_size(message.encoded_len())?;
        let mut output = Vec::with_capacity(message.encoded_len() + 5);
        message.encode_length_delimited(&mut output)?;
        Ok(output)
    }

    fn take_delimited(&self, buffer: &mut BytesMut) -> Result<Option<BytesMut>> {
        let Some((payload_len, prefix_len)) = parse_length_delimiter(buffer)? else {
            return Ok(None);
        };
        self.ensure_size(payload_len)?;

        let total_len = prefix_len
            .checked_add(payload_len)
            .ok_or(Error::InvalidLengthDelimiter)?;
        if buffer.len() < total_len {
            return Ok(None);
        }

        buffer.advance(prefix_len);
        Ok(Some(buffer.split_to(payload_len)))
    }

    fn ensure_size(&self, actual: usize) -> Result<()> {
        if actual > self.limits.max_frame_bytes() {
            return Err(Error::FrameTooLarge {
                actual,
                max: self.limits.max_frame_bytes(),
            });
        }
        Ok(())
    }
}

/// 将任意具名 Protobuf 消息包装为 google.protobuf.Any。
pub fn pack_any<M: Message + Name>(message: &M) -> Any {
    Any {
        type_url: M::type_url(),
        value: message.encode_to_vec(),
    }
}

/// 将 Any 解包为指定 Protobuf 类型，并严格校验 type URL。
pub fn unpack_any<M: Message + Name + Default>(value: &Any) -> Result<M> {
    let expected = M::type_url();
    if value.type_url != expected {
        return Err(Error::AnyTypeMismatch {
            expected,
            actual: value.type_url.clone(),
        });
    }
    Ok(M::decode(value.value.as_slice())?)
}

fn ensure_client_body(frame: &ClientFrame) -> Result<()> {
    if frame.body.is_none() {
        return Err(Error::MissingBody {
            message: "ClientFrame",
        });
    }
    Ok(())
}

fn ensure_server_body(frame: &ServerFrame) -> Result<()> {
    if frame.body.is_none() {
        return Err(Error::MissingBody {
            message: "ServerFrame",
        });
    }
    Ok(())
}

fn parse_length_delimiter(bytes: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut value = 0_u64;
    for (index, &byte) in bytes.iter().take(10).enumerate() {
        if index == 9 && byte > 1 {
            return Err(Error::InvalidLengthDelimiter);
        }
        if index > 0 && byte == 0 {
            return Err(Error::InvalidLengthDelimiter);
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            let length = usize::try_from(value).map_err(|_| Error::InvalidLengthDelimiter)?;
            return Ok(Some((length, index + 1)));
        }
    }

    if bytes.len() >= 10 {
        Err(Error::InvalidLengthDelimiter)
    } else {
        Ok(None)
    }
}
