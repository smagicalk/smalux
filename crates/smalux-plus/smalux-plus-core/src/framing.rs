//! Worker stdin/stdout 的长度前缀帧。

use prost::Message;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{PluginError, WorkerFrame};

pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

pub async fn read_frame<R>(reader: &mut R) -> Result<Option<WorkerFrame>, PluginError>
where
    R: AsyncRead + Unpin,
{
    let mut length = [0u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(PluginError::ExecutionFailed(error.to_string())),
    }
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_FRAME_BYTES {
        return Err(PluginError::ExecutionFailed(format!(
            "invalid Worker frame length {length}"
        )));
    }
    let mut bytes = vec![0u8; length];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|error| PluginError::ExecutionFailed(error.to_string()))?;
    WorkerFrame::decode(bytes.as_slice())
        .map(Some)
        .map_err(|error| PluginError::ExecutionFailed(format!("invalid Worker protobuf: {error}")))
}

pub async fn write_frame<W>(writer: &mut W, frame: &WorkerFrame) -> Result<(), PluginError>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = Vec::with_capacity(frame.encoded_len());
    Message::encode(frame, &mut bytes)
        .map_err(|error| PluginError::ExecutionFailed(error.to_string()))?;
    if bytes.is_empty() || bytes.len() > MAX_FRAME_BYTES {
        return Err(PluginError::ExecutionFailed(format!(
            "Worker frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    writer
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|error| PluginError::ExecutionFailed(error.to_string()))?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| PluginError::ExecutionFailed(error.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|error| PluginError::ExecutionFailed(error.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{read_frame, write_frame};
    use crate::{WorkerFrame, WorkerRequest, protocol::worker_frame};
    use tokio::io::BufReader;

    #[tokio::test]
    async fn frame_round_trips_with_a_big_endian_length_prefix() {
        let frame = WorkerFrame {
            body: Some(worker_frame::Body::Request(WorkerRequest { body: None })),
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &frame).await.unwrap();
        let decoded = read_frame(&mut BufReader::new(bytes.as_slice()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decoded, frame);
    }

    #[tokio::test]
    async fn truncated_frame_is_reported_as_end_of_stream() {
        let bytes = [0, 0, 0, 4, 1, 2];
        assert!(
            read_frame(&mut BufReader::new(bytes.as_slice()))
                .await
                .is_err()
        );
    }
}
