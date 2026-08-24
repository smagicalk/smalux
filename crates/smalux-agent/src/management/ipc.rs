//! Windows named pipe 与 Unix domain socket 的统一有界帧传输。

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::{
    CONTROL_PROTOCOL_VERSION, ControlRequest, ControlResponse, MAX_CONTROL_FRAME_BYTES,
    ManagementState, RequestEnvelope, ResponseEnvelope,
};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

pub async fn serve(
    endpoint: PathBuf,
    state: Arc<ManagementState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    platform::serve(endpoint, state, shutdown).await
}

pub async fn request(endpoint: &Path, request: ControlRequest) -> anyhow::Result<ControlResponse> {
    platform::request(endpoint, request).await
}

/// 只探测本地管理端点是否能被打开，不发送请求或改变 Agent 状态。
pub async fn endpoint_is_active(endpoint: &Path) -> bool {
    platform::endpoint_is_active(endpoint).await
}

async fn serve_stream<S>(mut stream: S, state: &ManagementState) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let envelope: RequestEnvelope = read_json_frame(&mut stream).await?;
    let response = if envelope.protocol_version == CONTROL_PROTOCOL_VERSION {
        state.handle(envelope.request).await
    } else {
        ControlResponse::Error {
            code: "unsupported_protocol_version".to_owned(),
            message: format!(
                "control protocol version {} is unsupported; expected {}",
                envelope.protocol_version, CONTROL_PROTOCOL_VERSION
            ),
        }
    };
    write_json_frame(
        &mut stream,
        &ResponseEnvelope {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            response,
        },
    )
    .await
}

async fn request_stream<S>(
    mut stream: S,
    request: ControlRequest,
) -> anyhow::Result<ControlResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_json_frame(
        &mut stream,
        &RequestEnvelope {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            request,
        },
    )
    .await?;
    let response: ResponseEnvelope = read_json_frame(&mut stream).await?;
    anyhow::ensure!(
        response.protocol_version == CONTROL_PROTOCOL_VERSION,
        "Agent control protocol version {} is unsupported",
        response.protocol_version
    );
    Ok(response.response)
}

async fn read_json_frame<T: serde::de::DeserializeOwned>(
    stream: &mut (impl AsyncRead + Unpin),
) -> anyhow::Result<T> {
    let length = tokio::time::timeout(IO_TIMEOUT, stream.read_u32()).await?? as usize;
    anyhow::ensure!(
        length <= MAX_CONTROL_FRAME_BYTES,
        "control frame exceeds size limit"
    );
    let mut bytes = vec![0; length];
    tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut bytes)).await??;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn write_json_frame<T: serde::Serialize>(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &T,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(value)?;
    anyhow::ensure!(
        bytes.len() <= MAX_CONTROL_FRAME_BYTES,
        "control frame exceeds size limit"
    );
    let length = u32::try_from(bytes.len())?;
    tokio::time::timeout(IO_TIMEOUT, async {
        stream.write_u32(length).await?;
        stream.write_all(&bytes).await?;
        stream.flush().await
    })
    .await??;
    Ok(())
}

#[cfg(windows)]
mod platform {
    use std::{ffi::c_void, mem::size_of, os::windows::ffi::OsStrExt, ptr};

    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::{
            Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW,
            SECURITY_ATTRIBUTES,
        },
    };

    use super::*;

    const SDDL_REVISION_1: u32 = 1;
    // System、Administrators 和对象所有者拥有完全控制；不授予 Everyone/Anonymous。
    const PIPE_DACL: &str = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)";

    pub async fn serve(
        endpoint: PathBuf,
        state: Arc<ManagementState>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut server = create_server(&endpoint)?;
        tracing::info!(endpoint = %endpoint.display(), "Agent local control pipe listening");
        loop {
            tokio::select! {
                result = server.connect() => result?,
                _ = shutdown.cancelled() => return Ok(()),
            }
            if let Err(error) = serve_stream(&mut server, &state).await {
                tracing::warn!(error = %error, "Agent local control request failed");
            }
            server.disconnect()?;
        }
    }

    /// 在同步作用域完成原始安全描述符的创建和释放，避免裸指针进入异步 Future。
    fn create_server(
        endpoint: &Path,
    ) -> anyhow::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
        let security = PipeSecurity::new()?;
        let options = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .to_owned();
        // SAFETY: security 在 pipe 创建完成前保持 SECURITY_ATTRIBUTES 与描述符有效。
        let server = unsafe {
            options.create_with_security_attributes_raw(endpoint, security.attributes_ptr())?
        };
        // SECURITY_ATTRIBUTES 只在 CreateNamedPipeW 调用期间使用，创建后立即释放描述符。
        drop(security);
        Ok(server)
    }

    pub async fn request(
        endpoint: &Path,
        request: ControlRequest,
    ) -> anyhow::Result<ControlResponse> {
        let client = ClientOptions::new().open(endpoint)?;
        request_stream(client, request).await
    }

    pub async fn endpoint_is_active(endpoint: &Path) -> bool {
        ClientOptions::new().open(endpoint).is_ok()
    }

    struct PipeSecurity {
        descriptor: *mut c_void,
        attributes: SECURITY_ATTRIBUTES,
    }

    impl PipeSecurity {
        fn new() -> anyhow::Result<Self> {
            let wide = std::ffi::OsStr::new(PIPE_DACL)
                .encode_wide()
                .chain(Some(0))
                .collect::<Vec<_>>();
            let mut descriptor = ptr::null_mut();
            // SAFETY: wide 是以 NUL 结尾的有效 SDDL；Windows 分配的描述符由 Drop 释放。
            let success = unsafe {
                ConvertStringSecurityDescriptorToSecurityDescriptorW(
                    wide.as_ptr(),
                    SDDL_REVISION_1,
                    &mut descriptor,
                    ptr::null_mut(),
                )
            };
            anyhow::ensure!(
                success != 0,
                "failed to create Agent control pipe security descriptor: {}",
                std::io::Error::last_os_error()
            );
            Ok(Self {
                descriptor,
                attributes: SECURITY_ATTRIBUTES {
                    nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
                    lpSecurityDescriptor: descriptor,
                    bInheritHandle: 0,
                },
            })
        }

        fn attributes_ptr(&self) -> *mut c_void {
            (&self.attributes as *const SECURITY_ATTRIBUTES)
                .cast_mut()
                .cast()
        }
    }

    impl Drop for PipeSecurity {
        fn drop(&mut self) {
            // SAFETY: descriptor 来自 ConvertStringSecurityDescriptor... 且只释放一次。
            unsafe { LocalFree(self.descriptor) };
        }
    }
}

#[cfg(unix)]
mod platform {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    use tokio::net::{UnixListener, UnixStream};

    use super::*;

    pub async fn serve(
        endpoint: PathBuf,
        state: Arc<ManagementState>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        if let Some(parent) = endpoint.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tokio::fs::try_exists(&endpoint).await? {
            let metadata = tokio::fs::symlink_metadata(&endpoint).await?;
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "Agent control endpoint exists but is not a socket: {}",
                endpoint.display()
            );
            match UnixStream::connect(&endpoint).await {
                Ok(_) => anyhow::bail!(
                    "Agent control socket is already in use: {}",
                    endpoint.display()
                ),
                Err(_) => tokio::fs::remove_file(&endpoint).await?,
            }
        }
        let listener = UnixListener::bind(&endpoint)?;
        std::fs::set_permissions(&endpoint, std::fs::Permissions::from_mode(0o600))?;
        tracing::info!(endpoint = %endpoint.display(), "Agent local control socket listening");
        let result = loop {
            let (stream, _) = tokio::select! {
                result = listener.accept() => result?,
                _ = shutdown.cancelled() => break Ok(()),
            };
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(error) = serve_stream(stream, &state).await {
                    tracing::warn!(error = %error, "Agent local control request failed");
                }
            });
        };
        drop(listener);
        let _ = tokio::fs::remove_file(&endpoint).await;
        result
    }

    pub async fn request(
        endpoint: &Path,
        request: ControlRequest,
    ) -> anyhow::Result<ControlResponse> {
        request_stream(UnixStream::connect(endpoint).await?, request).await
    }

    pub async fn endpoint_is_active(endpoint: &Path) -> bool {
        UnixStream::connect(endpoint).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::duplex;

    use super::*;

    #[tokio::test]
    async fn bounded_json_frame_round_trips_request() {
        let (mut left, mut right) = duplex(4096);
        let send = tokio::spawn(async move {
            write_json_frame(
                &mut left,
                &RequestEnvelope {
                    protocol_version: CONTROL_PROTOCOL_VERSION,
                    request: ControlRequest::Status,
                },
            )
            .await
            .unwrap();
        });
        let request: RequestEnvelope = read_json_frame(&mut right).await.unwrap();
        send.await.unwrap();
        assert_eq!(request.protocol_version, CONTROL_PROTOCOL_VERSION);
        assert!(matches!(request.request, ControlRequest::Status));
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_before_allocation() {
        let (mut left, mut right) = duplex(16);
        tokio::spawn(async move {
            left.write_u32((MAX_CONTROL_FRAME_BYTES + 1) as u32)
                .await
                .unwrap();
        });
        let result = read_json_frame::<RequestEnvelope>(&mut right).await;
        assert!(result.unwrap_err().to_string().contains("size limit"));
    }
}
