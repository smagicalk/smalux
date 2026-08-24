//! Windows Named Pipe 与 Unix Domain Socket 的版本化有界 JSON 传输。
//!
//! 每条消息使用 `4 字节大端长度 + UTF-8 JSON`。传输层只处理本地连接、帧边界、版本和
//! 超时，业务校验由 `AdminService` 负责。Windows 管道拒绝远程客户端并设置 DACL；Unix
//! Socket 权限固定为 `0600`，因此该端点不能替代面向远程用户的 HTTP 授权层。

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::{
    AdminService, CONTROL_PROTOCOL_VERSION, ControlRequest, ControlResponse,
    MAX_CONTROL_FRAME_BYTES, RequestEnvelope, ResponseEnvelope,
};

const IO_TIMEOUT: Duration = Duration::from_secs(3);

/// 启动当前平台的本地管理监听器，直到全局关闭令牌被取消。
pub(crate) async fn serve(
    endpoint: PathBuf,
    service: Arc<AdminService>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    platform::serve(endpoint, service, shutdown).await
}

/// 连接本地管理端点，发送单个请求并等待单个响应。
///
/// `request_timeout` 覆盖连接、发送和接收的总时长；帧读写内部另有固定超时，避免
/// 已连接但不继续发送数据的客户端永久占用任务。
pub async fn request(
    endpoint: &Path,
    request_timeout: Duration,
    request: ControlRequest,
) -> anyhow::Result<ControlResponse> {
    tokio::time::timeout(request_timeout, platform::request(endpoint, request))
        .await
        .map_err(|_| anyhow::anyhow!("Server management request timed out"))?
}

/// 在已连接流上处理一次请求；一个 IPC 连接不会复用来执行多条管理命令。
async fn serve_stream<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    service: &AdminService,
) -> anyhow::Result<()> {
    let envelope: RequestEnvelope = read_json_frame(&mut stream).await?;
    let response = if envelope.protocol_version == CONTROL_PROTOCOL_VERSION {
        service.handle(envelope.request).await
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

/// 客户端侧的一问一答流程，并校验响应协议版本。
async fn request_stream<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    request: ControlRequest,
) -> anyhow::Result<ControlResponse> {
    write_json_frame(
        &mut stream,
        &RequestEnvelope {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            request,
        },
    )
    .await?;
    let envelope: ResponseEnvelope = read_json_frame(&mut stream).await?;
    anyhow::ensure!(
        envelope.protocol_version == CONTROL_PROTOCOL_VERSION,
        "Server control protocol version {} is unsupported",
        envelope.protocol_version
    );
    Ok(envelope.response)
}

/// 先验证长度上限，再分配缓冲区和解析 JSON，避免依据不可信长度进行无界分配。
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

/// 序列化并写出一个有界 JSON 帧，最后 flush 确保短连接对端能立即读取。
async fn write_json_frame<T: SerializeFrame>(
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

/// 限定帧写入函数只接受可序列化值，避免在签名中重复较长的 trait 路径。
trait SerializeFrame: serde::Serialize {}
impl<T: serde::Serialize> SerializeFrame for T {}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::{ffi::c_void, mem::size_of, os::windows::ffi::OsStrExt, ptr};
    use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::{
            Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW,
            SECURITY_ATTRIBUTES,
        },
    };

    const SDDL_REVISION_1: u32 = 1;
    // 仅允许 LocalSystem、Builtin Administrators 和创建管道的 Owner 完全访问。
    const PIPE_DACL: &str = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)";

    pub async fn serve(
        endpoint: PathBuf,
        service: Arc<AdminService>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut server = create_server(&endpoint)?;
        tracing::info!(endpoint = %endpoint.display(), "Server local management pipe listening");
        loop {
            tokio::select! { result = server.connect() => result?, _ = shutdown.cancelled() => return Ok(()) }
            if let Err(error) = serve_stream(&mut server, &service).await {
                tracing::warn!(error = %error, "Server local management request failed");
            }
            server.disconnect()?;
        }
    }

    /// 创建只接受本机客户端的首个命名管道实例，并附加受限 DACL。
    fn create_server(
        endpoint: &Path,
    ) -> anyhow::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
        let security = PipeSecurity::new()?;
        let options = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .to_owned();
        let server = unsafe {
            options.create_with_security_attributes_raw(endpoint, security.attributes_ptr())?
        };
        Ok(server)
    }

    pub async fn request(
        endpoint: &Path,
        request: ControlRequest,
    ) -> anyhow::Result<ControlResponse> {
        request_stream(ClientOptions::new().open(endpoint)?, request).await
    }

    /// 持有 Win32 安全描述符，保证管道创建完成前底层指针始终有效。
    struct PipeSecurity {
        descriptor: *mut c_void,
        attributes: SECURITY_ATTRIBUTES,
    }
    impl PipeSecurity {
        /// 把常量 SDDL 转换成 Win32 `SECURITY_ATTRIBUTES`。
        fn new() -> anyhow::Result<Self> {
            let wide = std::ffi::OsStr::new(PIPE_DACL)
                .encode_wide()
                .chain(Some(0))
                .collect::<Vec<_>>();
            let mut descriptor = ptr::null_mut();
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
                "failed to create Server control pipe security descriptor: {}",
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
        /// 返回 `create_with_security_attributes_raw` 要求的非 const 原始指针。
        fn attributes_ptr(&self) -> *mut c_void {
            (&self.attributes as *const SECURITY_ATTRIBUTES)
                .cast_mut()
                .cast()
        }
    }
    impl Drop for PipeSecurity {
        fn drop(&mut self) {
            // 转换 API 使用 LocalAlloc 分配描述符，必须配对 LocalFree。
            unsafe { LocalFree(self.descriptor) };
        }
    }
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    use tokio::net::{UnixListener, UnixStream};

    pub async fn serve(
        endpoint: PathBuf,
        service: Arc<AdminService>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        if let Some(parent) = endpoint.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tokio::fs::try_exists(&endpoint).await? {
            // 只清理无法连接的旧 socket；普通文件或正在使用的 socket 都拒绝覆盖。
            let metadata = tokio::fs::symlink_metadata(&endpoint).await?;
            anyhow::ensure!(
                metadata.file_type().is_socket(),
                "Server control endpoint exists but is not a socket: {}",
                endpoint.display()
            );
            match UnixStream::connect(&endpoint).await {
                Ok(_) => anyhow::bail!(
                    "Server control socket is already in use: {}",
                    endpoint.display()
                ),
                Err(_) => tokio::fs::remove_file(&endpoint).await?,
            }
        }
        let listener = UnixListener::bind(&endpoint)?;
        std::fs::set_permissions(&endpoint, std::fs::Permissions::from_mode(0o600))?;
        let result = loop {
            let (stream, _) = tokio::select! { result = listener.accept() => result?, _ = shutdown.cancelled() => break Ok(()) };
            let service = Arc::clone(&service);
            tokio::spawn(async move {
                if let Err(error) = serve_stream(stream, &service).await {
                    tracing::warn!(error = %error, "Server local management request failed");
                }
            });
        };
        let _ = tokio::fs::remove_file(&endpoint).await;
        result
    }
    pub async fn request(
        endpoint: &Path,
        request: ControlRequest,
    ) -> anyhow::Result<ControlResponse> {
        request_stream(UnixStream::connect(endpoint).await?, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn bounded_frame_round_trips() {
        let (mut left, mut right) = duplex(4096);
        let writer = tokio::spawn(async move {
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
        let envelope: RequestEnvelope = read_json_frame(&mut right).await.unwrap();
        writer.await.unwrap();
        assert_eq!(envelope.protocol_version, CONTROL_PROTOCOL_VERSION);
        assert!(matches!(envelope.request, ControlRequest::Status));
    }
}
