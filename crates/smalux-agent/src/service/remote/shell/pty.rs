//! 远程 shell PTY 管道。
//!
//! `portable-pty` 的读写和 child wait 都是阻塞 API，因此这里统一隔离到专用线程，
//! 对 Tokio 侧只暴露异步输出 channel 和少量控制方法。

use crate::config::model::RemoteShellConfig;
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use std::io::{Read, Write};
use std::sync::mpsc as std_mpsc;
use std::thread::{self, JoinHandle as StdJoinHandle};
use std::time::Duration as StdDuration;
use tokio::sync::{mpsc, oneshot};

/// PTY 输出队列大小。
const SHELL_OUTPUT_CHANNEL_CAPACITY: usize = 128;
/// PTY 单次读取缓冲大小。
const PTY_OUTPUT_BUFFER_SIZE: usize = 8192;
/// child wait 线程轮询间隔。
const CHILD_WAIT_POLL_INTERVAL: StdDuration = StdDuration::from_millis(50);

/// 已启动的 PTY shell。
pub(super) struct PtyShell {
    /// PTY master，用于 resize。
    master: Box<dyn MasterPty + Send>,
    /// 写入 PTY 的阻塞线程入口。
    input_tx: std_mpsc::Sender<Vec<u8>>,
    /// PTY 输出异步接收端。
    pub(super) output_rx: mpsc::Receiver<Vec<u8>>,
    /// child wait 结果。
    pub(super) exit_rx: oneshot::Receiver<anyhow::Result<Option<i32>>>,
    /// child kill 请求。
    kill_tx: std_mpsc::Sender<()>,
    /// PTY reader 线程。
    reader_thread: StdJoinHandle<()>,
    /// PTY writer 线程。
    writer_thread: StdJoinHandle<()>,
    /// child wait 线程。
    child_thread: StdJoinHandle<()>,
}

impl PtyShell {
    /// 调整 PTY 尺寸。
    pub(super) fn resize(&mut self, cols: u16, rows: u16) -> anyhow::Result<()> {
        self.master.resize(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        Ok(())
    }

    /// 写入 PTY 输入。
    pub(super) fn send_input(&self, input: Vec<u8>) -> anyhow::Result<()> {
        self.input_tx.send(input)?;
        Ok(())
    }

    /// 请求停止 child。
    pub(super) fn request_stop(&self) {
        let _ = self.kill_tx.send(());
    }

    /// 关闭 PTY shell 并等待阻塞线程退出。
    pub(super) async fn shutdown(self) {
        self.request_stop();
        let PtyShell {
            master,
            input_tx,
            output_rx,
            exit_rx,
            kill_tx,
            reader_thread,
            writer_thread,
            child_thread,
        } = self;
        drop(master);
        drop(input_tx);
        drop(output_rx);
        drop(exit_rx);
        drop(kill_tx);
        join_shell_thread(reader_thread, "remote shell pty reader").await;
        join_shell_thread(writer_thread, "remote shell pty writer").await;
        join_shell_thread(child_thread, "remote shell child waiter").await;
    }
}

/// 启动本地 PTY shell。
pub(super) fn spawn_pty_shell(
    shell_config: &RemoteShellConfig,
    cols: u16,
    rows: u16,
) -> anyhow::Result<PtyShell> {
    let program = shell_config.program_or_default();
    tracing::info!(
        program = %program,
        cols,
        rows,
        "remote shell pty spawning"
    );

    let pty_system = native_pty_system();
    let pair = pty_system.openpty(PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    })?;
    let command = CommandBuilder::new(program);
    let child = pair.slave.spawn_command(command)?;
    drop(pair.slave);
    let reader = pair.master.try_clone_reader()?;
    let writer = pair.master.take_writer()?;
    let (input_tx, input_rx) = std_mpsc::channel::<Vec<u8>>();
    let (output_tx, output_rx) = mpsc::channel::<Vec<u8>>(SHELL_OUTPUT_CHANNEL_CAPACITY);
    let (kill_tx, kill_rx) = std_mpsc::channel::<()>();
    let (exit_tx, exit_rx) = oneshot::channel::<anyhow::Result<Option<i32>>>();

    let reader_thread = spawn_pty_reader(reader, output_tx);
    let writer_thread = spawn_pty_writer(writer, input_rx);
    let child_thread = spawn_child_waiter(child, kill_rx, exit_tx);

    Ok(PtyShell {
        master: pair.master,
        input_tx,
        output_rx,
        exit_rx,
        kill_tx,
        reader_thread,
        writer_thread,
        child_thread,
    })
}

/// 启动 PTY reader 阻塞线程。
fn spawn_pty_reader(
    mut reader: Box<dyn Read + Send>,
    output_tx: mpsc::Sender<Vec<u8>>,
) -> StdJoinHandle<()> {
    thread::spawn(move || {
        let mut buffer = [0_u8; PTY_OUTPUT_BUFFER_SIZE];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    if output_tx.blocking_send(buffer[..read].to_vec()).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, "remote shell pty reader stopped");
                    break;
                }
            }
        }
    })
}

/// 启动 PTY writer 阻塞线程。
fn spawn_pty_writer(
    mut writer: Box<dyn Write + Send>,
    input_rx: std_mpsc::Receiver<Vec<u8>>,
) -> StdJoinHandle<()> {
    thread::spawn(move || {
        while let Ok(input) = input_rx.recv() {
            if let Err(err) = writer.write_all(&input).and_then(|_| writer.flush()) {
                tracing::debug!(error = %err, "remote shell pty writer stopped");
                break;
            }
        }
    })
}

/// 启动 child wait 阻塞线程。
fn spawn_child_waiter(
    mut child: Box<dyn Child + Send + Sync>,
    kill_rx: std_mpsc::Receiver<()>,
    exit_tx: oneshot::Sender<anyhow::Result<Option<i32>>>,
) -> StdJoinHandle<()> {
    thread::spawn(move || {
        let result = loop {
            if kill_rx.try_recv().is_ok()
                && let Err(err) = child.kill()
            {
                break Err(anyhow::anyhow!(err));
            }

            match child.try_wait() {
                Ok(Some(status)) => break Ok(Some(status.exit_code() as i32)),
                Ok(None) => thread::sleep(CHILD_WAIT_POLL_INTERVAL),
                Err(err) => break Err(anyhow::anyhow!(err)),
            }
        };

        let _ = exit_tx.send(result);
    })
}

/// 等待阻塞线程退出。
async fn join_shell_thread(handle: StdJoinHandle<()>, name: &'static str) {
    match tokio::task::spawn_blocking(move || handle.join()).await {
        Ok(Ok(())) => {}
        Ok(Err(_panic)) => {
            tracing::warn!(thread = name, "remote shell thread panicked");
        }
        Err(err) => {
            tracing::warn!(thread = name, error = %err, "remote shell thread join failed");
        }
    }
}
