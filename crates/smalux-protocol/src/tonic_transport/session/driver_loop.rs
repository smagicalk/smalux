//! `SessionDriver` 独占会话后运行的命令、入站和维护循环。

use std::time::Duration;

use tokio::sync::mpsc;
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::agent::v1::ProtocolFrame;
use crate::tonic_transport::driver::DriverCommand;

use super::{SessionEvent, TonicNoiseSession, TransportError};

impl TonicNoiseSession {
    /// 运行 Driver 的单所有者事件循环，保证 Noise nonce 始终串行推进。
    pub(crate) async fn run_driver(
        mut self,
        mut commands: mpsc::Receiver<DriverCommand>,
        events: mpsc::Sender<Result<SessionEvent, TransportError>>,
    ) {
        info!(role = ?self.role, "Noise session driver started");
        let tick_period = self.heartbeat.interval.max(Duration::from_millis(1));
        let mut maintenance = tokio::time::interval(tick_period);
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        maintenance.tick().await;

        loop {
            tokio::select! {
                inbound = self.inbound.message() => {
                    if !self.handle_driver_inbound(inbound, &events).await { return; }
                }
                command = commands.recv() => {
                    if !self.handle_driver_command(command, &events).await { return; }
                }
                _ = maintenance.tick() => {
                    if !self.handle_driver_maintenance(&events).await { return; }
                }
            }
        }
    }

    async fn handle_driver_inbound(
        &mut self,
        inbound: Result<Option<ProtocolFrame>, Status>,
        events: &mpsc::Sender<Result<SessionEvent, TransportError>>,
    ) -> bool {
        let frame = match inbound {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                info!("remote closed stream; stopping Noise session driver");
                let _ = events.send(Err(TransportError::Closed)).await;
                return false;
            }
            Err(error) => {
                error!(error = %error, "inbound gRPC stream failed in Noise session driver");
                let _ = events.send(Err(TransportError::Status(error))).await;
                return false;
            }
        };
        match self.process_driver_frame(frame).await {
            Ok(Some(event)) => {
                if events.send(Ok(event)).await.is_err() {
                    debug!("business event receiver dropped; stopping Noise session driver");
                    return false;
                }
            }
            Ok(None) => {}
            Err(error) => {
                error!(error = %error, "failed to process inbound Noise frame in driver");
                let _ = events.send(Err(error)).await;
                return false;
            }
        }
        self.flush_buffered_events(events).await.is_ok()
    }

    async fn handle_driver_command(
        &mut self,
        command: Option<DriverCommand>,
        events: &mpsc::Sender<Result<SessionEvent, TransportError>>,
    ) -> bool {
        match command {
            Some(DriverCommand::Send { message, completed }) => match self.send(message).await {
                Ok(()) => {
                    let _ = completed.send(Ok(()));
                    true
                }
                Err(error) => {
                    error!(error = %error, "Driver failed to send encrypted message");
                    let _ = completed.send(Err(error));
                    false
                }
            },
            Some(DriverCommand::Shutdown { completed }) => {
                info!("Noise session driver shutdown requested");
                let _ = completed.send(());
                false
            }
            Some(DriverCommand::Ping { nonce, completed }) => match self.ping(nonce).await {
                Ok(()) => {
                    let _ = completed.send(Ok(()));
                    true
                }
                Err(error) => {
                    warn!(error = %error, "Driver failed to send heartbeat ping");
                    let _ = completed.send(Err(error));
                    false
                }
            },
            Some(DriverCommand::RequestRekey { completed }) => {
                match self.request_rekey().await {
                    Ok(generation) => {
                        let _ = completed.send(Ok(generation));
                    }
                    Err(error) => {
                        warn!(error = %error, "Driver failed to request Noise rekey");
                        let _ = completed.send(Err(error));
                        return false;
                    }
                }
                self.flush_buffered_events(events).await.is_ok()
            }
            Some(DriverCommand::RequireRekey { completed }) => match self.require_rekey().await {
                Ok(generation) => {
                    let _ = completed.send(Ok(generation));
                    true
                }
                Err(error) => {
                    warn!(error = %error, "Driver failed to require Noise rekey");
                    let _ = completed.send(Err(error));
                    false
                }
            },
            Some(DriverCommand::HeartbeatStats { completed }) => {
                let _ = completed.send(Ok(self.heartbeat_stats()));
                true
            }
            None => {
                info!("all Noise session driver command senders dropped");
                false
            }
        }
    }

    async fn handle_driver_maintenance(
        &mut self,
        events: &mpsc::Sender<Result<SessionEvent, TransportError>>,
    ) -> bool {
        if let Err(error) = self.perform_maintenance().await {
            error!(error = %error, "Noise session maintenance failed in driver");
            let _ = events.send(Err(error)).await;
            return false;
        }
        self.flush_buffered_events(events).await.is_ok()
    }

    async fn process_driver_frame(
        &mut self,
        frame: ProtocolFrame,
    ) -> Result<Option<SessionEvent>, TransportError> {
        self.process_inbound_frame(frame)
            .await?
            .map(SessionEvent::try_from)
            .transpose()
    }

    async fn flush_buffered_events(
        &mut self,
        events: &mpsc::Sender<Result<SessionEvent, TransportError>>,
    ) -> Result<(), ()> {
        while let Some(message) = self.buffered_messages.pop_front() {
            events
                .send(SessionEvent::try_from(message))
                .await
                .map_err(|_| ())?;
        }
        Ok(())
    }
}
