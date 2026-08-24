//! 已建立会话内的心跳与连接 rekey 控制状态机。

use std::time::Instant;

use tracing::{debug, info, trace, warn};

use crate::agent::v1::{Pong, RekeyAck, SessionControl, session_control};

use super::{
    HeartbeatSample, NoiseSessionRole, TonicNoiseSession, TransportError, control_message,
    unix_micros,
};

impl TonicNoiseSession {
    /// 处理一条已经解密的控制消息；业务消息不会进入这个 module。
    pub(super) async fn handle_control(
        &mut self,
        control: SessionControl,
    ) -> Result<(), TransportError> {
        match control.body {
            Some(session_control::Body::Ping(ping)) => {
                trace!(nonce = ping.nonce, "received encrypted heartbeat ping");
                let responder_received_at_unix_micros = unix_micros();
                let responder_sent_at_unix_micros = unix_micros();
                self.send(control_message(session_control::Body::Pong(Pong {
                    nonce: ping.nonce,
                    echoed_sent_at_unix_micros: ping.sent_at_unix_micros,
                    responder_received_at_unix_micros,
                    responder_sent_at_unix_micros,
                })))
                .await
            }
            Some(session_control::Body::Pong(pong)) => {
                let Some(sent_at) = self.pending_pings.remove(&pong.nonce) else {
                    warn!(nonce = pong.nonce, "received an unmatched heartbeat pong");
                    return Ok(());
                };
                self.record_heartbeat_sample(HeartbeatSample {
                    nonce: pong.nonce,
                    rtt: sent_at.elapsed(),
                    sent_at_unix_micros: pong.echoed_sent_at_unix_micros,
                    responder_received_at_unix_micros: pong.responder_received_at_unix_micros,
                    responder_sent_at_unix_micros: pong.responder_sent_at_unix_micros,
                    received_at_unix_micros: unix_micros(),
                });
                Ok(())
            }
            Some(session_control::Body::RekeyRequest(request))
                if self.role != NoiseSessionRole::Initiator =>
            {
                info!(
                    generation = request.generation,
                    "responder received Noise rekey request"
                );
                if request.generation != self.secure.generation().saturating_add(1) {
                    warn!(
                        requested_generation = request.generation,
                        current_generation = self.secure.generation(),
                        "received unexpected Noise rekey generation"
                    );
                    return Err(TransportError::Protocol(
                        "unexpected rekey generation".to_owned(),
                    ));
                }
                self.secure.rekey_incoming();
                self.send(control_message(session_control::Body::RekeyAck(RekeyAck {
                    generation: request.generation,
                })))
                .await?;
                self.secure.rekey_outgoing();
                self.secure.finish_rekey(request.generation);
                self.established_at = Instant::now();
                info!(
                    generation = request.generation,
                    "responder completed Noise rekey"
                );
                Ok(())
            }
            Some(session_control::Body::RekeyRequired(_))
                if self.role == NoiseSessionRole::Initiator =>
            {
                debug!("initiator received a Server rekey requirement");
                Err(TransportError::RekeyRequired)
            }
            Some(session_control::Body::RekeyAck(_)) => {
                warn!("received an unexpected Noise rekey acknowledgement");
                Err(TransportError::Protocol(
                    "unexpected rekey acknowledgement".to_owned(),
                ))
            }
            _ => {
                warn!("received an invalid encrypted session control message");
                Err(TransportError::Protocol(
                    "invalid session control message".to_owned(),
                ))
            }
        }
    }
}
