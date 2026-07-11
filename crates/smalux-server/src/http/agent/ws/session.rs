#[derive(Debug)]
pub(crate) enum AgentConnectionSecurity {
    Plain,
    Handshaking {
        key_id: String,
        handshake: Option<String>,
        session_id: [u8; 16],
    },
    Secure {
        key_id: String,
        transport: Option<String>,
        session_id: [u8; 16],
    },
}

#[derive(Debug)]
pub(crate) struct AgentConnectionContext {
    pub connection_id: uuid::Uuid,
    pub agent_id: Option<String>,
    pub security: AgentConnectionSecurity,
    pub last_seen_at: std::time::Instant,
    pub last_client_sequence: u64,
    pub last_server_sequence: u64,
    /// server 下发 wire sequence。
    pub next_wire_sequence: u64,
}

impl AgentConnectionContext {
    pub(crate) fn new() -> Self {
        Self {
            connection_id: uuid::Uuid::new_v4(),
            agent_id: None,
            security: AgentConnectionSecurity::Plain,
            last_seen_at: std::time::Instant::now(),
            last_client_sequence: 0,
            last_server_sequence: 0,
            next_wire_sequence: 1,
        }
    }

    fn next_sequence(&mut self) -> u64 {
        let sequence = self.next_wire_sequence;
        self.next_wire_sequence += 1;
        sequence
    }
}

pub(crate) struct AgentWsSession {
    socket: axum::extract::ws::WebSocket,
    state: crate::state::AppState,
    context: AgentConnectionContext,
    server_rx: tokio::sync::mpsc::Receiver<smalux_protocol::ServerFrame>,
}

/// server 内部写 WS 的队列容量。
const WRITER_QUEUE_SIZE: usize = 64;
/// server 下发 ServerFrame 的队列容量。
const SERVER_FRAME_QUEUE_SIZE: usize = 128;

impl AgentWsSession {
    pub fn new(socket: axum::extract::ws::WebSocket, state: crate::state::AppState) -> Self {
        let (_server_tx, server_rx) = tokio::sync::mpsc::channel(10);
        Self {
            socket,
            state,
            context: AgentConnectionContext::new(),
            server_rx,
        }
    }
}
