/// 一条在线 agent 连接的可下发句柄
#[derive(Clone, Debug)]
pub(crate) struct AgentConnectionHandle {
    /// 本次 WS 连接 ID，每次重连都会变化。
    pub connection_id: uuid::Uuid,
    /// agent 业务 ID，第一次收到 ClientFrame 后才能确认。
    pub agent_id: Option<String>,
    /// server -> agent 下发队列。
    pub server_tx: tokio::sync::mpsc::Sender<smalux_protocol::ServerFrame>,
}

#[derive(Debug, Default)]
pub(crate) struct AgentConnectionRegistry {
    connection_id_to_handle:
        tokio::sync::RwLock<std::collections::HashMap<uuid::Uuid, AgentConnectionHandle>>,
    agent_id_to_connection_id: tokio::sync::RwLock<std::collections::HashMap<String, uuid::Uuid>>,
}

/// 连接注册表
impl AgentConnectionRegistry {
    pub(crate) fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::default())
    }
    /// 注册一条新连接。
    pub async fn register(&self, handle: AgentConnectionHandle) {
        if let Some(agent_id) = handle.agent_id.clone() {
            self.agent_id_to_connection_id
                .write()
                .await
                .insert(agent_id, handle.connection_id);
        }
        self.connection_id_to_handle
            .write()
            .await
            .insert(handle.connection_id, handle);
    }

    /// 连接拿到 agent_id 后更新索引。
    pub async fn bind_agent_id(&self, connection_id: uuid::Uuid, agent_id: String) {
        let mut connections = self.connection_id_to_handle.write().await;
        if let Some(handle) = connections.get_mut(&connection_id) {
            handle.agent_id = Some(agent_id.clone());
            self.agent_id_to_connection_id
                .write()
                .await
                .insert(agent_id, connection_id);
        }
    }

    ///根据connection_id移除连接
    pub async fn unregister(&self, connection_id: uuid::Uuid) {
        let handle = self
            .connection_id_to_handle
            .write()
            .await
            .remove(&connection_id);
        if let Some(handle) = handle {
            if let Some(agent_id) = handle.agent_id {
                self.agent_id_to_connection_id
                    .write()
                    .await
                    .remove(&agent_id);
            }
        }
    }

    ///根据agent_id 发送信息
    pub async fn send_to_agent(
        &self,
        agent_id: &str,
        server_frame: smalux_protocol::ServerFrame,
    ) -> anyhow::Result<()> {
        let connection_id = self
            .agent_id_to_connection_id
            .read()
            .await
            .get(agent_id)
            .ok_or_else(|| anyhow::anyhow!("agent is not online: {}", agent_id))?
            .clone();
        let server_tx = self
            .connection_id_to_handle
            .read()
            .await
            .get(&connection_id)
            .map(|handle| handle.server_tx.clone())
            .ok_or_else(|| anyhow::anyhow!("agent connection is missing: {agent_id}"))?;
        server_tx.send(server_frame).await?;
        Ok(())
    }
}
