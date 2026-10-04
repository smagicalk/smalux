//! Agent capability 与插件 inventory 最近快照的持久化 Adapter。

use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sea_orm::{ActiveModelTrait, ActiveValue::Set, EntityTrait};
use smalux_protocol::agent::v1::{AgentCapabilitySnapshot, AgentPluginInventory};

use super::{
    DatabaseError, ServerDatabase,
    entity::{agent_capability, agent_plugin_inventory},
};

impl ServerDatabase {
    /// 保存 Agent 当前能力快照；revision 不回退，重复快照幂等覆盖更新时间。
    pub async fn save_agent_capability(
        &self,
        agent_id: &str,
        snapshot: &AgentCapabilitySnapshot,
    ) -> Result<(), DatabaseError> {
        let revision = i64::try_from(snapshot.revision).map_err(|_| {
            DatabaseError::InvalidAgentSnapshot("capability revision exceeds i64".to_owned())
        })?;
        let now = unix_micros()?;
        match agent_capability::Entity::find_by_id(agent_id)
            .one(self.connection())
            .await?
        {
            Some(model) if model.revision > revision => Ok(()),
            Some(model) => {
                let mut active: agent_capability::ActiveModel = model.into();
                active.revision = Set(revision);
                active.agent_version = Set(snapshot.agent_version.clone());
                active.payload = Set(snapshot.encode_to_vec());
                active.updated_at = Set(now);
                active.update(self.connection()).await?;
                Ok(())
            }
            None => {
                agent_capability::Entity::insert(agent_capability::ActiveModel {
                    agent_id: Set(agent_id.to_owned()),
                    revision: Set(revision),
                    agent_version: Set(snapshot.agent_version.clone()),
                    payload: Set(snapshot.encode_to_vec()),
                    updated_at: Set(now),
                })
                .exec(self.connection())
                .await?;
                Ok(())
            }
        }
    }

    /// 保存 Agent 当前插件 inventory；revision 不回退。
    pub async fn save_agent_plugin_inventory(
        &self,
        agent_id: &str,
        inventory: &AgentPluginInventory,
    ) -> Result<(), DatabaseError> {
        let revision = i64::try_from(inventory.revision).map_err(|_| {
            DatabaseError::InvalidAgentSnapshot("plugin inventory revision exceeds i64".to_owned())
        })?;
        let now = unix_micros()?;
        match agent_plugin_inventory::Entity::find_by_id(agent_id)
            .one(self.connection())
            .await?
        {
            Some(model) if model.revision > revision => Ok(()),
            Some(model) => {
                let mut active: agent_plugin_inventory::ActiveModel = model.into();
                active.revision = Set(revision);
                active.payload = Set(inventory.encode_to_vec());
                active.updated_at = Set(now);
                active.update(self.connection()).await?;
                Ok(())
            }
            None => {
                agent_plugin_inventory::Entity::insert(agent_plugin_inventory::ActiveModel {
                    agent_id: Set(agent_id.to_owned()),
                    revision: Set(revision),
                    payload: Set(inventory.encode_to_vec()),
                    updated_at: Set(now),
                })
                .exec(self.connection())
                .await?;
                Ok(())
            }
        }
    }
}

fn unix_micros() -> Result<i64, DatabaseError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_micros()
        .min(i64::MAX as u128) as i64)
}
