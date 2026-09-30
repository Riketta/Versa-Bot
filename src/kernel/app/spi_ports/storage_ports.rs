use async_trait::async_trait;

/// Guild-level settings: timezone, language, feature flags, etc.
#[async_trait]
pub trait GuildStoragePort: Send + Sync {
    // async fn get(&self, guild_id: crate::kernel::models::GuildId) ->
    //     Result<Option<serde_json::Value>, crate::kernel::models::OutboundError>;
    // async fn upsert(&self, guild_id: crate::kernel::models::GuildId, settings: serde_json::Value) ->
    //     Result<(), crate::kernel::models::OutboundError>;
    // async fn delete(&self, guild_id: crate::kernel::models::GuildId) ->
    //     Result<(), crate::kernel::models::OutboundError>;
}

/// Arbitrary JSON document store scoped per guild + plugin + key.
/// Plugins never share a namespace; plugin_id is their registered slug.
#[async_trait]
pub trait PluginStoragePort: Send + Sync {
    // async fn get(&self, guild_id: crate::kernel::models::GuildId, plugin_id: &str, key: &str) ->
    //     Result<Option<serde_json::Value>, crate::kernel::models::OutboundError>;
    // async fn set(
    //     &self,
    //     guild_id: crate::kernel::models::GuildId,
    //     plugin_id: &str,
    //     key: &str,
    //     value: serde_json::Value,
    // ) -> Result<(), crate::kernel::models::OutboundError>;
    // async fn delete(&self, guild_id: crate::kernel::models::GuildId, plugin_id: &str, key: &str) ->
    //     Result<(), crate::kernel::models::OutboundError>;
    // async fn list_keys(&self, guild_id: crate::kernel::models::GuildId, plugin_id: &str) ->
    //     Result<Vec<String>, crate::kernel::models::OutboundError>;
}
