use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::kernel::models::{GuildId, Platform, StorageError};

/// Namespace reserved for guild-level settings (timezone, language, feature
/// flags). Plugins use their registered slug as namespace.
pub const GUILD_SETTINGS: &str = "guild";

/// Driven port: guild-partitioned document storage. The kernel consumes it to
/// build event-scoped `GuildStorage` handles (see `KernelServices`).
pub trait StoragePort: Send + Sync {
    fn guild_scoped(&self, platform: Platform, guild_id: GuildId) -> Arc<dyn GuildStorage>;
}

/// All documents of a single guild. The handle is bound to one
/// `(platform, guild_id)` pair at creation and exposes no guild parameter -
/// reading or writing another guild's data is impossible by construction.
/// Namespaced per plugin (`namespace` = the plugin's registered slug);
/// the `guild` namespace is reserved for guild settings.
#[async_trait]
pub trait GuildStorage: Send + Sync {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>, StorageError>;

    async fn set(&self, namespace: &str, key: &str, value: Value) -> Result<(), StorageError>;

    async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError>;

    async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError>;
}
