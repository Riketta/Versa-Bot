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

/// One appended record: its guild-scoped sequence number and payload.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredRecord {
    pub seq: u64,
    pub payload: Value,
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

    /// Appends a record to the namespace's append-only log, assigning the
    /// next guild-scoped sequence number and returning it. For high-volume
    /// ordered data (conversation history, audit trails) that outgrows
    /// key-value documents; documents remain the tool for settings and state.
    /// Sequence assignment is a single statement - atomic in SQLite; on
    /// PostgreSQL two concurrent appends to the same scope can collide on the
    /// primary key and one fails loudly (single-process deployments plus
    /// plugin-side per-channel serialization keep this unreachable; storage
    /// level locking is deferred until a multi-instance deployment exists).
    async fn append(&self, namespace: &str, payload: Value) -> Result<u64, StorageError>;

    /// Records after `after_seq` in ascending sequence order, at most
    /// `limit` of them.
    async fn list_after(
        &self,
        namespace: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError>;

    /// Number of records after `after_seq`.
    async fn count_after(&self, namespace: &str, after_seq: u64) -> Result<u64, StorageError>;
}
