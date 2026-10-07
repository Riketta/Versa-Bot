use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::kernel::models::{GuildId, StorageError};

/// Namespace reserved for guild-level settings (timezone, language, feature
/// flags). Plugins use their registered slug as namespace.
pub const GUILD_SETTINGS: &str = "guild";

/// Driven port: guild-partitioned document storage. The kernel consumes it to
/// build event-scoped `GuildStorage` handles (see `KernelServices`);
/// event-less plugins (scheduler-driven pollers) use [`StoragePort::list_guilds`]
/// to discover which guilds exist at all.
#[async_trait]
pub trait StoragePort: Send + Sync {
    /// `platform` is the deployment's stable slug (see `PlatformInfoPort`) -
    /// a namespace label the storage never interprets.
    fn guild_scoped(&self, platform: &str, guild_id: GuildId) -> Arc<dyn GuildStorage>;

    /// Every guild that has at least one stored document, in stable
    /// (platform, guild) order. The platform slug is returned as stored -
    /// poll-driven plugins compare it against `PlatformInfoPort::slug` to
    /// keep their own deployment's guilds. Poll-driven plugins iterate this
    /// to find their per-guild config instead of keeping their own registry.
    async fn list_guilds(&self) -> Result<Vec<(String, GuildId)>, StorageError>;
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

    /// The NEWEST `limit` records of the namespace in ascending sequence
    /// order - the tail read that lets consumers bound their working set
    /// (the engine's live window) without paging the whole log. May include
    /// records below a caller's cutoff; callers filter by their own
    /// watermark.
    async fn list_last(
        &self,
        namespace: &str,
        limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError>;

    /// Number of records after `after_seq`.
    async fn count_after(&self, namespace: &str, after_seq: u64) -> Result<u64, StorageError>;

    /// Removes one record by sequence number, returning how many rows went
    /// (0 = no such record in this namespace). The record log is append-only
    /// by discipline; this is the one sanctioned deletion path, reserved for
    /// explicit moderation removals (the LLM plugin's `/llm_forget`) - it
    /// deletes exactly the addressed row, never a range.
    async fn delete_record(&self, namespace: &str, seq: u64) -> Result<u64, StorageError>;
}
