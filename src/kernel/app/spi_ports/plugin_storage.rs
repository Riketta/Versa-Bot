use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::kernel::models::StorageError;

/// Driven port: plugin-global document storage - operator-side aggregates
/// and settings that deliberately cross guild boundaries (token counters,
/// plugin-wide state). This is NOT a second user-data store: user content
/// (message text, prompts) must never land here - the guild-partitioned
/// [`StoragePort`](super::StoragePort) exists precisely so such data cannot
/// cross guilds. The kernel trusts this store like its own telemetry:
/// counters, ids, and settings keys are fine, message-sized payloads are not.
#[async_trait]
pub trait PluginStoragePort: Send + Sync {
    /// `platform` is the deployment's stable slug (see `PlatformInfoPort`) -
    /// a namespace label the storage never interprets.
    fn plugin_scoped(&self, platform: &str) -> Arc<dyn PluginStorage>;
}

/// All plugin-global documents of one deployment. The handle is bound to the
/// deployment's platform at creation and exposes no platform parameter.
/// Namespaces are per plugin (the plugin's registered slug); keys are
/// namespace-local.
#[async_trait]
pub trait PluginStorage: Send + Sync {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>, StorageError>;

    async fn set(&self, namespace: &str, key: &str, value: Value) -> Result<(), StorageError>;

    async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError>;

    async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError>;
}
