//! Shared test doubles for unit tests. Compiled only under `cfg(test)` -
//! part of the single-crate test binary, invisible to release builds.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;

use crate::kernel::{
    models::{GuildId, OutboundError, OutboundMessage, Platform, StorageError},
    spi_ports::{ChatOutputFactoryPort, ChatOutputPort, GuildStorage, StoragePort},
};

type Row = (String, i64, String, String);

/// In-memory [`StoragePort`]: guild-partitioned, mirroring the real
/// adapter's isolation shape.
#[derive(Default)]
pub struct InMemoryStorage {
    documents: Arc<Mutex<HashMap<Row, Value>>>,
}

impl InMemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seeds a document directly (test arrangement without going through
    /// the port).
    pub fn seed(
        &self,
        platform: Platform,
        guild_id: GuildId,
        namespace: &str,
        key: &str,
        value: Value,
    ) {
        self.documents.lock().insert(
            (
                platform.as_str().to_owned(),
                guild_id.get() as i64,
                namespace.to_owned(),
                key.to_owned(),
            ),
            value,
        );
    }
}

impl StoragePort for InMemoryStorage {
    fn guild_scoped(&self, platform: Platform, guild_id: GuildId) -> Arc<dyn GuildStorage> {
        Arc::new(ScopedView {
            documents: Arc::clone(&self.documents),
            key_prefix: (platform.as_str().to_owned(), guild_id.get() as i64),
        })
    }
}

struct ScopedView {
    documents: Arc<Mutex<HashMap<Row, Value>>>,
    key_prefix: (String, i64),
}

#[async_trait]
impl GuildStorage for ScopedView {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        Ok(self
            .documents
            .lock()
            .get(&(platform, guild_id, namespace.to_owned(), key.to_owned()))
            .cloned())
    }

    async fn set(&self, namespace: &str, key: &str, value: Value) -> Result<(), StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        self.documents
            .lock()
            .insert((platform, guild_id, namespace.to_owned(), key.to_owned()), value);
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        self.documents.lock().remove(&(platform, guild_id, namespace.to_owned(), key.to_owned()));
        Ok(())
    }

    async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        Ok(self
            .documents
            .lock()
            .keys()
            .filter(|(p, g, ns, _)| p == &platform && g == &guild_id && ns == namespace)
            .map(|(_, _, _, key)| key.clone())
            .collect())
    }
}

/// [`ChatOutputPort`] that records every send for assertions.
#[derive(Default)]
pub struct RecordingChatOutput {
    messages: Mutex<Vec<String>>,
}

impl RecordingChatOutput {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[must_use]
    pub fn messages(&self) -> Vec<String> {
        self.messages.lock().clone()
    }
}

#[async_trait]
impl ChatOutputPort for RecordingChatOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        self.messages.lock().push(message.content);
        Ok(())
    }
}

/// [`ChatOutputFactoryPort`] handing out the same [`RecordingChatOutput`] for
/// every origin and channel - channel-agnostic, so assertions can stay flat.
pub struct RecordingChatOutputFactory {
    output: Arc<RecordingChatOutput>,
}

impl RecordingChatOutputFactory {
    #[must_use]
    pub fn new(output: Arc<RecordingChatOutput>) -> Self {
        Self { output }
    }

    #[must_use]
    pub fn boxed(self) -> Arc<dyn ChatOutputFactoryPort> {
        Arc::new(self)
    }
}

impl ChatOutputFactoryPort for RecordingChatOutputFactory {
    fn chat_output(&self, _origin: &crate::kernel::models::Origin) -> Arc<dyn ChatOutputPort> {
        Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
    }

    fn channel_output(
        &self,
        _origin: &crate::kernel::models::Origin,
        _channel_id: crate::kernel::models::ChannelId,
    ) -> Arc<dyn ChatOutputPort> {
        Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
    }
}
