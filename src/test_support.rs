//! Shared test doubles for unit tests. Compiled only under `cfg(test)` -
//! part of the single-crate test binary, invisible to release builds.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;

use crate::kernel::{
    models::{GuildId, OutboundError, OutboundMessage, Platform, StorageError},
    spi_ports::{ChatOutputFactoryPort, ChatOutputPort, GuildStorage, StoragePort, StoredRecord},
};

type Row = (String, i64, String, String);

/// Scope of one append-only record log: `(platform, guild_id, namespace)`.
type RecordScope = (String, i64, String);

/// In-memory [`StoragePort`]: guild-partitioned, mirroring the real
/// adapter's isolation shape for documents and records alike.
#[derive(Default)]
pub struct InMemoryStorage {
    documents: Arc<Mutex<HashMap<Row, Value>>>,
    records: Arc<Mutex<HashMap<RecordScope, Vec<(u64, Value)>>>>,
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
            records: Arc::clone(&self.records),
            key_prefix: (platform.as_str().to_owned(), guild_id.get() as i64),
        })
    }
}

struct ScopedView {
    documents: Arc<Mutex<HashMap<Row, Value>>>,
    records: Arc<Mutex<HashMap<RecordScope, Vec<(u64, Value)>>>>,
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

    async fn append(&self, namespace: &str, payload: Value) -> Result<u64, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        let mut records = self.records.lock();
        let scope = records.entry((platform, guild_id, namespace.to_owned())).or_default();
        // Appends are ordered, so the last entry carries the highest seq.
        let seq = scope.last().map_or(1, |(last_seq, _)| last_seq + 1);
        scope.push((seq, payload));
        Ok(seq)
    }

    async fn list_after(
        &self,
        namespace: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        let records = self.records.lock();
        Ok(records
            .get(&(platform, guild_id, namespace.to_owned()))
            .map(|rows| {
                rows.iter()
                    .filter(|(seq, _)| *seq > after_seq)
                    .take(limit as usize)
                    .map(|(seq, payload)| StoredRecord { seq: *seq, payload: payload.clone() })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn count_after(&self, namespace: &str, after_seq: u64) -> Result<u64, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        let records = self.records.lock();
        Ok(records
            .get(&(platform, guild_id, namespace.to_owned()))
            .map_or(0, |rows| rows.iter().filter(|(seq, _)| *seq > after_seq).count() as u64))
    }
}

/// [`ChatOutputPort`] that records every send for assertions.
#[derive(Default)]
pub struct RecordingChatOutput {
    messages: Mutex<Vec<OutboundMessage>>,
}

impl RecordingChatOutput {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Flat text projection of every send (content plus embed title/description)
    /// for simple string assertions.
    #[must_use]
    pub fn messages(&self) -> Vec<String> {
        self.messages.lock().iter().map(text_of).collect()
    }

    /// Every send in full, for asserting flags and embeds.
    #[must_use]
    pub fn sent(&self) -> Vec<OutboundMessage> {
        self.messages.lock().clone()
    }
}

fn text_of(message: &OutboundMessage) -> String {
    if message.embeds.is_empty() {
        return message.content.clone();
    }

    let embeds = message
        .embeds
        .iter()
        .map(|embed| format!("{}: {}", embed.title, embed.description))
        .collect::<Vec<_>>()
        .join("\n");
    if message.content.is_empty() { embeds } else { format!("{}\n{}", message.content, embeds) }
}

#[async_trait]
impl ChatOutputPort for RecordingChatOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        self.messages.lock().push(message);
        Ok(())
    }
}

/// [`StoragePort`] whose reads and writes always fail - fixture for
/// unreadable-policy paths (auth fail-closed, tracker audit-loss logging).
pub struct FailingStorage;

struct FailingView;

#[async_trait]
impl GuildStorage for FailingView {
    async fn get(&self, _namespace: &str, _key: &str) -> Result<Option<Value>, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }

    async fn set(&self, _namespace: &str, _key: &str, _value: Value) -> Result<(), StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }

    async fn delete(&self, _namespace: &str, _key: &str) -> Result<(), StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }

    async fn list_keys(&self, _namespace: &str) -> Result<Vec<String>, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }

    async fn append(&self, _namespace: &str, _payload: Value) -> Result<u64, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }

    async fn list_after(
        &self,
        _namespace: &str,
        _after_seq: u64,
        _limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }

    async fn count_after(&self, _namespace: &str, _after_seq: u64) -> Result<u64, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }
}

impl StoragePort for FailingStorage {
    fn guild_scoped(&self, _platform: Platform, _guild_id: GuildId) -> Arc<dyn GuildStorage> {
        Arc::new(FailingView)
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

    fn stream_output(
        &self,
        _origin: &crate::kernel::models::Origin,
    ) -> Arc<dyn crate::kernel::spi_ports::ChatStreamPort> {
        // No streaming assertions exist yet; plugins under test get a port
        // that reports failure loudly instead of silently pretending success.
        Arc::new(NoopChatStream) as Arc<dyn crate::kernel::spi_ports::ChatStreamPort>
    }
}

/// [`ChatStreamPort`] test double: refuses to stream, so a plugin that
/// unexpectedly reaches for streaming fails its test instead of no-oping.
struct NoopChatStream;

#[async_trait::async_trait]
impl crate::kernel::spi_ports::ChatStreamPort for NoopChatStream {
    async fn begin(
        &self,
        _message: crate::kernel::models::OutboundMessage,
    ) -> Result<crate::kernel::models::MessageId, crate::kernel::models::OutboundError> {
        Err(crate::kernel::models::OutboundError::Send(
            "streaming not supported by the test factory".to_owned(),
        ))
    }

    async fn update(
        &self,
        _message: crate::kernel::models::MessageId,
        _content: String,
    ) -> Result<(), crate::kernel::models::OutboundError> {
        Err(crate::kernel::models::OutboundError::Send(
            "streaming not supported by the test factory".to_owned(),
        ))
    }
}
