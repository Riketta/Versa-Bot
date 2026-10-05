//! Shared test doubles for unit tests. Compiled only under `cfg(test)` -
//! part of the single-crate test binary, invisible to release builds.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;
use serde_json::Value;

use async_trait::async_trait;

use crate::kernel::{
    models::{GuildId, OutboundError, OutboundMessage, StorageError},
    plugin_ports::{
        CommandDescriptor, CommandHandler, CommandRegistryPort, Job, JobHandle, SchedulerPort,
    },
    spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatTypingGuard, GUILD_SETTINGS, GuildStorage,
        PlatformInfoPort, StoragePort, StoredRecord,
    },
};

/// Slug of the test platform - what [`TestPlatformInfo`] serves and what
/// test fixtures namespace their storage rows under.
pub const TEST_PLATFORM_SLUG: &str = "test";

/// [`PlatformInfoPort`] for tests: identity is just a constant, exactly as
/// an adapter would declare it.
pub struct TestPlatformInfo;

impl PlatformInfoPort for TestPlatformInfo {
    fn slug(&self) -> &'static str {
        TEST_PLATFORM_SLUG
    }

    fn display_name(&self) -> &'static str {
        "Test"
    }

    /// Same caps as the wired adapter, so tests pin the limits production
    /// exercises.
    fn message_limit(&self) -> Option<usize> {
        Some(2000)
    }

    fn embed_limit(&self) -> Option<usize> {
        Some(4096)
    }
}

/// Arc'd [`TestPlatformInfo`] for `KernelServices` fixtures.
#[must_use]
pub fn test_platform_info() -> std::sync::Arc<dyn PlatformInfoPort> {
    std::sync::Arc::new(TestPlatformInfo)
}

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
        platform: &str,
        guild_id: GuildId,
        namespace: &str,
        key: &str,
        value: Value,
    ) {
        self.documents.lock().insert(
            (platform.to_owned(), guild_id.get() as i64, namespace.to_owned(), key.to_owned()),
            value,
        );
    }
}

#[async_trait]
impl StoragePort for InMemoryStorage {
    fn guild_scoped(&self, platform: &str, guild_id: GuildId) -> Arc<dyn GuildStorage> {
        Arc::new(ScopedView {
            documents: Arc::clone(&self.documents),
            records: Arc::clone(&self.records),
            key_prefix: (platform.to_owned(), guild_id.get() as i64),
        })
    }

    /// Distinct scopes over seeded documents and record logs, ordered - the
    /// fake mirrors the real adapter's shape so poll-driven plugins test
    /// their discovery logic.
    async fn list_guilds(&self) -> Result<Vec<(String, GuildId)>, StorageError> {
        let mut scopes: Vec<(String, i64)> = Vec::new();
        {
            let documents = self.documents.lock();
            scopes
                .extend(documents.keys().map(|(platform, guild, _, _)| (platform.clone(), *guild)));
        }
        {
            let records = self.records.lock();
            scopes.extend(records.keys().map(|(platform, guild, _)| (platform.clone(), *guild)));
        }
        scopes.sort();
        scopes.dedup();
        Ok(scopes
            .into_iter()
            .filter_map(|(platform, guild)| {
                let guild = u64::try_from(guild).ok()?;
                Some((platform, GuildId(guild)))
            })
            .collect())
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
        // Same reserved-namespace policy as the real sqlx adapter, so plugin
        // tests fail on a guild-settings write instead of passing against
        // the fake and breaking only against a real database.
        if namespace == GUILD_SETTINGS {
            return Err(StorageError::Forbidden("the 'guild' namespace is reserved".to_owned()));
        }
        let (platform, guild_id) = self.key_prefix.clone();
        self.documents
            .lock()
            .insert((platform, guild_id, namespace.to_owned(), key.to_owned()), value);
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError> {
        if namespace == GUILD_SETTINGS {
            return Err(StorageError::Forbidden("the 'guild' namespace is reserved".to_owned()));
        }
        let (platform, guild_id) = self.key_prefix.clone();
        self.documents.lock().remove(&(platform, guild_id, namespace.to_owned(), key.to_owned()));
        Ok(())
    }

    async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        let mut keys: Vec<String> = self
            .documents
            .lock()
            .keys()
            .filter(|(p, g, ns, _)| p == &platform && g == &guild_id && ns == namespace)
            .map(|(_, _, _, key)| key.clone())
            .collect();
        // The real adapter orders by key (ORDER BY key) - mirror it so
        // multi-key assertions stay deterministic.
        keys.sort();
        Ok(keys)
    }

    async fn append(&self, namespace: &str, payload: Value) -> Result<u64, StorageError> {
        if namespace == GUILD_SETTINGS {
            return Err(StorageError::Forbidden("the 'guild' namespace is reserved".to_owned()));
        }
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

    async fn list_last(
        &self,
        namespace: &str,
        limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        let records = self.records.lock();
        let mut newest: Vec<StoredRecord> = records
            .get(&(platform, guild_id, namespace.to_owned()))
            .map(|rows| {
                rows.iter()
                    .rev()
                    .take(limit as usize)
                    .map(|(seq, payload)| StoredRecord { seq: *seq, payload: payload.clone() })
                    .collect()
            })
            .unwrap_or_default();
        newest.reverse();
        Ok(newest)
    }

    async fn count_after(&self, namespace: &str, after_seq: u64) -> Result<u64, StorageError> {
        let (platform, guild_id) = self.key_prefix.clone();
        let records = self.records.lock();
        Ok(records
            .get(&(platform, guild_id, namespace.to_owned()))
            .map_or(0, |rows| rows.iter().filter(|(seq, _)| *seq > after_seq).count() as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fake mirrors the real adapter's reserved-namespace guard: plugin
    /// writes to `guild` are rejected, reads stay permitted.
    #[tokio::test]
    async fn reserved_guild_namespace_rejects_plugin_writes() {
        let storage = InMemoryStorage::new();
        let guild = storage.guild_scoped("test", GuildId(1));

        let set = guild.set(GUILD_SETTINGS, "language", Value::String("en".to_owned())).await;
        assert!(matches!(set, Err(StorageError::Forbidden(_))));
        let delete = guild.delete(GUILD_SETTINGS, "language").await;
        assert!(matches!(delete, Err(StorageError::Forbidden(_))));
        let append = guild.append(GUILD_SETTINGS, Value::String("x".to_owned())).await;
        assert!(matches!(append, Err(StorageError::Forbidden(_))));

        // Arrangement seeding is direct (not via the port); reads stay
        // permitted for every caller.
        storage.seed(
            "test",
            GuildId(1),
            GUILD_SETTINGS,
            "language",
            Value::String("en".to_owned()),
        );
        let read = guild.get(GUILD_SETTINGS, "language").await.expect("read permitted");
        assert_eq!(read, Some(Value::String("en".to_owned())));
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

    async fn list_last(
        &self,
        _namespace: &str,
        _limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }

    async fn count_after(&self, _namespace: &str, _after_seq: u64) -> Result<u64, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }
}

#[async_trait]
impl StoragePort for FailingStorage {
    fn guild_scoped(&self, _platform: &str, _guild_id: GuildId) -> Arc<dyn GuildStorage> {
        Arc::new(FailingView)
    }

    async fn list_guilds(&self) -> Result<Vec<(String, GuildId)>, StorageError> {
        Err(StorageError::Database("simulated storage failure".to_owned()))
    }
}

/// One send routed through [`ChannelRecordingFactory`]: the guild the
/// output was bound to, the channel it was addressed to, and the flat text
/// projection of the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetedSend {
    pub guild: u64,
    pub channel: u64,
    pub text: String,
}

/// [`ChatOutputFactoryPort`] that records WHICH guild/channel each send
/// targeted - fixture for pinning "delivered to the assigned channel, and
/// nowhere else".
pub struct ChannelRecordingFactory {
    sent: Arc<Mutex<Vec<TargetedSend>>>,
}

impl ChannelRecordingFactory {
    #[must_use]
    pub fn new() -> Self {
        Self { sent: Arc::new(Mutex::new(Vec::new())) }
    }

    /// Every targeted send, in dispatch order.
    #[must_use]
    pub fn targeted(&self) -> Vec<TargetedSend> {
        self.sent.lock().clone()
    }

    #[must_use]
    pub fn boxed(self) -> Arc<dyn ChatOutputFactoryPort> {
        Arc::new(self)
    }

    fn output_for(
        &self,
        guild: Option<GuildId>,
        channel: crate::kernel::models::ChannelId,
    ) -> ChannelRecordingOutput {
        ChannelRecordingOutput {
            sent: Arc::clone(&self.sent),
            guild: guild.map_or(0, GuildId::get),
            channel: channel.get(),
        }
    }
}

impl Default for ChannelRecordingFactory {
    fn default() -> Self {
        Self::new()
    }
}

struct ChannelRecordingOutput {
    sent: Arc<Mutex<Vec<TargetedSend>>>,
    guild: u64,
    channel: u64,
}

#[async_trait]
impl ChatOutputPort for ChannelRecordingOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        self.sent.lock().push(TargetedSend {
            guild: self.guild,
            channel: self.channel,
            text: text_of(&message),
        });
        Ok(())
    }
}

#[async_trait]
impl ChatOutputFactoryPort for ChannelRecordingFactory {
    fn chat_output(&self, origin: &crate::kernel::models::Origin) -> Arc<dyn ChatOutputPort> {
        Arc::new(self.output_for(origin.guild_id, origin.channel_id))
    }

    fn start_typing(&self, _origin: &crate::kernel::models::Origin) -> ChatTypingGuard {
        ChatTypingGuard::dead()
    }

    fn channel_output(
        &self,
        origin: &crate::kernel::models::Origin,
        channel_id: crate::kernel::models::ChannelId,
    ) -> Arc<dyn ChatOutputPort> {
        Arc::new(self.output_for(origin.guild_id, channel_id))
    }

    fn stream_output(
        &self,
        _origin: &crate::kernel::models::Origin,
    ) -> Arc<dyn crate::kernel::spi_ports::ChatStreamPort> {
        Arc::new(NoopChatStream) as Arc<dyn crate::kernel::spi_ports::ChatStreamPort>
    }

    fn message_link(
        &self,
        _origin: &crate::kernel::models::Origin,
        _channel_id: crate::kernel::models::ChannelId,
        _message_id: crate::kernel::models::MessageId,
    ) -> Option<String> {
        None
    }
}

/// [`ChatOutputFactoryPort`] whose sends always fail - fixture for the
/// at-most-once delivery contract (failed send => no record, no bus event,
/// state still advances).
pub struct FailingChatOutputFactory;

struct FailingChatOutput;

#[async_trait]
impl ChatOutputPort for FailingChatOutput {
    async fn send(&self, _message: OutboundMessage) -> Result<(), OutboundError> {
        Err(OutboundError::Send("simulated delivery failure".to_owned()))
    }
}

#[async_trait]
impl ChatOutputFactoryPort for FailingChatOutputFactory {
    fn chat_output(&self, _origin: &crate::kernel::models::Origin) -> Arc<dyn ChatOutputPort> {
        Arc::new(FailingChatOutput)
    }

    fn start_typing(&self, _origin: &crate::kernel::models::Origin) -> ChatTypingGuard {
        ChatTypingGuard::dead()
    }

    fn channel_output(
        &self,
        _origin: &crate::kernel::models::Origin,
        _channel_id: crate::kernel::models::ChannelId,
    ) -> Arc<dyn ChatOutputPort> {
        Arc::new(FailingChatOutput)
    }

    fn stream_output(
        &self,
        _origin: &crate::kernel::models::Origin,
    ) -> Arc<dyn crate::kernel::spi_ports::ChatStreamPort> {
        Arc::new(NoopChatStream) as Arc<dyn crate::kernel::spi_ports::ChatStreamPort>
    }

    fn message_link(
        &self,
        _origin: &crate::kernel::models::Origin,
        _channel_id: crate::kernel::models::ChannelId,
        _message_id: crate::kernel::models::MessageId,
    ) -> Option<String> {
        None
    }
}

/// [`ChatOutputFactoryPort`] handing out the same [`RecordingChatOutput`] for
/// every origin and channel - channel-agnostic, so assertions can stay flat.
pub struct RecordingChatOutputFactory {
    output: Arc<RecordingChatOutput>,
    typing_starts: AtomicUsize,
}

impl RecordingChatOutputFactory {
    #[must_use]
    pub fn new(output: Arc<RecordingChatOutput>) -> Self {
        Self { output, typing_starts: AtomicUsize::new(0) }
    }

    /// How many times the typing indicator was started (the Discord adapter
    /// refreshes it internally; tests only see the start).
    #[must_use]
    pub fn typing_starts(&self) -> usize {
        self.typing_starts.load(Ordering::SeqCst)
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

    fn start_typing(&self, _origin: &crate::kernel::models::Origin) -> ChatTypingGuard {
        self.typing_starts.fetch_add(1, Ordering::SeqCst);
        ChatTypingGuard::dead()
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

    fn message_link(
        &self,
        _origin: &crate::kernel::models::Origin,
        _channel_id: crate::kernel::models::ChannelId,
        _message_id: crate::kernel::models::MessageId,
    ) -> Option<String> {
        None
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

/// [`SchedulerPort`] double: accepts jobs, never runs them - the handle is
/// already dead, so `stop()` paths stay exercisable without a runtime.
#[derive(Default)]
pub struct NoopScheduler;

impl SchedulerPort for NoopScheduler {
    fn schedule(
        &self,
        _name: &str,
        _interval: std::time::Duration,
        _job: Arc<dyn Job>,
    ) -> JobHandle {
        JobHandle::new(Arc::new(|| {}))
    }
}

/// [`CommandRegistryPort`] double that captures what a plugin's `init()`
/// declared, so registration tests can assert descriptors and fetch
/// handlers without a full kernel.
#[derive(Default)]
pub struct CapturingCommandRegistry {
    entries: Mutex<HashMap<String, (CommandDescriptor, Arc<dyn CommandHandler>)>>,
}

impl CapturingCommandRegistry {
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.lock().keys().cloned().collect();
        names.sort();
        names
    }

    #[must_use]
    pub fn handler(&self, name: &str) -> Option<Arc<dyn CommandHandler>> {
        self.entries.lock().get(name).map(|(_, handler)| Arc::clone(handler))
    }
}

impl CommandRegistryPort for CapturingCommandRegistry {
    fn register(&self, descriptor: CommandDescriptor, handler: Arc<dyn CommandHandler>) {
        self.entries.lock().insert(descriptor.name.clone(), (descriptor, handler));
    }

    fn lookup(&self, name: &str) -> Option<Arc<dyn CommandHandler>> {
        self.handler(name)
    }

    fn descriptor(&self, name: &str) -> Option<CommandDescriptor> {
        self.entries.lock().get(name).map(|(descriptor, _)| descriptor.clone())
    }

    fn descriptors(&self) -> Vec<CommandDescriptor> {
        self.names().iter().filter_map(|name| self.descriptor(name)).collect()
    }
}

/// Guards the Discord presentation limits for registered commands:
/// descriptions are the users' only in-app documentation, and the Discord
/// registrar's sync rejects anything over 100 characters with a 400. Call
/// from each plugin's registration test so a too-long mini-doc fails the
/// build, not the production command sync.
///
/// # Panics
/// When any command or argument description exceeds Discord's 100-character
/// cap - the panic names the offending command, argument and text.
pub fn assert_descriptions_fit_discord(
    descriptors: &[crate::kernel::plugin_ports::CommandDescriptor],
) {
    for descriptor in descriptors {
        assert!(
            descriptor.description.chars().count() <= 100,
            "command `{}`: description over Discord's 100-character cap: {:?}",
            descriptor.name,
            descriptor.description
        );
        for argument in &descriptor.arguments {
            assert!(
                argument.description.chars().count() <= 100,
                "argument `{}` of command `{}`: description over Discord's 100-character cap: {:?}",
                argument.name,
                descriptor.name,
                argument.description
            );
        }
    }
}
