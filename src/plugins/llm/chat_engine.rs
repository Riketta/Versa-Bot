//! The conversation engine: called per inbound message, under the channel
//! lock, off the pipeline task. Decides capture and trigger, assembles the
//! context, calls the completion port, delivers (and splits) the reply, and
//! records the bot's own turn.
//!
//! Failure policy: every storage/provider failure here is logged and
//! swallowed - a broken history or a down provider must not crash the bot
//! or spam users; the pipeline has already moved on.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::kernel::{
    models::{MessageId, MessagePayload, Origin, OutboundMessage},
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::completion_port::{CompletionRequest, LlmCompletionPort};
use super::conversation::{self, ConversationRecord, RecordRole};
use super::model::{ChannelConfig, ConversationState, NAMESPACE, channel_state_key};
use super::providers::LlmSettings;

pub struct ChatEngine {
    settings: Arc<LlmSettings>,
    completion: Arc<dyn LlmCompletionPort>,
}

impl ChatEngine {
    #[must_use]
    pub fn new(settings: Arc<LlmSettings>, completion: Arc<dyn LlmCompletionPort>) -> Self {
        Self { settings, completion }
    }

    /// Processes one inbound message of an assigned channel. Always
    /// returns normally - failures are logged, never propagated.
    pub async fn handle_message(
        &self,
        origin: &Origin,
        payload: &MessagePayload,
        config: &ChannelConfig,
        services: &KernelServices,
    ) {
        let Some(storage) = &services.guild_storage else {
            return; // DMs cannot have channel config; unreachable via `pre`.
        };
        let channel_id = origin.channel_id.get();

        // History integrity unknown -> skip the message entirely rather
        // than answer from a degraded or duplicated context.
        let Some(state) = self.load_state(storage, channel_id).await else {
            return;
        };
        let mut live = self.load_live_records(storage, state.cutoff_seq).await;

        let reply_to = payload.reply_to.map(MessageId::get);
        if conversation::should_capture(config.capture_mode, payload.mentions_bot, reply_to, &live)
        {
            let record = ConversationRecord {
                message_id: origin.message_id.map(MessageId::get),
                role: RecordRole::User,
                author: payload.author_name.clone(),
                content: payload.content.clone(),
                reply_to,
                captured_at: unix_now(),
            };
            match Self::append_record(storage, &record).await {
                Ok(()) => live.push(record),
                Err(err) => {
                    tracing::warn!(channel = channel_id, %err, "failed to capture message into history");
                }
            }
        }

        if !conversation::should_trigger(payload.mentions_bot, reply_to, &live) {
            return;
        }

        self.answer(origin, config, &state, &live, services).await;
    }

    /// Completes and delivers the answer for a triggering message. The
    /// triggering message is already part of `live`, so the context ends
    /// with what the user just said.
    async fn answer(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        state: &ConversationState,
        live: &[ConversationRecord],
        services: &KernelServices,
    ) {
        let channel_id = origin.channel_id.get();
        let depth = usize::try_from(config.history_depth).unwrap_or(usize::MAX);
        let window: &[ConversationRecord] = if live.len() > depth {
            let (_, tail) = live.split_at(live.len() - depth);
            tail
        } else {
            live
        };

        let request = CompletionRequest {
            model: config.model.clone(),
            messages: conversation::assemble_context(config, &self.settings, state, window),
            params: config.params.clone(),
        };
        let response = match self.completion.complete(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(channel = channel_id, model = %config.model, %err, "LLM completion failed - no reply sent");
                return;
            }
        };

        let max_length = config.max_length.unwrap_or(self.settings.max_message_length);
        let chunks = conversation::split_reply(&response.content, max_length);
        if chunks.is_empty() {
            tracing::warn!(channel = channel_id, "completion returned no deliverable content");
            return;
        }

        // The streaming port doubles as the plain reply path: `begin`
        // delivers the first chunk and yields the platform message id the
        // assistant record needs for reply-chain detection.
        let stream = services.chat_output_factory.stream_output(origin);
        let mut first_message_id: Option<u64> = None;
        if let Some(first) = chunks.first() {
            match stream.begin(OutboundMessage::text(first.clone())).await {
                Ok(id) => first_message_id = Some(id.get()),
                Err(err) => {
                    tracing::warn!(channel = channel_id, %err, "stream begin failed - falling back to plain sends");
                }
            }
        }

        let delivered_first = usize::from(first_message_id.is_some());
        for chunk in chunks.iter().skip(delivered_first) {
            if let Err(err) = services.chat_output.send(OutboundMessage::text(chunk.clone())).await
            {
                tracing::warn!(channel = channel_id, %err, "failed to deliver reply part");
            }
        }

        let assistant = ConversationRecord {
            message_id: first_message_id,
            role: RecordRole::Assistant,
            author: None,
            content: response.content,
            reply_to: None,
            captured_at: unix_now(),
        };
        if let Some(storage) = &services.guild_storage
            && let Err(err) = Self::append_record(storage, &assistant).await
        {
            tracing::warn!(channel = channel_id, %err, "failed to record bot turn into history");
        }
    }

    /// `None` = the state document is unreadable (storage error): the
    /// caller skips the message instead of answering from an unknown
    /// history. A malformed document resets to a fresh window - better
    /// than bricking the channel until an admin intervenes.
    async fn load_state(
        &self,
        storage: &Arc<dyn GuildStorage>,
        channel_id: u64,
    ) -> Option<ConversationState> {
        match storage.get(NAMESPACE, &channel_state_key(channel_id)).await {
            Ok(Some(raw)) => Some(
                serde_json::from_value::<ConversationState>(raw)
                    .inspect_err(|_| {
                        tracing::warn!(
                            channel = channel_id,
                            "llm conversation state is malformed - starting fresh"
                        );
                    })
                    .unwrap_or_default(),
            ),
            Ok(None) => Some(ConversationState::default()),
            Err(err) => {
                tracing::warn!(channel = channel_id, %err, "llm conversation state unreadable - skipping message");
                None
            }
        }
    }

    async fn load_live_records(
        &self,
        storage: &Arc<dyn GuildStorage>,
        after_seq: u64,
    ) -> Vec<ConversationRecord> {
        let total = storage.count_after(NAMESPACE, after_seq).await.unwrap_or(0);
        if total == 0 {
            return Vec::new();
        }
        let limit = u32::try_from(total).unwrap_or(u32::MAX);
        let stored = storage.list_after(NAMESPACE, after_seq, limit).await.unwrap_or_default();
        stored
            .into_iter()
            .filter_map(|record| {
                serde_json::from_value::<ConversationRecord>(record.payload)
                    .inspect_err(|_| tracing::warn!("skipping malformed conversation record"))
                    .ok()
            })
            .collect()
    }

    async fn append_record(
        storage: &Arc<dyn GuildStorage>,
        record: &ConversationRecord,
    ) -> Result<(), crate::kernel::models::StorageError> {
        let payload = serde_json::to_value(record)
            .map_err(|err| crate::kernel::models::StorageError::Serialization(err.to_string()))?;
        storage.append(NAMESPACE, payload).await.map(|_| ())
    }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{ChannelId, GuildId, MessageId, OutboundError, Platform, UserId};
    use crate::kernel::spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, StoragePort,
    };
    use crate::plugins::llm::completion_port::{ChatRole, CompletionResponse, LlmError};
    use crate::plugins::llm::model::{CaptureMode, channel_config_key};
    use crate::test_support::{InMemoryStorage, RecordingChatOutput};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct FakeCompletion {
        responses: Mutex<Vec<Result<String, LlmError>>>,
        requests: Mutex<Vec<CompletionRequest>>,
    }

    impl FakeCompletion {
        fn requests(&self) -> Vec<CompletionRequest> {
            self.requests.lock().clone()
        }
    }

    #[async_trait]
    impl LlmCompletionPort for FakeCompletion {
        async fn complete(
            &self,
            request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            self.requests.lock().push(request);
            self.responses.lock().pop().map_or_else(
                || Ok(CompletionResponse { content: "canned".to_owned() }),
                |response| response.map(|content| CompletionResponse { content }),
            )
        }
    }

    /// Factory whose stream port succeeds - mirrors the Discord adapter's
    /// `begin`-returns-a-handle behavior the engine relies on.
    struct StreamRecordingFactory {
        begins: Arc<Mutex<Vec<String>>>,
        output: Arc<RecordingChatOutput>,
    }

    impl ChatOutputFactoryPort for StreamRecordingFactory {
        fn chat_output(&self, _origin: &Origin) -> Arc<dyn ChatOutputPort> {
            Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
        }

        fn channel_output(
            &self,
            _origin: &Origin,
            _channel_id: ChannelId,
        ) -> Arc<dyn ChatOutputPort> {
            Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
        }

        fn stream_output(&self, _origin: &Origin) -> Arc<dyn ChatStreamPort> {
            Arc::new(RecordingStream {
                begins: Arc::clone(&self.begins),
                counter: AtomicU64::new(100),
            })
        }
    }

    struct RecordingStream {
        begins: Arc<Mutex<Vec<String>>>,
        counter: AtomicU64,
    }

    #[async_trait]
    impl ChatStreamPort for RecordingStream {
        async fn begin(&self, message: OutboundMessage) -> Result<MessageId, OutboundError> {
            self.begins.lock().push(message.content);
            Ok(MessageId(self.counter.fetch_add(1, Ordering::Relaxed)))
        }

        async fn update(&self, _message: MessageId, _content: String) -> Result<(), OutboundError> {
            Ok(())
        }
    }

    struct TestCtx {
        engine: ChatEngine,
        fake: Arc<FakeCompletion>,
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        begins: Arc<Mutex<Vec<String>>>,
        services: KernelServices,
    }

    fn ctx(responses: Vec<Result<String, LlmError>>) -> TestCtx {
        let settings = Arc::new(LlmSettings::default());
        let fake = Arc::new(FakeCompletion {
            responses: Mutex::new(responses),
            requests: Mutex::new(Vec::new()),
        });
        let engine =
            ChatEngine::new(Arc::clone(&settings), Arc::clone(&fake) as Arc<dyn LlmCompletionPort>);
        let storage = Arc::new(InMemoryStorage::new());
        let output = RecordingChatOutput::new();
        let begins = Arc::new(Mutex::new(Vec::new()));
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(StreamRecordingFactory {
                begins: Arc::clone(&begins),
                output: Arc::clone(&output),
            }),
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        };
        TestCtx { engine, fake, storage, output, begins, services }
    }

    fn assigned_config() -> ChannelConfig {
        ChannelConfig::assigned("local/gemma".to_owned())
    }

    fn seed_config(storage: &InMemoryStorage, config: &ChannelConfig) {
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_config_key(2),
            serde_json::to_value(config).expect("config expected to serialize"),
        );
    }

    fn origin() -> Origin {
        Origin {
            platform: Platform::Discord,
            guild_id: Some(GuildId(1)),
            channel_id: ChannelId(2),
            user_id: UserId(3),
            message_id: Some(MessageId(77)),
            reply_token: None,
        }
    }

    fn payload(mentions_bot: bool, reply_to: Option<u64>) -> MessagePayload {
        MessagePayload {
            content: "hello bot".to_owned(),
            author_name: Some("alice".to_owned()),
            author_roles: Vec::new(),
            author_permissions: 0,
            reply_to: reply_to.map(MessageId),
            mentions_bot,
        }
    }

    fn user_record(message_id: u64, author: &str, content: &str) -> ConversationRecord {
        ConversationRecord {
            message_id: Some(message_id),
            role: RecordRole::User,
            author: Some(author.to_owned()),
            content: content.to_owned(),
            reply_to: None,
            captured_at: 0,
        }
    }

    async fn append_record(storage: &Arc<InMemoryStorage>, record: &ConversationRecord) {
        let payload = serde_json::to_value(record).expect("record expected to serialize");
        storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .append(NAMESPACE, payload)
            .await
            .expect("append expected to succeed");
    }

    async fn stored_records(ctx: &TestCtx) -> Vec<ConversationRecord> {
        ctx.storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .list_after(NAMESPACE, 0, 100)
            .await
            .expect("records readable")
            .into_iter()
            .map(|stored| {
                serde_json::from_value::<ConversationRecord>(stored.payload)
                    .expect("record expected to deserialize")
            })
            .collect()
    }

    #[tokio::test]
    async fn mention_triggers_reply_and_self_recording() {
        let ctx = ctx(vec![Ok("hi alice".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        // Context sent to the provider: system, placeholder, the user turn.
        let requests = ctx.fake.requests();
        assert_eq!(requests.len(), 1);
        let request = requests.first().expect("one request expected");
        assert_eq!(request.model, "local/gemma");
        assert_eq!(request.messages.len(), 3);
        assert_eq!(request.messages.first().map(|m| m.role), Some(ChatRole::System));
        assert_eq!(
            request.messages.get(1).map(|m| m.content.as_str()),
            Some(conversation::NO_EARLIER_CONTEXT)
        );
        assert_eq!(request.messages.get(2).map(|m| m.content.as_str()), Some("alice: hello bot"));

        // Reply delivered via the stream begin; assistant turn recorded with
        // the returned handle for future reply-chain detection.
        assert_eq!(ctx.begins.lock().clone(), vec!["hi alice".to_owned()]);
        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 2);
        assert_eq!(
            records.first().map(|r| (r.role, r.message_id)),
            Some((RecordRole::User, Some(77)))
        );
        let assistant = records.get(1).expect("assistant record expected");
        assert_eq!(assistant.role, RecordRole::Assistant);
        assert!(assistant.message_id.is_some());
        assert_eq!(assistant.content, "hi alice");
    }

    #[tokio::test]
    async fn unrelated_message_is_ignored_in_bot_related_mode() {
        let ctx = ctx(vec![]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &assigned_config(), &ctx.services)
            .await;

        assert!(ctx.fake.requests().is_empty());
        assert!(ctx.begins.lock().is_empty());
        assert!(stored_records(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn all_messages_mode_captures_without_triggering() {
        let ctx = ctx(vec![]);
        let config = ChannelConfig { capture_mode: CaptureMode::AllMessages, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(false, None), &config, &ctx.services).await;

        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().map(|r| r.role), Some(RecordRole::User));
        assert!(ctx.fake.requests().is_empty());
        assert!(ctx.begins.lock().is_empty());
    }

    #[tokio::test]
    async fn reply_chain_into_conversation_captures_and_reply_to_bot_triggers() {
        let ctx = ctx(vec![Ok("answering".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());
        append_record(&ctx.storage, &user_record(10, "alice", "hi")).await;
        append_record(
            &ctx.storage,
            &ConversationRecord {
                message_id: Some(11),
                role: RecordRole::Assistant,
                author: None,
                content: "hello!".to_owned(),
                reply_to: None,
                captured_at: 0,
            },
        )
        .await;

        // Bob replies to the bot's message (id 11): no mention, still triggers.
        ctx.engine
            .handle_message(&origin(), &payload(false, Some(11)), &assigned_config(), &ctx.services)
            .await;

        assert_eq!(ctx.fake.requests().len(), 1);
        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 4);
        assert_eq!(
            records.get(2).map(|r| (r.role, r.reply_to)),
            Some((RecordRole::User, Some(11)))
        );
        assert_eq!(records.get(3).map(|r| r.role), Some(RecordRole::Assistant));
    }

    #[tokio::test]
    async fn completion_failure_sends_nothing_and_records_no_bot_turn() {
        let ctx = ctx(vec![Err(LlmError::Request("provider down".to_owned()))]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        assert!(ctx.begins.lock().is_empty());
        assert!(ctx.output.messages().is_empty());
        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().map(|r| r.role), Some(RecordRole::User));
    }

    #[tokio::test]
    async fn oversized_replies_split_across_messages() {
        let ctx = ctx(vec![Ok("aaa\nbbb".to_owned())]);
        let config = ChannelConfig { max_length: Some(5), ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        // First chunk via begin, remainder via plain sends.
        assert_eq!(ctx.begins.lock().clone(), vec!["aaa".to_owned()]);
        assert_eq!(ctx.output.messages(), vec!["bbb".to_owned()]);
        // The assistant record keeps the FULL text.
        let records = stored_records(&ctx).await;
        let assistant = records.get(1).expect("assistant record expected");
        assert_eq!(assistant.content, "aaa\nbbb");
    }

    #[tokio::test]
    async fn summary_flows_into_the_context_slot() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());
        ctx.storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_state_key(2),
            serde_json::json!({"summary": "the gist", "cutoff_seq": 0}),
        );

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        assert_eq!(
            request.messages.get(1).map(|m| m.content.as_str()),
            Some("Earlier conversation summary:\nthe gist")
        );
    }

    #[tokio::test]
    async fn history_depth_clamps_the_window() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        let config = ChannelConfig { history_depth: 2, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        // Fixed slots (system + summary) plus the newest 2 turns only.
        assert_eq!(request.messages.len(), 4);
        assert_eq!(request.messages.get(2).map(|m| m.content.as_str()), Some("a3: m3"));
        assert_eq!(request.messages.get(3).map(|m| m.content.as_str()), Some("alice: hello bot"));
    }

    #[tokio::test]
    async fn unreadable_state_skips_the_message() {
        let settings = Arc::new(LlmSettings::default());
        let fake = Arc::new(FakeCompletion {
            responses: Mutex::new(vec![]),
            requests: Mutex::new(Vec::new()),
        });
        let engine = ChatEngine::new(settings, Arc::clone(&fake) as Arc<dyn LlmCompletionPort>);
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(crate::test_support::RecordingChatOutputFactory::new(
                Arc::clone(&output),
            )),
            guild_storage: Some(
                crate::test_support::FailingStorage.guild_scoped(Platform::Discord, GuildId(1)),
            ),
        };

        engine.handle_message(&origin(), &payload(true, None), &assigned_config(), &services).await;

        assert!(fake.requests().is_empty());
        assert!(output.messages().is_empty());
    }
}
