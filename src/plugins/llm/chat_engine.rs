//! The conversation engine: called per inbound message, under the channel
//! lock, off the pipeline task. Decides capture and trigger, assembles the
//! context, calls the completion port, delivers (and splits) the reply,
//! records the bot's own turn, and keeps the window compacted.
//!
//! Failure policy: every storage/provider failure here is logged and
//! swallowed - a broken history or a down provider must not crash the bot
//! or spam users; the pipeline has already moved on. Service-visible
//! failures (completion, compaction) additionally reach the guild's
//! configured service channel, with error notices rate-limited so an
//! outage cannot spam it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::kernel::{
    models::{ChannelId, Embed, GuildId, MessageId, MessagePayload, Origin, OutboundMessage},
    services::KernelServices,
    spi_ports::{ChatStreamPort, GuildStorage},
};

use super::completion_port::{CompletionRequest, LlmCompletionPort, TokenUsage};
use super::conversation::{self, ConversationRecord, RecordRole};
use super::model::{
    ChannelConfig, ConversationState, GenParams, NAMESPACE, SERVICE_CHANNEL_KEY, UsageStats,
    blend_ratio, channel_state_key, channel_stats_key, records_namespace, unix_now,
};
use super::providers::LlmSettings;
use super::rng::{RandomPort, RandomScope};

/// Identifies one channel for service-notice cooldowns: platform, guild,
/// channel.
type ChannelKey = (String, u64, u64);

/// Minimum interval between error notices for the same channel: a down
/// provider must not turn every triggering message into an admin ping.
const NOTICE_COOLDOWN: Duration = Duration::from_secs(300);

/// Minimum interval between random chime-ins for the same channel: the deck
/// already prevents statistical clumping, this prevents two chime-ins on
/// consecutive messages.
const CHIME_COOLDOWN: Duration = Duration::from_secs(300);

/// Upper bound of in-place edits while progressively revealing an answer;
/// the reveal step derives from the content length, so this bounds the whole
/// reveal regardless of `max_length`.
const MAX_STREAM_EDITS: usize = 10;

pub struct ChatEngine {
    settings: Arc<LlmSettings>,
    completion: Arc<dyn LlmCompletionPort>,
    rng: Arc<dyn RandomPort>,
    /// Last error-notice time per channel (see [`NOTICE_COOLDOWN`]).
    notices: Mutex<HashMap<ChannelKey, Instant>>,
    /// Last random chime-in time per channel (see [`CHIME_COOLDOWN`]).
    chimes: Mutex<HashMap<ChannelKey, Instant>>,
}

impl ChatEngine {
    #[must_use]
    pub fn new(
        settings: Arc<LlmSettings>,
        completion: Arc<dyn LlmCompletionPort>,
        rng: Arc<dyn RandomPort>,
    ) -> Self {
        Self {
            settings,
            completion,
            rng,
            notices: Mutex::new(HashMap::new()),
            chimes: Mutex::new(HashMap::new()),
        }
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
        let usage_stats = self.load_stats(storage, channel_id).await;
        let calibrated = usage_stats.last.is_some();
        let mut live = self.load_live_records(storage, channel_id, state.cutoff_seq).await;

        let reply_to = payload.reply_to.map(MessageId::get);
        let mut live_records: Vec<ConversationRecord> =
            live.iter().map(|(_, record)| record.clone()).collect();
        let mut captured = false;
        if conversation::should_capture(
            config.capture_mode,
            payload.mentions_bot,
            reply_to,
            &live_records,
        ) {
            let record = ConversationRecord {
                message_id: origin.message_id.map(MessageId::get),
                role: RecordRole::User,
                author: payload.author_name.clone(),
                content: payload.content.clone(),
                reply_to,
                captured_at: unix_now(),
            };
            match Self::append_record(storage, channel_id, &record).await {
                Ok(seq) => {
                    live.push((seq, record.clone()));
                    // The triggering message must be part of the context.
                    live_records.push(record);
                    captured = true;
                }
                Err(err) => {
                    tracing::warn!(channel = channel_id, %err, "failed to capture message into history");
                }
            }
        }

        if conversation::should_trigger(payload.mentions_bot, reply_to, &live_records) {
            self.answer(
                origin,
                config,
                &state,
                &live_records,
                usage_stats.tokens_per_char,
                calibrated,
                services,
            )
            .await;
        } else if captured
            && config.random_chance_percent > 0.0
            && self.chime_allowed(origin)
            && self.rng.chance_percent(
                RandomScope {
                    platform: origin.platform.as_str(),
                    guild_id: origin.guild_id.map_or(0, GuildId::get),
                    channel_id: origin.channel_id.get(),
                },
                config.random_chance_percent,
            )
        {
            // Random chime-in: same delivery path as a mention reply - the
            // answer is assembled from the context the message just joined
            // and recorded as an assistant turn. Only CAPTURED messages are
            // eligible: chiming in on a message the bot never tracked would
            // look like answering nothing.
            self.note_chime(origin);
            self.answer(
                origin,
                config,
                &state,
                &live_records,
                usage_stats.tokens_per_char,
                calibrated,
                services,
            )
            .await;
        }

        // Compaction runs after the reply (the triggering turn used the
        // pre-compaction context) and after every capture, so all-messages
        // channels compact too - not just chatty ones.
        self.maybe_compact(
            origin,
            config,
            &state,
            &live,
            usage_stats.tokens_per_char,
            calibrated,
            services,
        )
        .await;
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
        tokens_per_char: f64,
        calibrated: bool,
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
        let budget = conversation::resolve_budget(config, &self.settings, calibrated);

        let messages = conversation::assemble_context(
            config,
            &self.settings,
            state,
            window,
            tokens_per_char,
            budget,
        );
        // The endpoint's usage report is measured against exactly this
        // context, so the ratio calibration compares like with like.
        #[allow(clippy::cast_precision_loss)] // estimator: precision loss is fine
        let context_chars: u64 =
            messages.iter().map(|message| message.content.chars().count() as u64).sum();
        let request = CompletionRequest {
            model: config.model.clone(),
            messages,
            params: config.params.clone(),
        };
        let response = match self.completion.complete(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(channel = channel_id, model = %config.model, %err, "LLM completion failed - no reply sent");
                if let Some(storage) = &services.guild_storage {
                    self.notify_service(
                        services,
                        origin,
                        storage,
                        "LLM completion failed",
                        format!("Model `{}`: {err}\nNo reply was sent.", config.model),
                        true,
                    )
                    .await;
                }
                return;
            }
        };
        self.record_usage(
            services,
            channel_id,
            response.usage,
            context_chars,
            tokens_per_char,
            budget,
        )
        .await;

        let max_length = config.max_length.unwrap_or(self.settings.max_message_length);
        let chunks = conversation::split_reply(&response.content, max_length);
        if chunks.is_empty() {
            tracing::warn!(channel = channel_id, "completion returned no deliverable content");
            return;
        }

        // The streaming port doubles as the plain reply path: `begin`
        // delivers the first chunk and yields the platform message id the
        // assistant record needs for reply-chain detection. Streaming mode
        // starts from a placeholder and reveals the first chunk in place;
        // plain mode delivers the full first chunk immediately.
        let stream = services.chat_output_factory.stream_output(origin);
        let first_chunk = chunks.first().expect("non-empty chunks checked").clone();
        let placeholder = if config.streaming { "…" } else { first_chunk.as_str() };
        let mut first_message_id: Option<u64> = None;
        match stream.begin(OutboundMessage::text(placeholder.to_owned())).await {
            Ok(id) => first_message_id = Some(id.get()),
            Err(err) => {
                tracing::warn!(channel = channel_id, %err, "stream begin failed - falling back to plain sends");
            }
        }

        if let (true, Some(id)) = (config.streaming, first_message_id) {
            self.reveal_progressively(&stream, MessageId(id), &first_chunk).await;
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
            && let Err(err) = Self::append_record(storage, channel_id, &assistant).await
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

    /// The channel's live window: `(seq, record)` pairs after the cutoff,
    /// ascending. Malformed records are skipped (with a warning) instead of
    /// failing the whole window.
    async fn load_live_records(
        &self,
        storage: &Arc<dyn GuildStorage>,
        channel_id: u64,
        after_seq: u64,
    ) -> Vec<(u64, ConversationRecord)> {
        let records_ns = records_namespace(channel_id);
        let total = storage.count_after(&records_ns, after_seq).await.unwrap_or(0);
        if total == 0 {
            return Vec::new();
        }
        let limit = u32::try_from(total).unwrap_or(u32::MAX);
        let stored = storage.list_after(&records_ns, after_seq, limit).await.unwrap_or_default();
        stored
            .into_iter()
            .filter_map(|record| {
                serde_json::from_value::<ConversationRecord>(record.payload)
                    .inspect_err(|_| tracing::warn!("skipping malformed conversation record"))
                    .ok()
                    .map(|parsed| (record.seq, parsed))
            })
            .collect()
    }

    async fn append_record(
        storage: &Arc<dyn GuildStorage>,
        channel_id: u64,
        record: &ConversationRecord,
    ) -> Result<u64, crate::kernel::models::StorageError> {
        let payload = serde_json::to_value(record)
            .map_err(|err| crate::kernel::models::StorageError::Serialization(err.to_string()))?;
        storage.append(&records_namespace(channel_id), payload).await
    }

    /// Folds the oldest live messages into the summary once the window
    /// outgrows `history_depth`: everything but `compaction_keep_tail`
    /// newest records is summarized (via the compaction model) and the
    /// cutoff advances past them. Runs AFTER the reply - the triggering
    /// turn used the pre-compaction context. The commit is one document
    /// write: crash mid-way leaves the old state intact. Compaction is
    /// never a sliding window - the prompt prefix stays byte-stable
    /// between compactions, so provider prompt caches stay warm.
    async fn maybe_compact(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        state: &ConversationState,
        live: &[(u64, ConversationRecord)],
        tokens_per_char: f64,
        calibrated: bool,
        services: &KernelServices,
    ) {
        if !config.compaction_enabled {
            return;
        }
        let channel_id = origin.channel_id.get();
        let depth = usize::try_from(config.history_depth).unwrap_or(usize::MAX);
        if live.len() <= depth {
            return;
        }
        let keep_tail = usize::try_from(self.settings.compaction_keep_tail).unwrap_or(usize::MAX);
        if keep_tail == 0 || keep_tail >= live.len() {
            tracing::warn!(
                channel = channel_id,
                keep_tail = self.settings.compaction_keep_tail,
                live = live.len(),
                "compaction would make no progress - skipping"
            );
            return;
        }

        let (chunk, _tail) = live.split_at(live.len() - keep_tail);
        let chunk_end_seq = chunk.last().map_or(0, |(seq, _)| *seq);
        let chunk_records: Vec<ConversationRecord> =
            chunk.iter().map(|(_, record)| record.clone()).collect();
        let prompt = config
            .compaction_prompt
            .clone()
            .unwrap_or_else(|| self.settings.default_compaction_prompt.clone());
        let model = config
            .compaction_model
            .clone()
            .or_else(|| self.settings.compaction_model.clone())
            .unwrap_or_else(|| config.model.clone());

        let messages =
            conversation::compaction_input(&prompt, state.summary.as_deref(), &chunk_records);
        #[allow(clippy::cast_precision_loss)] // estimator: precision loss is fine
        let context_chars: u64 =
            messages.iter().map(|message| message.content.chars().count() as u64).sum();
        let request = CompletionRequest {
            model,
            messages,
            // Summarization needs no sampling tuning - provider defaults.
            params: GenParams::default(),
        };
        let response = match self.completion.complete(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(channel = channel_id, %err, "LLM compaction failed - window keeps growing");
                if let Some(storage) = &services.guild_storage {
                    self.notify_service(
                        services,
                        origin,
                        storage,
                        "LLM compaction failed",
                        format!("{err}\nThe context window keeps growing until this succeeds."),
                        true,
                    )
                    .await;
                }
                return;
            }
        };
        self.record_usage(
            services,
            channel_id,
            response.usage,
            context_chars,
            tokens_per_char,
            conversation::resolve_budget(config, &self.settings, calibrated),
        )
        .await;

        let new_state = ConversationState {
            summary: Some(response.content),
            cutoff_seq: chunk_end_seq,
            cutoff_at: Some(unix_now()),
        };
        if let Some(storage) = &services.guild_storage {
            self.commit_compaction(
                origin,
                storage,
                new_state,
                chunk_records.len(),
                keep_tail,
                services,
            )
            .await;
        }
    }

    /// Persists the compacted state atomically (one document write) and
    /// reports the outcome to the service channel.
    async fn commit_compaction(
        &self,
        origin: &Origin,
        storage: &Arc<dyn GuildStorage>,
        new_state: ConversationState,
        folded: usize,
        keep_tail: usize,
        services: &KernelServices,
    ) {
        let channel_id = origin.channel_id.get();
        match storage
            .set(
                NAMESPACE,
                &channel_state_key(channel_id),
                serde_json::to_value(&new_state).unwrap_or(serde_json::Value::Null),
            )
            .await
        {
            Ok(()) => {
                tracing::info!(
                    channel = channel_id,
                    folded,
                    cutoff = new_state.cutoff_seq,
                    "conversation compacted"
                );
                self.notify_service(
                    services,
                    origin,
                    storage,
                    "Conversation compacted",
                    format!(
                        "Folded {folded} messages into the summary; the live window now starts \
                         fresh with {keep_tail} messages kept."
                    ),
                    false,
                )
                .await;
            }
            Err(err) => {
                tracing::warn!(channel = channel_id, %err, "failed to persist compacted state");
                self.notify_service(
                    services,
                    origin,
                    storage,
                    "LLM compaction failed",
                    format!("Could not persist the summary: {err}"),
                    true,
                )
                .await;
            }
        }
    }

    /// Best-effort report to the guild's configured service channel;
    /// without one this is tracing only. Error notices are rate-limited
    /// per channel ([`NOTICE_COOLDOWN`]), success notices are not.
    async fn notify_service(
        &self,
        services: &KernelServices,
        origin: &Origin,
        storage: &Arc<dyn GuildStorage>,
        title: &str,
        description: String,
        is_error: bool,
    ) {
        if is_error && !self.error_notice_allowed(origin) {
            return;
        }
        let raw = match storage.get(NAMESPACE, SERVICE_CHANNEL_KEY).await {
            Ok(Some(raw)) => raw,
            Ok(None) => return, // no service channel: logs only
            Err(err) => {
                tracing::warn!(%err, "service channel config unreadable");
                return;
            }
        };
        let Some(service_channel) = raw.as_str().and_then(|value| value.parse::<u64>().ok()) else {
            tracing::warn!("service channel config is malformed");
            return;
        };
        let output =
            services.chat_output_factory.channel_output(origin, ChannelId(service_channel));
        let notice = OutboundMessage::embed(Embed { title: title.to_owned(), description });
        if let Err(err) = output.send(notice).await {
            tracing::warn!(%err, "failed to deliver service notice");
        }
    }

    fn error_notice_allowed(&self, origin: &Origin) -> bool {
        let key: ChannelKey = (
            origin.platform.as_str().to_owned(),
            origin.guild_id.map_or(0, GuildId::get),
            origin.channel_id.get(),
        );
        let mut notices = self.notices.lock();
        match notices.get(&key) {
            Some(last) if last.elapsed() < NOTICE_COOLDOWN => false,
            _ => {
                notices.insert(key, Instant::now());
                true
            }
        }
    }

    fn chime_allowed(&self, origin: &Origin) -> bool {
        let key: ChannelKey = (
            origin.platform.as_str().to_owned(),
            origin.guild_id.map_or(0, GuildId::get),
            origin.channel_id.get(),
        );
        !matches!(self.chimes.lock().get(&key), Some(last) if last.elapsed() < CHIME_COOLDOWN)
    }

    fn note_chime(&self, origin: &Origin) {
        let key: ChannelKey = (
            origin.platform.as_str().to_owned(),
            origin.guild_id.map_or(0, GuildId::get),
            origin.channel_id.get(),
        );
        self.chimes.lock().insert(key, Instant::now());
    }

    /// Persists the last completion's token usage and blends the observed
    /// tokens-per-character ratio into the channel's estimate. Best effort:
    /// a failed write only degrades the next estimate back to the previous
    /// ratio; endpoints without usage stats write nothing at all.
    async fn record_usage(
        &self,
        services: &KernelServices,
        channel_id: u64,
        usage: Option<TokenUsage>,
        context_chars: u64,
        tokens_per_char: f64,
        budget: Option<u64>,
    ) {
        let Some(usage) = usage else {
            return; // endpoint does not report usage: nothing to record
        };
        let Some(storage) = &services.guild_storage else {
            return;
        };
        let stats = UsageStats {
            last: Some(usage),
            tokens_per_char: blend_ratio(tokens_per_char, usage.prompt_tokens, context_chars),
            last_budget: budget,
        };
        if let Err(err) = storage
            .set(
                NAMESPACE,
                &channel_stats_key(channel_id),
                serde_json::to_value(&stats).unwrap_or(serde_json::Value::Null),
            )
            .await
        {
            tracing::warn!(channel = channel_id, %err, "failed to persist token usage stats");
        }
    }

    /// The channel's calibration state; unreadable stats fall back to the
    /// default ratio (stats are observability, never reply-blocking).
    async fn load_stats(&self, storage: &Arc<dyn GuildStorage>, channel_id: u64) -> UsageStats {
        storage
            .get(NAMESPACE, &channel_stats_key(channel_id))
            .await
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_value(raw).ok())
            .unwrap_or_default()
    }

    /// Reveals `content` in place on `message`, at the configured cadence,
    /// in at most [`MAX_STREAM_EDITS`] edits - the last one carries the
    /// exact full text. Failed updates are logged and skipped: the next
    /// tick re-sends a longer prefix, so a transient edit failure heals
    /// itself; the message is only ever behind, never wrong.
    async fn reveal_progressively(
        &self,
        stream: &Arc<dyn ChatStreamPort>,
        message: MessageId,
        content: &str,
    ) {
        let interval = Duration::from_millis(self.settings.stream_interval_ms.max(1));
        let total = content.chars().count();
        let step = total.div_ceil(MAX_STREAM_EDITS).max(1);
        let mut revealed = 0;
        while revealed < total {
            tokio::time::sleep(interval).await;
            revealed = (revealed + step).min(total);
            let text: String = content.chars().take(revealed).collect();
            if let Err(err) = stream.update(message, text).await {
                tracing::warn!(%err, "streaming update failed - continuing");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{ChannelId, GuildId, MessageId, OutboundError, Platform, UserId};
    use crate::kernel::spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, StoragePort,
    };
    use crate::plugins::llm::completion_port::{
        ChatRole, CompletionResponse, LlmError, TokenUsage,
    };
    use crate::plugins::llm::model::{CaptureMode, channel_config_key};
    use crate::plugins::llm::rng::RandRandom;
    use crate::test_support::{InMemoryStorage, RecordingChatOutput};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct FakeCompletion {
        responses: Mutex<Vec<Result<String, LlmError>>>,
        requests: Mutex<Vec<CompletionRequest>>,
        usage: Mutex<Option<TokenUsage>>,
    }

    impl FakeCompletion {
        fn requests(&self) -> Vec<CompletionRequest> {
            self.requests.lock().clone()
        }

        fn set_usage(&self, usage: Option<TokenUsage>) {
            *self.usage.lock() = usage;
        }
    }

    #[async_trait]
    impl LlmCompletionPort for FakeCompletion {
        async fn complete(
            &self,
            request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            self.requests.lock().push(request);
            let usage = self.usage.lock().to_owned();
            self.responses.lock().pop().map_or_else(
                || Ok(CompletionResponse { content: "canned".to_owned(), usage }),
                |response| response.map(|content| CompletionResponse { content, usage }),
            )
        }
    }

    /// Factory whose stream port succeeds - mirrors the Discord adapter's
    /// `begin`-returns-a-handle behavior the engine relies on.
    struct StreamRecordingFactory {
        begins: Arc<Mutex<Vec<String>>>,
        updates: Arc<Mutex<Vec<String>>>,
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
                updates: Arc::clone(&self.updates),
                counter: AtomicU64::new(100),
            })
        }

        fn message_link(
            &self,
            _origin: &Origin,
            channel_id: ChannelId,
            message_id: MessageId,
        ) -> Option<String> {
            Some(format!("link://{}/{}", channel_id.get(), message_id.get()))
        }
    }

    struct RecordingStream {
        begins: Arc<Mutex<Vec<String>>>,
        updates: Arc<Mutex<Vec<String>>>,
        counter: AtomicU64,
    }

    #[async_trait]
    impl ChatStreamPort for RecordingStream {
        async fn begin(&self, message: OutboundMessage) -> Result<MessageId, OutboundError> {
            self.begins.lock().push(message.content);
            Ok(MessageId(self.counter.fetch_add(1, Ordering::Relaxed)))
        }

        async fn update(&self, _message: MessageId, content: String) -> Result<(), OutboundError> {
            self.updates.lock().push(content);
            Ok(())
        }
    }

    /// Deterministic RNG for chime-in tests.
    struct FixedRandom(bool);

    #[async_trait]
    impl RandomPort for FixedRandom {
        fn chance_percent(&self, _scope: RandomScope, _percent: f64) -> bool {
            self.0
        }
    }

    struct TestCtx {
        engine: ChatEngine,
        fake: Arc<FakeCompletion>,
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        begins: Arc<Mutex<Vec<String>>>,
        updates: Arc<Mutex<Vec<String>>>,
        services: KernelServices,
    }

    fn ctx(responses: Vec<Result<String, LlmError>>) -> TestCtx {
        ctx_random(LlmSettings::default(), Arc::new(RandRandom), responses)
    }

    fn ctx_with(settings: LlmSettings, responses: Vec<Result<String, LlmError>>) -> TestCtx {
        ctx_random(settings, Arc::new(RandRandom), responses)
    }

    fn ctx_random(
        settings: LlmSettings,
        rng: Arc<dyn RandomPort>,
        responses: Vec<Result<String, LlmError>>,
    ) -> TestCtx {
        let settings = Arc::new(settings);
        let fake = Arc::new(FakeCompletion {
            responses: Mutex::new(responses),
            requests: Mutex::new(Vec::new()),
            usage: Mutex::new(None),
        });
        let engine = ChatEngine::new(
            Arc::clone(&settings),
            Arc::clone(&fake) as Arc<dyn LlmCompletionPort>,
            rng,
        );
        let storage = Arc::new(InMemoryStorage::new());
        let output = RecordingChatOutput::new();
        let begins = Arc::new(Mutex::new(Vec::new()));
        let updates = Arc::new(Mutex::new(Vec::new()));
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(StreamRecordingFactory {
                begins: Arc::clone(&begins),
                updates: Arc::clone(&updates),
                output: Arc::clone(&output),
            }),
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        };
        TestCtx { engine, fake, storage, output, begins, updates, services }
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
            .append(&records_namespace(2), payload)
            .await
            .expect("append expected to succeed");
    }

    async fn stored_records(ctx: &TestCtx) -> Vec<ConversationRecord> {
        ctx.storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .list_after(&records_namespace(2), 0, 100)
            .await
            .expect("records readable")
            .into_iter()
            .map(|stored| {
                serde_json::from_value::<ConversationRecord>(stored.payload)
                    .expect("record expected to deserialize")
            })
            .collect()
    }

    fn seed_service_channel(ctx: &TestCtx) {
        ctx.storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            SERVICE_CHANNEL_KEY,
            serde_json::json!("9"),
        );
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
            usage: Mutex::new(None),
        });
        let engine = ChatEngine::new(
            settings,
            Arc::clone(&fake) as Arc<dyn LlmCompletionPort>,
            Arc::new(RandRandom) as Arc<dyn RandomPort>,
        );
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

    #[tokio::test]
    async fn compaction_runs_after_reply_when_window_outgrows_depth() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx =
            ctx_with(settings, vec![Ok("summary text".to_owned()), Ok("the answer".to_owned())]);
        let config = ChannelConfig { history_depth: 3, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        // Chat completion first, compaction second; the chunk holds only the
        // oldest two records (keep_tail = 2 keeps the newest pair).
        let requests = ctx.fake.requests();
        assert_eq!(requests.len(), 2);
        let transcript = requests
            .get(1)
            .and_then(|request| request.messages.get(1))
            .map(|message| message.content.clone())
            .expect("compaction transcript expected");
        assert!(transcript.contains("a1: m1"));
        assert!(transcript.contains("a2: m2"));
        assert!(!transcript.contains("a3: m3"));

        let state_raw = ctx
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable")
            .expect("compacted state expected");
        let state: ConversationState =
            serde_json::from_value(state_raw).expect("state expected to deserialize");
        assert_eq!(state.summary.as_deref(), Some("summary text"));
        assert_eq!(state.cutoff_seq, 2);
        assert!(state.cutoff_at.is_some());

        // The reply went out and all records (seeded + capture + assistant)
        // are kept.
        assert_eq!(ctx.begins.lock().clone(), vec!["the answer".to_owned()]);
        assert_eq!(stored_records(&ctx).await.len(), 5);
    }

    #[tokio::test]
    async fn compaction_failure_keeps_state_and_reports_to_service_channel() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx = ctx_with(
            settings,
            vec![Err(LlmError::Request("summarizer down".to_owned())), Ok("the answer".to_owned())],
        );
        let config = ChannelConfig { history_depth: 3, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        seed_service_channel(&ctx);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        // The reply went out; the compaction failure left the state intact.
        assert_eq!(ctx.begins.lock().clone(), vec!["the answer".to_owned()]);
        let state_raw = ctx
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable");
        assert_eq!(state_raw, None);
        assert!(ctx.output.messages().iter().any(|message| message.contains("compaction failed")));
    }

    #[tokio::test]
    async fn error_notices_are_rate_limited_per_channel() {
        let ctx = ctx(vec![Err(LlmError::Request("down".to_owned())); 5]);
        seed_config(&ctx.storage, &assigned_config());
        seed_service_channel(&ctx);

        for _ in 0..3 {
            ctx.engine
                .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
                .await;
        }

        // Three failed completions - one rate-limited error embed.
        assert_eq!(ctx.output.messages().len(), 1);
    }

    #[tokio::test]
    async fn compaction_disabled_lets_the_window_grow() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx = ctx_with(settings, vec![Ok("the answer".to_owned())]);
        let config =
            ChannelConfig { history_depth: 1, compaction_enabled: false, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        // Only the chat completion ran; no state was ever written.
        assert_eq!(ctx.fake.requests().len(), 1);
        let state_raw = ctx
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable");
        assert_eq!(state_raw, None);
        // Seeded record + capture + the assistant turn.
        assert_eq!(stored_records(&ctx).await.len(), 3);
    }

    #[tokio::test]
    async fn random_reply_fires_on_captured_non_trigger_message() {
        let ctx = ctx_random(
            LlmSettings::default(),
            Arc::new(FixedRandom(true)),
            vec![Ok("random thought".to_owned())],
        );
        let config = ChannelConfig { capture_mode: CaptureMode::AllMessages, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(false, None), &config, &ctx.services).await;

        assert_eq!(ctx.fake.requests().len(), 1);
        assert_eq!(ctx.begins.lock().clone(), vec!["random thought".to_owned()]);
        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 2);
        assert_eq!(records.get(1).map(|record| record.role), Some(RecordRole::Assistant));
    }

    #[tokio::test]
    async fn random_reply_miss_leaves_only_the_capture() {
        let ctx = ctx_random(LlmSettings::default(), Arc::new(FixedRandom(false)), vec![]);
        let config = ChannelConfig { capture_mode: CaptureMode::AllMessages, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(false, None), &config, &ctx.services).await;

        assert!(ctx.fake.requests().is_empty());
        assert_eq!(stored_records(&ctx).await.len(), 1);
    }

    #[tokio::test]
    async fn random_reply_never_fires_for_uncaptured_messages() {
        // bot_related mode, no mention, no reply link: the message is not
        // even in the context - chiming in would look like answering nothing.
        let ctx = ctx_random(LlmSettings::default(), Arc::new(FixedRandom(true)), vec![]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &assigned_config(), &ctx.services)
            .await;

        assert!(ctx.fake.requests().is_empty());
        assert!(stored_records(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn random_chime_ins_are_cooldown_gated() {
        let ctx = ctx_random(
            LlmSettings::default(),
            Arc::new(FixedRandom(true)),
            vec![Ok("chime".to_owned())],
        );
        let config = ChannelConfig { capture_mode: CaptureMode::AllMessages, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        for _ in 0..2 {
            ctx.engine
                .handle_message(&origin(), &payload(false, None), &config, &ctx.services)
                .await;
        }

        // First message chimes; the second lands inside the cooldown window.
        assert_eq!(ctx.fake.requests().len(), 1);
        // Two captures + one assistant turn.
        assert_eq!(stored_records(&ctx).await.len(), 3);
    }

    #[tokio::test]
    async fn streaming_reveals_progressively_then_finalizes() {
        let settings = LlmSettings { stream_interval_ms: 1, ..LlmSettings::default() };
        let ctx = ctx_with(settings, vec![Ok("answer text".to_owned())]);
        let config = ChannelConfig { streaming: true, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        // Placeholder begin, then prefix reveals ending on the exact chunk.
        assert_eq!(ctx.begins.lock().clone(), vec!["…".to_owned()]);
        let updates = ctx.updates.lock().clone();
        assert_eq!(updates.last().map(String::as_str), Some("answer text"));
        assert!(updates.len() <= MAX_STREAM_EDITS);
        let first = updates.first().expect("at least one update expected");
        assert!(first.chars().count() < "answer text".chars().count());
        // Reveals are prefixes of the final text - never wrong, only behind.
        for update in &updates {
            assert!("answer text".starts_with(update.as_str()));
        }
    }

    #[tokio::test]
    async fn token_usage_is_recorded_and_calibrates_the_estimate() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        ctx.fake.set_usage(Some(TokenUsage {
            prompt_tokens: 1200,
            completion_tokens: 5,
            total_tokens: 1205,
            cached_tokens: None,
        }));
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        let stats_raw = ctx
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, &channel_stats_key(2))
            .await
            .expect("stats readable")
            .expect("stats expected");
        let stats: UsageStats =
            serde_json::from_value(stats_raw).expect("stats expected to deserialize");
        assert_eq!(stats.last.map(|usage| usage.prompt_tokens), Some(1200));
        // 1200 prompt tokens over a tiny default context pins the ratio at
        // the clamp - the estimate reacts strongly to the first observation.
        assert!((stats.tokens_per_char - 2.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn stats_stay_absent_when_endpoint_reports_nothing() {
        let ctx = ctx(vec![Ok("ok".to_owned())]); // no usage reported
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        let stats_raw = ctx
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, &channel_stats_key(2))
            .await
            .expect("stats readable");
        assert_eq!(stats_raw, None);
    }
}
