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
use tokio::sync::mpsc;

use crate::kernel::{
    models::{ChannelId, Embed, GuildId, MessageId, MessagePayload, Origin, OutboundMessage},
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::completion_port::{
    CompletionRequest, CompletionResponse, LlmCompletionPort, LlmError, TokenUsage,
};
use super::conversation::{self, ConversationRecord, RecordRole};
use super::model::{
    CaptureMode, ChannelConfig, ConversationState, GenParams, NAMESPACE, SERVICE_CHANNEL_KEY,
    UsageStats, blend_ratio, channel_state_key, channel_stats_key, records_namespace, unix_now,
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

/// One answer attempt's inputs, grouped so [`ChatEngine::answer`] and the
/// chime helper keep flat signatures.
struct AnswerRequest<'a> {
    config: &'a ChannelConfig,
    state: &'a ConversationState,
    live: &'a [ConversationRecord],
    usage_stats: &'a UsageStats,
    /// Audit label: `"triggered"` (the user addressed the bot) or `"chime"`
    /// (unprompted roll).
    trigger: &'a str,
}

/// Fields of the per-completion audit record, grouped so the logging helper
/// keeps a flat signature.
struct AnswerAudit<'a> {
    trigger: &'a str,
    started: Instant,
    model: &'a str,
    usage: Option<TokenUsage>,
    window: usize,
    window_used: usize,
    context_chars: u64,
    calibrated: bool,
}

/// Outcome of a streaming completion attempt: `Done` carries the full
/// authoritative response; `Partial` means the stream died after content
/// was already revealed (the shown text is what the endpoint produced - it
/// is delivered as-is, never patched over with the fallback notice);
/// `Failed` means nothing was shown and the regular failure policy applies.
enum LiveOutcome {
    Done(CompletionResponse),
    Partial(String),
    Failed(LlmError),
}

/// Generic public notice when a message that explicitly addresses the bot
/// cannot receive a generated answer. Deliberately detail-free: the error
/// classification goes to the service channel, endpoint bodies stay in the
/// logs.
const FALLBACK_MESSAGE: &str = "I couldn't process that just now - the language model is unreachable. Please try again in a moment.";

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

    /// Plugin-facing read access to the operator settings (command handlers
    /// need e.g. the prompt-file cap at registration time).
    #[must_use]
    pub fn settings(&self) -> &LlmSettings {
        &self.settings
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

        // History integrity unknown -> no generated answer. A mention is
        // decidable without any storage read, so the guaranteed-answer
        // contract still covers it; a reply-chain trigger is undetectable
        // here and stays silent (logged).
        let Some(state) = self.load_state(storage, channel_id).await else {
            if payload.mentions_bot {
                self.send_fallback(origin, services).await;
            }
            return;
        };
        let usage_stats = self.load_stats(storage, channel_id).await;
        let mut live = match self.load_live_records(storage, channel_id, state.cutoff_seq).await {
            Ok(live) => live,
            Err(err) => {
                // Same policy as the unreadable state doc above: history
                // integrity unknown - never answer from a degraded context.
                // Storage/LLM failures are error-grade: they surface as
                // GlitchTip issues (warn/info only ride along as log items).
                tracing::error!(channel = channel_id, %err, "llm record log unreadable - skipping message");
                if payload.mentions_bot {
                    self.send_fallback(origin, services).await;
                }
                return;
            }
        };

        let reply_to = payload.reply_to.map(MessageId::get);
        let mut live_records: Vec<ConversationRecord> =
            live.iter().map(|(_, record)| record.clone()).collect();
        let mut captured = false;
        let mut capture_failed = false;
        match self
            .capture_message(
                storage,
                origin,
                payload,
                reply_to,
                &config.capture_mode,
                &live_records,
            )
            .await
        {
            Some(Ok((seq, record))) => {
                live.push((seq, record.clone()));
                // The triggering message must be part of the context.
                live_records.push(record);
                captured = true;
            }
            Some(Err(err)) => {
                // History integrity: the message the user expects the bot
                // to have seen never entered the log. Answering anyway
                // would fabricate context - the trigger below degrades to
                // the fallback instead of a model answer.
                tracing::error!(
                    channel = channel_id,
                    %err,
                    "failed to capture message into history - skipping"
                );
                capture_failed = true;
            }
            None => {}
        }

        let request = AnswerRequest {
            config,
            state: &state,
            live: &live_records,
            usage_stats: &usage_stats,
            trigger: "triggered",
        };
        if conversation::should_trigger(payload.mentions_bot, reply_to, &live_records) {
            if capture_failed {
                self.send_fallback(origin, services).await;
            } else if !self.answer(origin, request, services).await {
                // The generated answer is impossible - the triggered message
                // still gets a visible response.
                self.send_fallback(origin, services).await;
            }
        } else if captured && config.random_chance_percent > 0.0 {
            // Random chime-in: same delivery path as a mention reply - the
            // answer is assembled from the context the message just joined
            // and recorded as an assistant turn. Only CAPTURED messages are
            // eligible: chiming in on a message the bot never tracked would
            // look like answering nothing. Unprompted, so a failed chime-in
            // stays silent - the guaranteed-answer contract covers only
            // messages addressed to the bot.
            self.maybe_chime(origin, request, services).await;
        }

        // Compaction runs after the reply (the triggering turn used the
        // pre-compaction context) and after every capture, so all-messages
        // channels compact too - not just chatty ones.
        self.maybe_compact(origin, config, &state, &live, services).await;
    }

    /// Capture half of the intake: `None` when the channel's mode does not
    /// want the message, `Some(result)` for the append attempt - `Err` means
    /// the message never entered the log (history integrity unknown).
    async fn capture_message(
        &self,
        storage: &Arc<dyn GuildStorage>,
        origin: &Origin,
        payload: &MessagePayload,
        reply_to: Option<u64>,
        mode: &CaptureMode,
        live_records: &[ConversationRecord],
    ) -> Option<Result<(u64, ConversationRecord), crate::kernel::models::StorageError>> {
        if !conversation::should_capture(*mode, payload.mentions_bot, reply_to, live_records) {
            return None;
        }
        let record = ConversationRecord {
            message_id: origin.message_id.map(MessageId::get),
            role: RecordRole::User,
            author: payload.author_name.clone(),
            content: payload.content.clone(),
            reply_to,
            captured_at: unix_now(),
        };
        Some(
            Self::append_record(storage, origin.channel_id.get(), &record)
                .await
                .map(|seq| (seq, record)),
        )
    }

    /// Completes and delivers the answer for a triggering message. The
    /// triggering message is already part of `live`, so the context ends
    /// with what the user just said. Returns whether a generated answer was
    /// actually delivered - `false` lets the caller honor the
    /// guaranteed-answer contract with the fallback notice.
    async fn answer(
        &self,
        origin: &Origin,
        request: AnswerRequest<'_>,
        services: &KernelServices,
    ) -> bool {
        let AnswerRequest { config, state, live, usage_stats, trigger } = request;
        // The answer may take tens of seconds: hold the platform typing
        // indicator across generation and delivery. The guard drops on every
        // return path - failure included, the caller's fallback follows
        // right after.
        let _typing = services.chat_output_factory.start_typing(origin);
        let channel_id = origin.channel_id.get();
        let depth = usize::try_from(config.history_depth).unwrap_or(usize::MAX);
        let skip = live.len().saturating_sub(depth);
        let window: &[ConversationRecord] = live.get(skip..).unwrap_or(live);
        let budget =
            conversation::resolve_budget(config, &self.settings, usage_stats.last.is_some());

        let messages = conversation::assemble_context(
            config,
            &self.settings,
            state,
            window,
            usage_stats.tokens_per_char,
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
        let started = Instant::now();

        // Streaming channels pull the answer live off the endpoint (SSE
        // deltas reveal on one message); everything else stays single-shot.
        let (live_id, outcome) = if config.streaming {
            self.complete_live(origin, request, services).await
        } else {
            (
                None,
                match self.completion.complete(request).await {
                    Ok(response) => LiveOutcome::Done(response),
                    Err(err) => LiveOutcome::Failed(err),
                },
            )
        };

        let response = match outcome {
            LiveOutcome::Done(response) => response,
            LiveOutcome::Partial(partial) => {
                // The stream died with content already on screen. The
                // channel sees a (partial) answer - the generic fallback
                // would contradict visible text. Finalize the partial,
                // log, and report through the service channel.
                tracing::error!(
                    channel = channel_id,
                    model = %config.model,
                    content_chars = partial.chars().count(),
                    "LLM stream interrupted - partial answer delivered"
                );
                if let Some(storage) = &services.guild_storage {
                    self.notify_service(
                        services,
                        origin,
                        storage,
                        "LLM stream interrupted",
                        format!(
                            "Model `{}`: the connection dropped mid-answer; a partial reply \
                             was delivered.",
                            config.model
                        ),
                        true,
                    )
                    .await;
                }
                return self
                    .deliver_reply(origin, config, channel_id, &partial, live_id, services)
                    .await;
            }
            LiveOutcome::Failed(err) => {
                tracing::error!(channel = channel_id, model = %config.model, %err, "LLM completion failed - no generated reply");
                if let Some(storage) = &services.guild_storage {
                    self.notify_service(
                        services,
                        origin,
                        storage,
                        "LLM completion failed",
                        format!("Model `{}`: {}.", config.model, err.classify()),
                        true,
                    )
                    .await;
                }
                return false;
            }
        };
        // Per-completion audit: usage, latency and context shape.
        self.log_answer_audit(
            channel_id,
            AnswerAudit {
                trigger,
                started,
                model: config.model.as_str(),
                usage: response.usage,
                window: live.len(),
                window_used: window.len(),
                context_chars,
                calibrated: usage_stats.last.is_some(),
            },
        );
        self.record_usage(
            services,
            channel_id,
            response.usage,
            context_chars,
            usage_stats.tokens_per_char,
            budget,
        )
        .await;

        self.deliver_reply(origin, config, channel_id, &response.content, live_id, services).await
    }

    /// Streaming completion half: pulls content deltas off the endpoint and
    /// reveals them live on one channel message - begun with the first
    /// delta, edits throttled to the configured cadence. Returns the live
    /// message id (when the reveal started) plus the outcome: the full
    /// authoritative response, a partial answer (stream died after content
    /// was shown), or the failure of a stream that never showed anything.
    async fn complete_live(
        &self,
        origin: &Origin,
        request: CompletionRequest,
        services: &KernelServices,
    ) -> (Option<MessageId>, LiveOutcome) {
        let (tx, mut rx) = mpsc::channel::<String>(32);
        let mut completion = Box::pin(self.completion.complete_streaming(request, tx));
        let stream = services.chat_output_factory.stream_output(origin);
        let mut live_id: Option<MessageId> = None;
        let mut revealed = String::new();
        let interval = Duration::from_millis(self.settings.stream_interval_ms.max(1));
        let mut next_edit = Instant::now() + interval;
        // A failed `begin` (platform hiccup) stops live revealing; the
        // completion still finishes and delivers through the plain path.
        let mut revealing = true;

        let outcome = loop {
            tokio::select! {
                biased;
                delta = rx.recv() => match delta {
                    Some(delta) => {
                        revealed.push_str(&delta);
                        if !revealing {
                            continue;
                        }
                        match live_id {
                            Some(id) => {
                                if Instant::now() >= next_edit {
                                    next_edit = Instant::now() + interval;
                                    if let Err(err) = stream.update(id, revealed.clone()).await {
                                        tracing::warn!(
                                            channel = origin.channel_id.get(),
                                            %err,
                                            "streaming update failed - continuing"
                                        );
                                    }
                                }
                            }
                            None => {
                                match stream.begin(OutboundMessage::text(revealed.clone())).await {
                                    Ok(id) => {
                                        live_id = Some(id);
                                        next_edit = Instant::now() + interval;
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            channel = origin.channel_id.get(),
                                            %err,
                                            "stream begin failed - delivering without live reveal"
                                        );
                                        revealing = false;
                                    }
                                }
                            }
                        }
                    }
                    // The adapter dropped the sender: the authoritative
                    // result is ready.
                    None => {
                        let result = completion.as_mut().await;
                        break match result {
                            Ok(response) => LiveOutcome::Done(response),
                            Err(err) if live_id.is_some() => {
                                tracing::warn!(
                                    channel = origin.channel_id.get(),
                                    %err,
                                    "stream failed after content was revealed"
                                );
                                LiveOutcome::Partial(revealed)
                            }
                            Err(err) => LiveOutcome::Failed(err),
                        };
                    }
                },
                res = &mut completion => {
                    // The adapter's future can resolve before the engine's
                    // first recv poll (tiny streams, single-poll fakes):
                    // drain whatever was buffered and reveal it - the
                    // outcome mapping below depends on knowing whether
                    // content was already on screen.
                    if revealing {
                        while let Ok(delta) = rx.try_recv() {
                            revealed.push_str(&delta);
                            match live_id {
                                None => {
                                    match stream
                                        .begin(OutboundMessage::text(revealed.clone()))
                                        .await
                                    {
                                        Ok(id) => live_id = Some(id),
                                        Err(err) => {
                                            tracing::warn!(
                                                channel = origin.channel_id.get(),
                                                %err,
                                                "stream begin failed - delivering without live reveal"
                                            );
                                            revealing = false;
                                            break;
                                        }
                                    }
                                }
                                Some(id) => {
                                    if let Err(err) =
                                        stream.update(id, revealed.clone()).await
                                    {
                                        tracing::warn!(
                                            channel = origin.channel_id.get(),
                                            %err,
                                            "streaming update failed - continuing"
                                        );
                                    }
                                }
                            }
                        }
                    }
                    break match res {
                        Ok(response) => LiveOutcome::Done(response),
                        Err(err) if live_id.is_some() => {
                            tracing::warn!(
                                channel = origin.channel_id.get(),
                                %err,
                                "stream failed after content was revealed"
                            );
                            LiveOutcome::Partial(revealed)
                        }
                        Err(err) => LiveOutcome::Failed(err),
                    };
                }
            }
        };
        (live_id, outcome)
    }

    /// Delivery half of [`ChatEngine::answer`]: splits the reply to the
    /// channel's length cap (on line boundaries), delivers it, and records
    /// the bot turn. `live_id` carries an already-streaming message from
    /// [`Self::complete_live`] - its final edit pins the exact first chunk;
    /// without one, `begin` posts the first chunk directly. Returns whether
    /// delivery could start - the caller's fallback follows when it did not.
    async fn deliver_reply(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        channel_id: u64,
        content: &str,
        live_id: Option<MessageId>,
        services: &KernelServices,
    ) -> bool {
        let max_length = config.max_length.unwrap_or(self.settings.max_message_length);
        let chunks = conversation::split_reply(content, max_length);
        if chunks.is_empty() {
            tracing::warn!(channel = channel_id, "completion returned no deliverable content");
            return false;
        }

        // The streaming port doubles as the plain reply path: `begin`
        // delivers the first chunk and yields the platform message id the
        // assistant record needs for reply-chain detection.
        let stream = services.chat_output_factory.stream_output(origin);
        let first_chunk = chunks.first().expect("non-empty chunks checked").clone();
        let first_message_id: Option<u64> = match live_id {
            Some(id) => {
                // The live message already shows prefixes of the answer;
                // the final edit pins it to the authoritative first chunk
                // (reasoning stripping may have shortened the raw deltas).
                if let Err(err) = stream.update(id, first_chunk.clone()).await {
                    tracing::warn!(channel = channel_id, %err, "final stream update failed");
                }
                Some(id.get())
            }
            None => match stream.begin(OutboundMessage::text(first_chunk.clone())).await {
                Ok(id) => Some(id.get()),
                Err(err) => {
                    tracing::warn!(
                        channel = channel_id,
                        %err,
                        "stream begin failed - falling back to plain sends"
                    );
                    None
                }
            },
        };

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
            content: content.to_owned(),
            reply_to: None,
            captured_at: unix_now(),
        };
        if let Some(storage) = &services.guild_storage
            && let Err(err) = Self::append_record(storage, channel_id, &assistant).await
        {
            tracing::error!(channel = channel_id, %err, "failed to record bot turn into history");
        }
        true
    }

    /// Per-completion audit record: usage, latency and context shape.
    /// Contents never log - sizes and counters only (info-grade: rides along
    /// to GlitchTip as a log item).
    fn log_answer_audit(&self, channel_id: u64, audit: AnswerAudit<'_>) {
        let AnswerAudit {
            trigger,
            started,
            model,
            usage,
            window,
            window_used,
            context_chars,
            calibrated,
        } = audit;
        tracing::info!(
            channel = channel_id,
            model,
            trigger,
            elapsed_ms = started.elapsed().as_millis(),
            prompt_tokens = usage.map_or(0, |usage| usage.prompt_tokens),
            completion_tokens = usage.map_or(0, |usage| usage.completion_tokens),
            cached_tokens = ?usage.and_then(|usage| usage.cached_tokens),
            reasoning_tokens = ?usage.and_then(|usage| usage.reasoning_tokens),
            window,
            window_used,
            context_chars,
            calibrated,
            "LLM answer generated"
        );
    }

    /// Random chime-in decision for one captured, non-triggering message:
    /// cooldown gate, deck-based roll, then the same delivery path as a
    /// triggered answer. Silent on every negative decision - only the roll
    /// trace at debug explains why the bot stayed quiet.
    async fn maybe_chime(
        &self,
        origin: &Origin,
        mut request: AnswerRequest<'_>,
        services: &KernelServices,
    ) {
        if !self.chime_allowed(origin) {
            tracing::debug!(channel = origin.channel_id.get(), "chime skipped - cooldown active");
            return;
        }
        let rolled = self.rng.chance_percent(
            RandomScope {
                platform: origin.platform.as_str(),
                guild_id: origin.guild_id.map_or(0, GuildId::get),
                channel_id: origin.channel_id.get(),
            },
            request.config.random_chance_percent,
        );
        tracing::debug!(
            channel = origin.channel_id.get(),
            chance = request.config.random_chance_percent,
            rolled,
            "chime roll"
        );
        if rolled {
            self.note_chime(origin);
            request.trigger = "chime";
            self.answer(origin, request, services).await;
        }
    }

    /// The guaranteed-answer contract: a message that explicitly addresses
    /// the bot (a mention, or a reply to a bot turn) never disappears
    /// silently. When the generated answer is impossible - provider failure,
    /// reasoning-only response, untrustworthy history - the channel gets
    /// this generic notice instead. It is never recorded as a bot turn and
    /// never carries error detail.
    async fn send_fallback(&self, origin: &Origin, services: &KernelServices) {
        if let Err(err) =
            services.chat_output.send(OutboundMessage::text(FALLBACK_MESSAGE.to_owned())).await
        {
            tracing::warn!(
                channel = origin.channel_id.get(),
                %err,
                "fallback notice delivery failed"
            );
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
                tracing::error!(channel = channel_id, %err, "llm conversation state unreadable - skipping message");
                None
            }
        }
    }

    /// The channel's live window: `(seq, record)` pairs after the cutoff,
    /// ascending. Malformed records are skipped (with a warning) instead of
    /// failing the whole window; an unreadable log is an error - the caller
    /// must skip the message rather than answer from a degraded context.
    async fn load_live_records(
        &self,
        storage: &Arc<dyn GuildStorage>,
        channel_id: u64,
        after_seq: u64,
    ) -> Result<Vec<(u64, ConversationRecord)>, crate::kernel::models::StorageError> {
        let records_ns = records_namespace(channel_id);
        let total = storage.count_after(&records_ns, after_seq).await?;
        if total == 0 {
            return Ok(Vec::new());
        }
        let limit = u32::try_from(total).unwrap_or(u32::MAX);
        let stored = storage.list_after(&records_ns, after_seq, limit).await?;
        Ok(stored
            .into_iter()
            .filter_map(|record| {
                serde_json::from_value::<ConversationRecord>(record.payload)
                    .inspect_err(|_| tracing::warn!("skipping malformed conversation record"))
                    .ok()
                    .map(|parsed| (record.seq, parsed))
            })
            .collect())
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
        // The summarizer call is deliberately NOT recorded into the channel's
        // chat stats: the EWMA ratio and the "last request" report must
        // reflect chat completions only, not compaction traffic.
        let request = CompletionRequest {
            model,
            messages,
            // Summarization needs no sampling tuning - provider defaults.
            params: GenParams::default(),
        };
        let response = match self.completion.complete(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::error!(channel = channel_id, %err, "LLM compaction failed - window keeps growing");
                if let Some(storage) = &services.guild_storage {
                    self.notify_service(
                        services,
                        origin,
                        storage,
                        "LLM compaction failed",
                        format!(
                            "{}.\nThe context window keeps growing until this succeeds.",
                            err.classify()
                        ),
                        true,
                    )
                    .await;
                }
                return;
            }
        };
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
                tracing::error!(channel = channel_id, %err, "failed to persist compacted state");
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
    /// per SERVICE channel ([`NOTICE_COOLDOWN`]) - an outage across many
    /// active channels still yields one embed per window - success notices
    /// are not rate-limited.
    async fn notify_service(
        &self,
        services: &KernelServices,
        origin: &Origin,
        storage: &Arc<dyn GuildStorage>,
        title: &str,
        description: String,
        is_error: bool,
    ) {
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
        // The cooldown keys on the destination, so it is known only after the
        // service-channel lookup above.
        if is_error && !self.error_notice_allowed(origin, service_channel) {
            return;
        }
        let output =
            services.chat_output_factory.channel_output(origin, ChannelId(service_channel));
        let notice = OutboundMessage::embed(Embed { title: title.to_owned(), description });
        if let Err(err) = output.send(notice).await {
            tracing::warn!(%err, "failed to deliver service notice");
        }
    }

    fn error_notice_allowed(&self, origin: &Origin, service_channel: u64) -> bool {
        let key: ChannelKey = (
            origin.platform.as_str().to_owned(),
            origin.guild_id.map_or(0, GuildId::get),
            service_channel,
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

    /// Persists the last chat completion's token usage and blends the
    /// observed tokens-per-character ratio into the channel's estimate.
    /// Compaction never records (its transcript is not chat traffic).
    /// Best effort: a failed write only degrades the next estimate back to
    /// the previous ratio; endpoints without usage stats write nothing.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{
        ChannelId, GuildId, MessageId, OutboundError, Platform, StorageError, UserId,
    };
    use crate::kernel::spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, ChatTypingGuard, GuildStorage,
        StoragePort, StoredRecord,
    };
    use crate::plugins::llm::completion_port::{
        ChatRole, CompletionResponse, LlmError, TokenUsage,
    };
    use crate::plugins::llm::model::{CaptureMode, channel_config_key};
    use crate::plugins::llm::rng::RandRandom;
    use crate::test_support::{InMemoryStorage, RecordingChatOutput};
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use serde_json::Value;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    struct FakeCompletion {
        responses: Mutex<Vec<Result<String, LlmError>>>,
        requests: Mutex<Vec<CompletionRequest>>,
        usage: Mutex<Option<TokenUsage>>,
        /// Per-call usage overrides (call order); falls back to `usage`.
        usage_queue: Mutex<VecDeque<Option<TokenUsage>>>,
    }

    impl FakeCompletion {
        fn requests(&self) -> Vec<CompletionRequest> {
            self.requests.lock().clone()
        }

        fn set_usage(&self, usage: Option<TokenUsage>) {
            *self.usage.lock() = usage;
        }

        /// Usage for the first N completion calls, in call order (chat,
        /// compaction, ...). Calls beyond the queue fall back to `usage`.
        fn set_usage_per_call(&self, usages: Vec<Option<TokenUsage>>) {
            self.usage_queue.lock().extend(usages);
        }
    }

    #[async_trait]
    impl LlmCompletionPort for FakeCompletion {
        async fn complete(
            &self,
            request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            self.requests.lock().push(request);
            let queued = self.usage_queue.lock().pop_front();
            let usage = queued.unwrap_or_else(|| self.usage.lock().to_owned());
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
        typing_starts: AtomicUsize,
    }

    impl StreamRecordingFactory {
        fn typing_starts(&self) -> usize {
            self.typing_starts.load(Ordering::SeqCst)
        }
    }

    impl ChatOutputFactoryPort for StreamRecordingFactory {
        fn chat_output(&self, _origin: &Origin) -> Arc<dyn ChatOutputPort> {
            Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
        }

        fn start_typing(&self, _origin: &Origin) -> ChatTypingGuard {
            self.typing_starts.fetch_add(1, Ordering::SeqCst);
            ChatTypingGuard::dead()
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

    /// Streaming fake: emits its chunks as real deltas, pausing between them
    /// so the engine's throttled reveal ticks, and resolves to the assembled
    /// text. `fail_at_start` / `fail_after` model endpoints that fail before
    /// any content or mid-stream.
    struct DeltaCompletion {
        chunks: Vec<String>,
        delay_ms: u64,
        fail_at_start: bool,
        fail_after: Option<usize>,
    }

    #[async_trait]
    impl LlmCompletionPort for DeltaCompletion {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            Ok(CompletionResponse { content: self.chunks.join(""), usage: None })
        }

        async fn complete_streaming(
            &self,
            _request: CompletionRequest,
            deltas: mpsc::Sender<String>,
        ) -> Result<CompletionResponse, LlmError> {
            if self.fail_at_start {
                return Err(LlmError::Request("endpoint down".to_owned()));
            }
            for (index, chunk) in self.chunks.iter().enumerate() {
                tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                let _ = deltas.send(chunk.clone()).await;
                if self.fail_after.is_some_and(|after| index + 1 == after) {
                    return Err(LlmError::Request("stream aborted".to_owned()));
                }
            }
            Ok(CompletionResponse { content: self.chunks.join(""), usage: None })
        }
    }

    /// Wiring for tests that drive the streaming path directly - same
    /// fixtures as [`ctx`], but with a custom completion port.
    struct DeltaCtx {
        engine: ChatEngine,
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        begins: Arc<Mutex<Vec<String>>>,
        updates: Arc<Mutex<Vec<String>>>,
        services: KernelServices,
    }

    fn ctx_delta(settings: LlmSettings, completion: Arc<dyn LlmCompletionPort>) -> DeltaCtx {
        let engine = ChatEngine::new(Arc::new(settings), completion, Arc::new(FixedRandom(false)));
        let storage = Arc::new(InMemoryStorage::new());
        let output = RecordingChatOutput::new();
        let begins = Arc::new(Mutex::new(Vec::new()));
        let updates = Arc::new(Mutex::new(Vec::new()));
        let factory = Arc::new(StreamRecordingFactory {
            begins: Arc::clone(&begins),
            updates: Arc::clone(&updates),
            output: Arc::clone(&output),
            typing_starts: AtomicUsize::new(0),
        });
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::clone(&factory) as Arc<dyn ChatOutputFactoryPort>,
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        };
        DeltaCtx { engine, storage, output, begins, updates, services }
    }

    /// Storage whose document half delegates to an in-memory storage but
    /// whose record log always fails - the fixture for the degraded-history
    /// policy tests (unreadable log, failed capture append).
    struct RecordsFailStorage {
        documents: Arc<InMemoryStorage>,
    }

    struct RecordsFailView {
        guild: Arc<dyn GuildStorage>,
    }

    #[async_trait]
    impl GuildStorage for RecordsFailView {
        async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>, StorageError> {
            self.guild.get(namespace, key).await
        }

        async fn set(&self, namespace: &str, key: &str, value: Value) -> Result<(), StorageError> {
            self.guild.set(namespace, key, value).await
        }

        async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError> {
            self.guild.delete(namespace, key).await
        }

        async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError> {
            self.guild.list_keys(namespace).await
        }

        async fn append(&self, _namespace: &str, _payload: Value) -> Result<u64, StorageError> {
            Err(StorageError::Database("records unavailable".to_owned()))
        }

        async fn list_after(
            &self,
            _namespace: &str,
            _after_seq: u64,
            _limit: u32,
        ) -> Result<Vec<StoredRecord>, StorageError> {
            Err(StorageError::Database("records unavailable".to_owned()))
        }

        async fn count_after(
            &self,
            _namespace: &str,
            _after_seq: u64,
        ) -> Result<u64, StorageError> {
            Err(StorageError::Database("records unavailable".to_owned()))
        }
    }

    impl StoragePort for RecordsFailStorage {
        fn guild_scoped(&self, platform: Platform, guild_id: GuildId) -> Arc<dyn GuildStorage> {
            Arc::new(RecordsFailView { guild: self.documents.guild_scoped(platform, guild_id) })
        }
    }

    /// Storage whose record-log READS work but whose appends always fail -
    /// isolates the failed-capture-append branch from the unreadable-log
    /// branch (which `RecordsFailStorage` covers).
    struct AppendFailStorage {
        documents: Arc<InMemoryStorage>,
    }

    struct AppendFailView {
        guild: Arc<dyn GuildStorage>,
    }

    #[async_trait]
    impl GuildStorage for AppendFailView {
        async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>, StorageError> {
            self.guild.get(namespace, key).await
        }

        async fn set(&self, namespace: &str, key: &str, value: Value) -> Result<(), StorageError> {
            self.guild.set(namespace, key, value).await
        }

        async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError> {
            self.guild.delete(namespace, key).await
        }

        async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError> {
            self.guild.list_keys(namespace).await
        }

        async fn append(&self, _namespace: &str, _payload: Value) -> Result<u64, StorageError> {
            Err(StorageError::Database("records unavailable".to_owned()))
        }

        async fn list_after(
            &self,
            namespace: &str,
            after_seq: u64,
            limit: u32,
        ) -> Result<Vec<StoredRecord>, StorageError> {
            self.guild.list_after(namespace, after_seq, limit).await
        }

        async fn count_after(&self, namespace: &str, after_seq: u64) -> Result<u64, StorageError> {
            self.guild.count_after(namespace, after_seq).await
        }
    }

    impl StoragePort for AppendFailStorage {
        fn guild_scoped(&self, platform: Platform, guild_id: GuildId) -> Arc<dyn GuildStorage> {
            Arc::new(AppendFailView { guild: self.documents.guild_scoped(platform, guild_id) })
        }
    }

    struct TestCtx {
        engine: ChatEngine,
        fake: Arc<FakeCompletion>,
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        begins: Arc<Mutex<Vec<String>>>,
        updates: Arc<Mutex<Vec<String>>>,
        factory: Arc<StreamRecordingFactory>,
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
            usage_queue: Mutex::new(VecDeque::new()),
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
        let factory = Arc::new(StreamRecordingFactory {
            begins: Arc::clone(&begins),
            updates: Arc::clone(&updates),
            output: Arc::clone(&output),
            typing_starts: AtomicUsize::new(0),
        });
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::clone(&factory) as Arc<dyn ChatOutputFactoryPort>,
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        };
        TestCtx { engine, fake, storage, output, begins, updates, factory, services }
    }

    fn assigned_config() -> ChannelConfig {
        // Chime chance is zero unless a test opts in: the chime-path tests
        // set an explicit percent next to their FixedRandom. Every other
        // untriggered capture then skips the roll entirely - the default
        // ctx() wires a real 2% coin (RandRandom), which made capture tests
        // flake when the coin landed.
        ChannelConfig {
            random_chance_percent: 0.0,
            ..ChannelConfig::assigned("local/gemma".to_owned())
        }
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
        stored_records_in(&ctx.storage).await
    }

    async fn stored_records_in(storage: &Arc<InMemoryStorage>) -> Vec<ConversationRecord> {
        storage
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

    /// Every generated answer holds the platform typing indicator across
    /// generation and delivery - users see the bot composing, not frozen.
    #[tokio::test]
    async fn answer_holds_the_typing_indicator() {
        let ctx = ctx(vec![Ok("hi alice".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        assert_eq!(ctx.factory.typing_starts(), 1);

        // The next answer starts it again - one guard per answer.
        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;
        assert_eq!(ctx.factory.typing_starts(), 2);
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
    async fn completion_failure_sends_fallback_and_records_no_bot_turn() {
        let ctx = ctx(vec![Err(LlmError::Request("provider down".to_owned()))]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        // The generated answer is gone, but the triggered message gets the
        // visible generic fallback - and no bot turn is recorded.
        assert!(ctx.begins.lock().is_empty());
        assert_eq!(ctx.output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
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

    /// The cutoff is a context boundary, not a deletion: records before
    /// `cutoff_seq` never re-enter the context but stay stored.
    #[tokio::test]
    async fn cutoff_keeps_old_records_out_of_the_context() {
        let ctx = ctx(vec![Ok("fresh answer".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());
        ctx.storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_state_key(2),
            serde_json::json!({"cutoff_seq": 2}),
        );
        append_record(&ctx.storage, &user_record(1, "old1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "old2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "new1", "m3")).await;
        append_record(&ctx.storage, &user_record(4, "new2", "m4")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        // The context holds only post-cutoff turns plus the triggering one.
        let request = ctx.fake.requests().first().expect("request expected").clone();
        let turns: Vec<&str> =
            request.messages.iter().map(|message| message.content.as_str()).collect();
        assert!(turns.iter().any(|turn| turn.contains("new1: m3")));
        assert!(!turns.iter().any(|turn| turn.contains("m1") || turn.contains("m2")));
        // The cutoff moved nothing: all records are still stored
        // (4 seeded + the captured trigger + the assistant turn).
        assert_eq!(stored_records(&ctx).await.len(), 6);
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
            usage_queue: Mutex::new(VecDeque::new()),
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
        // A mention is decidable without the state doc - the guarantee holds.
        assert_eq!(output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
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

    /// The summarizer call is invisible to chat stats: the EWMA ratio and
    /// the "last request" report reflect chat completions only.
    #[tokio::test]
    async fn compaction_does_not_pollute_chat_stats() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx =
            ctx_with(settings, vec![Ok("summary text".to_owned()), Ok("the answer".to_owned())]);
        ctx.fake.set_usage_per_call(vec![
            Some(TokenUsage {
                prompt_tokens: 100,
                completion_tokens: 10,
                total_tokens: 110,
                cached_tokens: None,
                reasoning_tokens: None,
            }),
            Some(TokenUsage {
                prompt_tokens: 50_000,
                completion_tokens: 20,
                total_tokens: 50_020,
                cached_tokens: None,
                reasoning_tokens: None,
            }),
        ]);
        let config = ChannelConfig { history_depth: 3, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        // Both completions ran (chat + compaction), but the stored "last
        // request" stays the CHAT call's usage - the summarizer never writes.
        assert_eq!(ctx.fake.requests().len(), 2);
        let stats_raw = ctx
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, &channel_stats_key(2))
            .await
            .expect("stats readable")
            .expect("stats expected");
        let stats: UsageStats =
            serde_json::from_value(stats_raw).expect("stats expected to deserialize");
        assert_eq!(stats.last.map(|usage| usage.prompt_tokens), Some(100));
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

    /// An unreadable record log degrades to no generated answer - the same
    /// policy as an unreadable state doc. The bot must not answer from a
    /// context that may be silently empty, but a mention still gets the
    /// visible fallback.
    #[tokio::test]
    async fn unreadable_record_log_falls_back_for_mentions() {
        let mut ctx = ctx(vec![Ok("should not answer".to_owned())]);
        ctx.services.guild_storage = Some(
            RecordsFailStorage { documents: Arc::clone(&ctx.storage) }
                .guild_scoped(Platform::Discord, GuildId(1)),
        );
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        assert!(ctx.fake.requests().is_empty());
        assert_eq!(ctx.output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
    }

    /// A failed capture append cannot produce a generated answer: the
    /// message never entered the log, so answering would fabricate a
    /// context that never saw it. The mention still gets the visible
    /// fallback, and the model is never called.
    #[tokio::test]
    async fn failed_capture_falls_back_without_answering() {
        let mut ctx = ctx(vec![Ok("fabricated answer".to_owned())]);
        ctx.services.guild_storage = Some(
            AppendFailStorage { documents: Arc::clone(&ctx.storage) }
                .guild_scoped(Platform::Discord, GuildId(1)),
        );
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        assert!(ctx.fake.requests().is_empty());
        assert_eq!(ctx.output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
        assert!(stored_records(&ctx).await.is_empty());
    }

    /// Service-channel embeds carry the error classification only: the
    /// endpoint's response body (operator-domain detail) stays in logs.
    #[tokio::test]
    async fn failure_embed_reports_no_endpoint_body() {
        let ctx = ctx(vec![Err(LlmError::Request(
            "HTTP 401: account=secret-org project=hidden".to_owned(),
        ))]);
        seed_config(&ctx.storage, &assigned_config());
        seed_service_channel(&ctx);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &ctx.services)
            .await;

        let text = ctx
            .output
            .messages()
            .into_iter()
            .find(|message| message.contains("could not be reached"))
            .expect("service notice expected");
        assert!(
            !text.contains("secret-org"),
            "the endpoint body must not reach guild-visible embeds"
        );
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

        // Three failed completions - one rate-limited error embed for the
        // operator, plus a visible fallback per triggered message.
        let messages = ctx.output.messages();
        assert_eq!(messages.iter().filter(|m| m.contains("LLM completion failed")).count(), 1);
        assert_eq!(messages.iter().filter(|m| m.contains(FALLBACK_MESSAGE)).count(), 3);
    }

    /// The cooldown keys on the SERVICE channel, not the origin: an outage
    /// across two active channels still yields a single embed per window.
    #[tokio::test]
    async fn error_notices_are_rate_limited_across_channels() {
        let ctx = ctx(vec![Err(LlmError::Request("down".to_owned())); 4]);
        seed_config(&ctx.storage, &assigned_config());
        seed_service_channel(&ctx);

        for channel in [2_u64, 3] {
            let mut channel_origin = origin();
            channel_origin.channel_id = ChannelId(channel);
            ctx.engine
                .handle_message(
                    &channel_origin,
                    &payload(true, None),
                    &assigned_config(),
                    &ctx.services,
                )
                .await;
        }

        let messages = ctx.output.messages();
        assert_eq!(
            messages.iter().filter(|m| m.contains("LLM completion failed")).count(),
            1,
            "one outage window = one embed, regardless of origin channels"
        );
        assert_eq!(messages.iter().filter(|m| m.contains(FALLBACK_MESSAGE)).count(), 2);
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
        let config = ChannelConfig {
            capture_mode: CaptureMode::AllMessages,
            random_chance_percent: 2.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(false, None), &config, &ctx.services).await;

        assert_eq!(ctx.fake.requests().len(), 1);
        assert_eq!(ctx.begins.lock().clone(), vec!["random thought".to_owned()]);
        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 2);
        assert_eq!(records.get(1).map(|record| record.role), Some(RecordRole::Assistant));
    }

    /// A chime-in is unprompted: when its completion fails, silence is the
    /// right outcome - the guaranteed-answer contract covers only messages
    /// addressed to the bot.
    #[tokio::test]
    async fn chime_failure_stays_silent() {
        let ctx = ctx_random(
            LlmSettings::default(),
            Arc::new(FixedRandom(true)),
            vec![Err(LlmError::Request("provider down".to_owned()))],
        );
        let config = ChannelConfig {
            capture_mode: CaptureMode::AllMessages,
            random_chance_percent: 2.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(false, None), &config, &ctx.services).await;

        // The capture stays, no answer is delivered, nothing is said.
        assert_eq!(ctx.fake.requests().len(), 1);
        assert!(ctx.begins.lock().is_empty());
        assert!(ctx.output.messages().is_empty());
        assert_eq!(stored_records(&ctx).await.len(), 1);
    }

    #[tokio::test]
    async fn random_reply_miss_leaves_only_the_capture() {
        let ctx = ctx_random(LlmSettings::default(), Arc::new(FixedRandom(false)), vec![]);
        let config = ChannelConfig {
            capture_mode: CaptureMode::AllMessages,
            random_chance_percent: 2.0,
            ..assigned_config()
        };
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
        let config = ChannelConfig {
            capture_mode: CaptureMode::AllMessages,
            random_chance_percent: 2.0,
            ..assigned_config()
        };
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

    /// Streaming channels reveal real deltas: the message begins with the
    /// first delta, live edits land between deltas (the fake pauses past
    /// the throttle cadence), and the final edit pins the authoritative
    /// text. Updates are always prefixes of it - behind, never wrong.
    #[tokio::test]
    async fn streaming_reveals_live_deltas() {
        let settings = LlmSettings { stream_interval_ms: 1, ..LlmSettings::default() };
        let ctx = ctx_delta(
            settings,
            Arc::new(DeltaCompletion {
                chunks: vec!["Answer".to_owned(), " continues".to_owned()],
                delay_ms: 5,
                fail_at_start: false,
                fail_after: None,
            }),
        );
        let config = ChannelConfig { streaming: true, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        assert_eq!(ctx.begins.lock().clone(), vec!["Answer".to_owned()]);
        let updates = ctx.updates.lock().clone();
        assert_eq!(updates.last().map(String::as_str), Some("Answer continues"));
        for update in &updates {
            assert!("Answer continues".starts_with(update.as_str()));
        }
        // The bot turn is recorded from the authoritative assembled text.
        let records = stored_records_in(&ctx.storage).await;
        let last = records.last().expect("bot turn recorded");
        assert_eq!(last.role, RecordRole::Assistant);
        assert_eq!(last.content, "Answer continues");
    }

    /// A stream that fails before the first delta shows nothing: the
    /// triggered message gets the guaranteed fallback, nothing is recorded.
    #[tokio::test]
    async fn stream_failure_before_first_delta_falls_back() {
        let ctx = ctx_delta(
            LlmSettings::default(),
            Arc::new(DeltaCompletion {
                chunks: vec![],
                delay_ms: 1,
                fail_at_start: true,
                fail_after: None,
            }),
        );
        let config = ChannelConfig { streaming: true, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        assert_eq!(ctx.output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
        assert!(ctx.begins.lock().is_empty());
        // The user's own turn is captured before the answer attempt - it
        // stays; nothing else may appear.
        let records = stored_records_in(&ctx.storage).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().map(|record| record.role), Some(RecordRole::User));
    }

    /// A stream that dies after content was revealed keeps that content
    /// visible and recorded - the generic fallback would contradict text
    /// the channel has already seen.
    #[tokio::test]
    async fn stream_failure_after_partial_keeps_partial_visible() {
        let ctx = ctx_delta(
            LlmSettings::default(),
            Arc::new(DeltaCompletion {
                chunks: vec!["partial answer".to_owned()],
                delay_ms: 1,
                fail_at_start: false,
                fail_after: Some(1),
            }),
        );
        let config = ChannelConfig { streaming: true, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &ctx.services).await;

        // No fallback: partial text is already on screen.
        assert!(ctx.output.messages().is_empty());
        assert_eq!(ctx.begins.lock().clone(), vec!["partial answer".to_owned()]);
        let updates = ctx.updates.lock().clone();
        assert_eq!(updates.last().map(String::as_str), Some("partial answer"));
        let records = stored_records_in(&ctx.storage).await;
        let last = records.last().expect("bot turn recorded");
        assert_eq!(last.role, RecordRole::Assistant);
        assert_eq!(last.content, "partial answer");
    }

    #[tokio::test]
    async fn token_usage_is_recorded_and_calibrates_the_estimate() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        ctx.fake.set_usage(Some(TokenUsage {
            prompt_tokens: 1200,
            completion_tokens: 5,
            total_tokens: 1205,
            cached_tokens: None,
            reasoning_tokens: None,
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
