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
use serde_json::Value;
use tokio::sync::mpsc;

use crate::kernel::{
    models::{ChannelId, Embed, GuildId, MessageId, MessagePayload, Origin, OutboundMessage},
    services::KernelServices,
    spi_ports::{ChatStreamPort, ChatTypingGuard, GuildStorage, PluginStorage},
};

use super::completion_port::{
    ChatMessage, ChatRole, CompletionRequest, CompletionResponse, LlmCompletionPort, LlmError,
    ResponseTiming, TokenUsage,
};
use super::conversation::{self, ConversationRecord, RecordRole};
use super::model::{
    ChannelConfig, ChannelKey, ConversationState, EmojiInject, EmojiWhitelistEntry,
    GUILD_EMOJI_WHITELIST_KEY, GenParams, NAMESPACE, SERVICE_CHANNEL_KEY, UsageStats, blend_ratio,
    channel_emoji_whitelist_key, channel_key, channel_state_key, channel_stats_key,
    parse_whitelist, records_namespace, unix_now,
};
use super::prompts::{PromptVars, render_prompt};
use super::providers::LlmSettings;
use super::rng::{RandomPort, RandomScope};
use super::tools;
use super::usage_total::{Dimension, Sample, UsageTracker};
use super::vision::{self, DEFAULT_IMAGE_PROMPT, ImageDescriber, ImageJob, ImageSource, UsageSink};
use crate::kernel::spi_ports::PlatformInfoPort;

/// One undescribed placeholder entry per image (feature off, cap overflow,
/// or recognition failure - the record still shows THAT an image existed,
/// with its real extension baked at capture).
fn undescribed(sources: &[ImageSource]) -> Vec<conversation::RecordImage> {
    sources
        .iter()
        .map(|source| conversation::RecordImage { description: None, ext: source.ext.clone() })
        .collect()
}

/// Minimum interval between error notices for the same channel: a down
/// provider must not turn every triggering message into an admin ping.
const NOTICE_COOLDOWN: Duration = Duration::from_secs(300);

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
    /// Current guild name from the payload - `{{guild_name}}` material.
    guild_name: Option<&'a str>,
}

/// Inputs of the per-channel stats write, grouped so
/// [`ChatEngine::record_usage`] keeps a flat signature.
struct UsageRecord {
    usage: Option<TokenUsage>,
    timing: ResponseTiming,
    context_chars: u64,
    /// Delivered answer size in characters - the completion-side measure the
    /// estimate falls back to when the endpoint reports no usage.
    completion_chars: u64,
    tokens_per_char: f64,
    budget: Option<u64>,
}

/// Fields of the per-completion audit record, grouped so the logging helper
/// keeps a flat signature.
struct AnswerAudit<'a> {
    trigger: &'a str,
    started: Instant,
    model: &'a str,
    usage: Option<TokenUsage>,
    /// Complete provider time plus its source (endpoint-reported when the
    /// provider publishes timings, else adapter-measured wall clock).
    timing: ResponseTiming,
    window: usize,
    window_used: usize,
    context_chars: u64,
    calibrated: bool,
    /// Whether the usage row came from the endpoint or the estimator.
    usage_source: &'static str,
    /// The model's updated all-time totals after this completion.
    cumulative: Dimension,
}

/// The compaction call's usage, carried to the commit's audit line.
struct CompactionAudit {
    model: String,
    sample: Sample,
    /// Whether the usage came from the endpoint or the estimator.
    reported: bool,
    cumulative: Dimension,
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

/// Live reveal state of one streaming answer: the target message plus the
/// throttle clock. [`Self::push`] begins the message with the first delta
/// and edits it at the configured cadence afterwards; [`Self::drain`]
/// applies leftovers that were buffered before the engine polled (a stream
/// can resolve faster than the engine's first poll). Edits always carry
/// prefixes of the revealed text - the message is only ever behind, never
/// wrong. The begin message natively replies to the triggering message.
/// A failed `begin` (platform hiccup) stops revealing; the completion still
/// finishes and delivers through the plain path.
struct LiveReveal {
    stream: Arc<dyn ChatStreamPort>,
    channel_id: u64,
    reply_to: Option<MessageId>,
    message: Option<MessageId>,
    revealed: String,
    revealing: bool,
    next_edit: Instant,
    interval: Duration,
    /// Platform message cap: every posted edit is clamped to it, so a
    /// stream longer than one message never 400s mid-reveal. The reveal is
    /// a prefix of the answer; `deliver_reply`'s final edit re-pins the
    /// authoritative first chunk and the overflow rides the plain parts.
    max_length: usize,
}

impl LiveReveal {
    fn new(
        stream: Arc<dyn ChatStreamPort>,
        channel_id: u64,
        reply_to: Option<MessageId>,
        interval: Duration,
        max_length: usize,
    ) -> Self {
        Self {
            stream,
            channel_id,
            reply_to,
            message: None,
            revealed: String::new(),
            revealing: true,
            next_edit: Instant::now() + interval,
            interval,
            max_length: max_length.max(1),
        }
    }

    async fn push(&mut self, delta: &str) {
        self.revealed.push_str(delta);
        if !self.revealing {
            return;
        }
        // The reveal is always a cap-fitting prefix of the revealed text -
        // an edit past the platform limit would be rejected outright, and
        // the final delivery re-pins the authoritative split anyway.
        let (visible, _) = conversation::split_utf16_floor(&self.revealed, self.max_length);
        if let Some(id) = self.message {
            if Instant::now() >= self.next_edit {
                self.next_edit = Instant::now() + self.interval;
                if let Err(err) = self.stream.update(id, visible.to_owned()).await {
                    tracing::warn!(
                        channel = self.channel_id,
                        %err,
                        "streaming update failed - continuing"
                    );
                }
            }
        } else {
            let mut begin = OutboundMessage::text(visible.to_owned());
            if let Some(reply_to) = self.reply_to {
                begin = begin.replying_to(reply_to);
            }
            match self.stream.begin(begin).await {
                Ok(id) => {
                    self.message = Some(id);
                    self.next_edit = Instant::now() + self.interval;
                }
                Err(err) => {
                    tracing::warn!(
                        channel = self.channel_id,
                        %err,
                        "stream begin failed - delivering without live reveal"
                    );
                    self.revealing = false;
                }
            }
        }
    }

    /// Applies deltas buffered before the adapter resolved.
    async fn drain(&mut self, rx: &mut mpsc::Receiver<String>) {
        while let Ok(delta) = rx.try_recv() {
            self.push(&delta).await;
        }
    }
}

/// Maps the streaming completion result onto the outcome: content already
/// revealed turns a stream failure into a partial answer (the fallback
/// notice would contradict visible text).
fn live_outcome(
    result: Result<CompletionResponse, LlmError>,
    reveal: &mut LiveReveal,
) -> LiveOutcome {
    match result {
        Ok(response) => LiveOutcome::Done(response),
        Err(err) if reveal.message.is_some() => {
            tracing::warn!(
                channel = reveal.channel_id,
                %err,
                "stream failed after content was revealed"
            );
            LiveOutcome::Partial(std::mem::take(&mut reveal.revealed))
        }
        Err(err) => LiveOutcome::Failed(err),
    }
}

/// Generic public notice when a message that explicitly addresses the bot
/// cannot receive a generated answer. Deliberately detail-free: the error
/// classification goes to the service channel, endpoint bodies stay in the
/// logs.
pub(crate) const FALLBACK_MESSAGE: &str = "I couldn't process that just now - the language model is unreachable. Please try again in a moment.";

pub struct ChatEngine {
    settings: Arc<LlmSettings>,
    completion: Arc<dyn LlmCompletionPort>,
    rng: Arc<dyn RandomPort>,
    /// Image recognition (capture-time descriptions); a no-op fake in
    /// tests.
    describer: Arc<dyn ImageDescriber>,
    /// The deployment's platform identity (adapter-owned values) - prompt
    /// templates and slug-keyed cooldowns read it.
    platform_info: Arc<dyn PlatformInfoPort>,
    /// Plugin-global token usage totals (models, guilds, UTC days). The
    /// tracker owns its plugin-global storage binding.
    usage: Arc<UsageTracker>,
    /// Last error-notice time per channel (see [`NOTICE_COOLDOWN`]).
    notices: Mutex<HashMap<ChannelKey, Instant>>,
    /// Last random chime-in time per channel; the cooldown length is the
    /// channel's `random_cooldown_secs` setting.
    chimes: Mutex<HashMap<ChannelKey, Instant>>,
    /// Last silent-react chime time per channel - independent of the reply
    /// chime tracker, so the two rolls coexist in parallel (the cooldown
    /// duration is the same `random_cooldown_secs`).
    react_chimes: Mutex<HashMap<ChannelKey, Instant>>,
}

impl ChatEngine {
    #[must_use]
    pub fn new(
        settings: Arc<LlmSettings>,
        completion: Arc<dyn LlmCompletionPort>,
        rng: Arc<dyn RandomPort>,
        describer: Arc<dyn ImageDescriber>,
        platform_info: Arc<dyn PlatformInfoPort>,
        plugin_storage: Arc<dyn PluginStorage>,
    ) -> Self {
        Self {
            settings,
            completion,
            rng,
            describer,
            platform_info,
            usage: UsageTracker::new(plugin_storage),
            notices: Mutex::new(HashMap::new()),
            chimes: Mutex::new(HashMap::new()),
            react_chimes: Mutex::new(HashMap::new()),
        }
    }

    /// Plugin-facing read access to the usage tracker: the plugin flushes it
    /// on a scheduler job and `/llm_usage` renders its snapshots.
    #[must_use]
    pub fn usage(&self) -> Arc<UsageTracker> {
        Arc::clone(&self.usage)
    }

    /// Plugin-facing read access to the operator settings (command handlers
    /// need e.g. the prompt-file cap at registration time).
    #[must_use]
    pub fn settings(&self) -> &LlmSettings {
        &self.settings
    }

    /// Processes one inbound message of an assigned channel. Always
    /// returns normally - failures are logged, never propagated. A caller
    /// that decided the trigger before the channel lock (and started the
    /// typing indicator there, so the queue wait reads as "composing")
    /// passes its guard in `pre_typing`; `None` lets the engine type itself
    /// when an answer is due (the chime path, which rolls under the lock).
    pub async fn handle_message(
        &self,
        origin: &Origin,
        payload: &MessagePayload,
        config: &ChannelConfig,
        services: &KernelServices,
        pre_typing: Option<ChatTypingGuard>,
    ) {
        let Some(storage) = &services.guild_storage else {
            return; // DMs cannot have channel config; unreachable via `pre`.
        };
        let channel_id = origin.channel_id.get();
        // Usage totals seed lazily on first use; the seed is a no-op once
        // done, so every message pays one `OnceCell::get`.
        self.usage.ensure_seeded().await;

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
        // Operational window: everything assembly (depth) plus the compaction
        // tail can ever need - the hard cap on per-message history loading.
        let depth = usize::try_from(config.context_messages).unwrap_or(usize::MAX);
        let keep_tail = usize::try_from(self.settings.compaction_keep_tail).unwrap_or(usize::MAX);
        let window = depth.saturating_add(keep_tail).max(1);
        let live = match self.load_live_records(storage, channel_id, state.cutoff_seq, window).await
        {
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
        // Split once, by move: the seqs ride parallel to the records, so the
        // window is never duplicated per message (compaction needs the seqs,
        // assembly the records).
        let (mut seqs, mut live_records): (Vec<u64>, Vec<ConversationRecord>) =
            live.into_iter().unzip();
        let mut captured = false;
        let mut capture_failed = false;
        match self.capture_message(storage, origin, payload, reply_to, config, &live_records).await
        {
            Some(Ok((seq, record))) => {
                // The triggering message must be part of the context.
                seqs.push(seq);
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
            guild_name: payload.guild_name.as_deref(),
        };
        if conversation::should_trigger(payload.mentions_bot, reply_to, &live_records) {
            if capture_failed {
                self.send_fallback(origin, services).await;
            } else if !self.answer(origin, request, services, pre_typing).await {
                // The generated answer is impossible - the triggered message
                // still gets a visible response.
                self.send_fallback(origin, services).await;
            }
        } else if captured
            && (config.random_reply_chance_percent > 0.0
                || (config.react && config.random_react_chance_percent > 0.0))
        {
            // Random chime-in: same delivery path as a mention reply - the
            // answer is assembled from the context the message just joined
            // and recorded as an assistant turn. Only CAPTURED messages are
            // eligible: chiming in on a message the bot never tracked would
            // look like answering nothing. Unprompted, so a failed chime-in
            // stays silent - the guaranteed-answer contract covers only
            // messages addressed to the bot. The react roll lives inside
            // `maybe_chime` and shares only the capture eligibility.
            self.maybe_chime(origin, request, services).await;
        }

        // Compaction runs after the reply (the triggering turn used the
        // pre-compaction context) and after every capture, so all-messages
        // channels compact too - not just chatty ones.
        self.maybe_compact(
            origin,
            config,
            &state,
            &seqs,
            &live_records,
            usage_stats.tokens_per_char,
            payload.guild_name.as_deref(),
            services,
        )
        .await;
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
        config: &ChannelConfig,
        live_records: &[ConversationRecord],
    ) -> Option<Result<(u64, ConversationRecord), crate::kernel::models::StorageError>> {
        if !conversation::should_capture(
            config.capture,
            payload.mentions_bot,
            reply_to,
            live_records,
        ) {
            return None;
        }
        let images = self.describe_images(config, payload, origin).await;
        let record = ConversationRecord {
            message_id: origin.message_id.map(MessageId::get),
            role: RecordRole::User,
            author: payload.author_name.clone(),
            sender_id: Some(origin.user_id.get()),
            guild_name: payload.guild_name.clone(),
            content: payload.content.clone(),
            reply_to,
            captured_at: unix_now(),
            images,
        };
        Some(
            Self::append_record(storage, origin.channel_id.get(), &record)
                .await
                .map(|seq| (seq, record)),
        )
    }

    /// Capture-time image recognition. The description is baked into the
    /// record before the append, so the rendered prompt stays byte-stable
    /// forever after. Best-effort by contract: channels without the
    /// feature, images over the per-message cap, and every failure mode
    /// record undescribed placeholders - capture never breaks, and the
    /// model still sees THAT an image was posted. Recognition usage stays
    /// out of the channel's chat stats (compaction precedent).
    async fn describe_images(
        &self,
        config: &ChannelConfig,
        payload: &MessagePayload,
        origin: &Origin,
    ) -> Vec<conversation::RecordImage> {
        let sources: Vec<ImageSource> = payload
            .attachments
            .iter()
            .map(|attachment| ImageSource {
                url: attachment.url.clone(),
                content_type: attachment.content_type.clone(),
                ext: conversation::placeholder_ext(
                    attachment.content_type.as_deref(),
                    attachment.file_name.as_deref(),
                ),
            })
            .filter(vision::is_image_source)
            .collect();
        if sources.is_empty() {
            return Vec::new();
        }
        if !config.images {
            return undescribed(&sources);
        }
        let Some(model) = config.image_model.clone().or_else(|| self.settings.image_model.clone())
        else {
            tracing::debug!(
                "channel image recognition enabled but no image model is configured - storing undescribed"
            );
            return undescribed(&sources);
        };
        let cap = usize::try_from(self.settings.max_images_per_message).unwrap_or(usize::MAX);
        let (described, overflow) = if sources.len() > cap {
            sources.split_at(cap)
        } else {
            (sources.as_slice(), [].as_slice())
        };
        let job = ImageJob {
            model,
            prompt: config
                .image_prompt
                .clone()
                .or_else(|| self.settings.image_prompt.clone())
                .unwrap_or_else(|| DEFAULT_IMAGE_PROMPT.to_owned()),
            // The command layer already clamps to the cap, but stored docs
            // can be hand-edited and the cap can drop below stored values -
            // enforce the ceiling at the point of use, not just at set time.
            max_side: config.image_max_side.min(self.settings.image_max_side_cap),
            jpeg_quality: self.settings.image_jpeg_quality,
            max_source_bytes: self.settings.image_max_source_bytes,
        };
        let started = Instant::now();
        // Recognition usage lands in the plugin-global totals (reported by
        // the endpoint, request-only when unreported) - never in the
        // channel's chat stats (compaction precedent).
        let sink = Some(UsageSink::new(&self.usage, origin.guild_id.map(GuildId::get)));
        let results = self.describer.describe(&job, described.to_vec(), sink).await;
        let described_count = results.iter().filter(|result| result.is_some()).count();
        let cumulative = self.usage.cumulative(&job.model);
        tracing::info!(
            total = sources.len(),
            described = described_count,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            model = %job.model,
            cumulative_requests = cumulative.requests,
            cumulative_prompt_tokens = cumulative.prompt_tokens,
            cumulative_completion_tokens = cumulative.completion_tokens,
            "image recognition completed"
        );
        let mut images: Vec<_> = results
            .into_iter()
            .zip(described)
            .map(|(description, source)| conversation::RecordImage {
                description,
                ext: source.ext.clone(),
            })
            .collect();
        images.extend(overflow.iter().map(|source| conversation::RecordImage {
            description: None,
            ext: source.ext.clone(),
        }));
        images
    }

    /// Assembles one answer attempt's completion request from the channel's
    /// live records: the newest `context_messages` records as the window, the
    /// resolved token budget, the character count the endpoint's usage
    /// report is measured against, and the window size for the audit row.
    /// Shared by the triggered/chime answer and the silent-react chime.
    /// Request-scoped prompt template values: bot identity from settings,
    /// the deployment's platform display name, a time snapshot taken now,
    /// guild/model from the current request.
    fn prompt_vars(&self, guild_name: Option<&str>, model: &str) -> PromptVars {
        PromptVars::new(
            self.settings.bot_name.clone(),
            self.settings.bot_id.clone(),
            i64::try_from(unix_now()).unwrap_or(i64::MAX),
            self.settings.time_offset_minutes,
            self.platform_info.display_name(),
            guild_name,
            model,
        )
    }

    /// The react tool's server-emoji menu: exact wire-form tokens, one per
    /// custom emoji. Without descriptions the menu is the historical single
    /// line in the platform's listing order; with any description it
    /// becomes a header plus sub-lines sorted by name. Both shapes are
    /// byte-stable between emoji-set or whitelist changes - the menu rides
    /// the prompt's cacheable prefix. Entries with a description render as
    /// sub-lines; a whitelist without descriptions keeps the plain single
    /// line. Empty unless something would actually be injected: `none`
    /// never, `all` only while the platform lists emojis, `whitelist` only
    /// while the effective list matches a server emoji - the default mode
    /// is a no-op while its list is empty, byte-identical to `none`. Every
    /// failure on the way degrades to empty (the menu is cosmetic).
    async fn react_emoji_menu(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        services: &KernelServices,
    ) -> String {
        if config.react_emoji_inject == EmojiInject::None {
            return String::new();
        }
        let mut emojis = services.chat_output_factory.reactable_emojis(origin).list().await;
        let mut described: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        if config.react_emoji_inject == EmojiInject::Whitelist {
            let whitelist = self.effective_emoji_whitelist(origin, services).await;
            emojis.retain(|emoji| whitelist.iter().any(|entry| entry.name == emoji.name));
            // Stale whitelists (every entry gone from the guild) stop here -
            // no menu, and the description map is not worth building.
            if emojis.is_empty() {
                return String::new();
            }
            described = whitelist
                .into_iter()
                .filter_map(|entry| {
                    let description = entry.description.filter(|text| !text.is_empty())?;
                    Some((entry.name, description))
                })
                .collect();
        }
        if emojis.is_empty() {
            return String::new();
        }
        if described.is_empty() {
            let tokens: Vec<String> = emojis.into_iter().map(|emoji| emoji.token).collect();
            return format!("\n- Custom emojis of this server (exact forms): {}", tokens.join(" "));
        }
        emojis.sort_by(|a, b| a.name.cmp(&b.name));
        let lines: Vec<String> = emojis
            .iter()
            .map(|emoji| match described.get(&emoji.name) {
                Some(description) => format!("  {} \u{2014} {description}", emoji.token),
                None => format!("  {}", emoji.token),
            })
            .collect();
        format!("\n- Custom emojis of this server (exact forms):\n{}", lines.join("\n"))
    }

    /// The emoji whitelist in effect for this channel: the channel's own
    /// list when non-empty, otherwise the guild-wide one. Unreadable docs
    /// degrade to empty - the menu just stays off, by the cosmetic-failure
    /// rule.
    async fn effective_emoji_whitelist(
        &self,
        origin: &Origin,
        services: &KernelServices,
    ) -> Vec<EmojiWhitelistEntry> {
        let Some(storage) = &services.guild_storage else {
            return Vec::new();
        };
        let channel = match storage
            .get(NAMESPACE, &channel_emoji_whitelist_key(origin.channel_id.get()))
            .await
        {
            Ok(Some(raw)) => Some(parse_whitelist(raw)),
            Ok(None) => None,
            Err(err) => {
                tracing::debug!(
                    channel = origin.channel_id.get(),
                    %err,
                    "channel emoji whitelist unreadable - menu stays off"
                );
                None
            }
        };
        if let Some(list) = channel.filter(|list| !list.is_empty()) {
            return list;
        }
        match storage.get(NAMESPACE, GUILD_EMOJI_WHITELIST_KEY).await {
            Ok(Some(raw)) => parse_whitelist(raw),
            Ok(None) => Vec::new(),
            Err(err) => {
                tracing::debug!(
                    channel = origin.channel_id.get(),
                    %err,
                    "guild emoji whitelist unreadable - menu stays off"
                );
                Vec::new()
            }
        }
    }

    fn assemble_prompt(
        &self,
        config: &ChannelConfig,
        state: &ConversationState,
        live: &[ConversationRecord],
        usage_stats: &UsageStats,
        vars: &PromptVars,
        emoji_menu: &str,
    ) -> (CompletionRequest, u64, Option<u64>, usize) {
        let depth = usize::try_from(config.context_messages).unwrap_or(usize::MAX);
        let skip = live.len().saturating_sub(depth);
        let window: &[ConversationRecord] = live.get(skip..).unwrap_or(live);
        let budget =
            conversation::resolve_budget(config, &self.settings, usage_stats.last.is_some());
        let messages = conversation::assemble_context(
            config,
            &self.settings,
            vars,
            state,
            window,
            usage_stats.tokens_per_char,
            budget,
            emoji_menu,
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
        (request, context_chars, budget, window.len())
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
        typing: Option<ChatTypingGuard>,
    ) -> bool {
        let AnswerRequest { config, state, live, usage_stats, trigger, guild_name } = request;
        // The answer may take tens of seconds: hold the platform typing
        // indicator across generation and delivery. A triggered run arrives
        // with the guard its caller started before the channel lock (the
        // queue wait must read as "composing" too); a chime run types here,
        // after its roll fired. The guard drops on every return path -
        // failure included, the caller's fallback follows right after.
        let _typing = typing.unwrap_or_else(|| services.chat_output_factory.start_typing(origin));
        let channel_id = origin.channel_id.get();
        let vars = self.prompt_vars(guild_name, &config.model);
        let emoji_menu = self.react_emoji_menu(origin, config, services).await;
        let (request, context_chars, budget, window_used) =
            self.assemble_prompt(config, state, live, usage_stats, &vars, &emoji_menu);
        let started = Instant::now();

        let max_length = self.split_limit(config);
        // Streaming channels pull the answer live off the endpoint (SSE
        // deltas reveal on one message); everything else stays single-shot.
        let (live_id, outcome) = if config.streaming {
            self.complete_live(origin, request, services, max_length).await
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
                self.report_stream_interrupted(origin, config, services).await;
                // The usage report died with the stream - the channel's
                // calibration and the delivered characters are all that is
                // left. Estimate, so the totals keep counting real spend.
                let completion_chars = u64::try_from(partial.chars().count()).unwrap_or(u64::MAX);
                let cumulative = self.usage.record(
                    &config.model,
                    origin.guild_id.map(GuildId::get),
                    Sample::estimated(context_chars, completion_chars, usage_stats.tokens_per_char),
                );
                // Reduced audit row: a partial answer WAS delivered (and
                // deliver_reply records it as a bot turn), so the
                // per-completion trail must not skip it. Usage and provider
                // timings died with the stream - chars and wall clock only.
                tracing::info!(
                    channel = channel_id,
                    trigger,
                    model = %config.model,
                    elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    content_chars = partial.chars().count(),
                    window = live.len(),
                    window_used,
                    partial = true,
                    usage_source = "estimated",
                    cumulative_requests = cumulative.requests,
                    cumulative_prompt_tokens = cumulative.prompt_tokens,
                    cumulative_completion_tokens = cumulative.completion_tokens,
                    "LLM answer generated (partial - stream interrupted)"
                );
                return self
                    .deliver_reply(origin, config, channel_id, &partial, live_id, services)
                    .await;
            }
            LiveOutcome::Failed(err) => {
                self.report_completion_failure(origin, config, services, &err).await;
                return false;
            }
        };
        // Totals first (the audit row reports the post-completion
        // cumulative), then the per-channel stats, then the audit itself.
        let usage_source = if response.usage.is_some() { "reported" } else { "estimated" };
        let cumulative = self
            .record_usage(
                services,
                origin,
                channel_id,
                &config.model,
                UsageRecord {
                    usage: response.usage,
                    timing: response.timing,
                    context_chars,
                    completion_chars: u64::try_from(response.content.chars().count())
                        .unwrap_or(u64::MAX),
                    tokens_per_char: usage_stats.tokens_per_char,
                    budget,
                },
            )
            .await;
        // Per-completion audit: usage, latency and context shape.
        Self::log_answer_audit(
            channel_id,
            &AnswerAudit {
                trigger,
                started,
                model: config.model.as_str(),
                usage: response.usage,
                timing: response.timing,
                window: live.len(),
                window_used,
                context_chars,
                calibrated: usage_stats.last.is_some(),
                usage_source,
                cumulative,
            },
        );

        // Tool protocol (R3/R4): markers never reach the channel or the
        // history - the cleaned text is what is delivered and recorded.
        let (visible, calls) = tools::extract_tool_calls(&response.content);
        // Reactions fire only where the channel opted in (the prompt taught
        // the tool); marker text is stripped regardless - a hallucinated
        // marker must not reach the channel either way.
        let reactions = if config.react {
            tools::react_tokens_from(&calls, self.settings.react_max_per_message)
        } else {
            Vec::new()
        };

        if visible.is_empty() {
            // Marker-only answer: the reactions ARE the response. A triggered
            // message still got its visible response (the emoji), so this
            // counts as answered - no fallback, and no bot turn recorded
            // (nothing textual was shown). Without reactions this is the
            // no-answer path and the caller's fallback applies.
            if reactions.is_empty() {
                tracing::warn!(
                    channel = channel_id,
                    model = %config.model,
                    trigger,
                    "completion produced no deliverable content - falling back"
                );
                return false;
            }
            tracing::debug!(
                channel = channel_id,
                count = reactions.len(),
                "marker-only answer - reacting without a reply"
            );
            self.apply_reactions(origin, &reactions, services).await;
            return true;
        }

        let delivered =
            self.deliver_reply(origin, config, channel_id, &visible, live_id, services).await;
        // Reactions decorate an already-delivered answer (R5): cosmetic,
        // never fatal, skipped entirely when nothing was delivered.
        if delivered && !reactions.is_empty() {
            self.apply_reactions(origin, &reactions, services).await;
        }
        delivered
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
        max_length: usize,
    ) -> (Option<MessageId>, LiveOutcome) {
        let (tx, mut rx) = mpsc::channel::<String>(32);
        let mut completion = Box::pin(self.completion.complete_streaming(request, tx));
        let mut reveal = LiveReveal::new(
            services.chat_output_factory.stream_output(origin),
            origin.channel_id.get(),
            origin.message_id,
            Duration::from_millis(self.settings.stream_interval_ms.max(1)),
            max_length,
        );

        let outcome = loop {
            tokio::select! {
                biased;
                delta = rx.recv() => match delta {
                    Some(delta) => reveal.push(&delta).await,
                    // The adapter dropped the sender: the authoritative
                    // result is ready.
                    None => break live_outcome(completion.as_mut().await, &mut reveal),
                },
                res = &mut completion => {
                    // The adapter's future can resolve before the engine's
                    // first recv poll (tiny streams, single-poll fakes):
                    // drain the buffered deltas first - the outcome mapping
                    // depends on knowing whether content was on screen.
                    reveal.drain(&mut rx).await;
                    break live_outcome(res, &mut reveal);
                }
            }
        };
        (reveal.message, outcome)
    }

    /// A completion that failed before anything was shown: error log plus
    /// service-channel notice. The caller owes the channel the fallback.
    async fn report_completion_failure(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        services: &KernelServices,
        err: &LlmError,
    ) {
        tracing::error!(
            channel = origin.channel_id.get(),
            model = %config.model,
            %err,
            "LLM completion failed - no generated reply"
        );
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
    }

    /// A stream that died after content was revealed: error log plus
    /// service-channel notice. The partial text is delivered as-is - the
    /// fallback notice would contradict what the channel already sees.
    async fn report_stream_interrupted(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        services: &KernelServices,
    ) {
        tracing::error!(
            channel = origin.channel_id.get(),
            model = %config.model,
            "LLM stream interrupted - partial answer delivered"
        );
        if let Some(storage) = &services.guild_storage {
            self.notify_service(
                services,
                origin,
                storage,
                "LLM stream interrupted",
                format!(
                    "Model `{}`: the connection dropped mid-answer; a partial reply was \
                     delivered.",
                    config.model
                ),
                true,
            )
            .await;
        }
    }

    /// Delivery half of [`ChatEngine::answer`]: splits the reply to the
    /// channel's length cap (on line boundaries), delivers it, and records
    /// the bot turn. The first message natively replies to the triggering
    /// one (`origin.message_id`) - both for mentions and for random
    /// chime-ins; follow-up split parts go out plain. `live_id` carries an
    /// already-streaming message from [`Self::complete_live`] - its final
    /// edit pins the exact first chunk (the reference was set at its
    /// `begin`); without one, `begin` posts the first chunk directly.
    /// The effective reply-splitting limit for a channel: the per-channel
    /// override, else the plugin-wide default (already clamped to the
    /// platform's cap at boot).
    fn split_limit(&self, config: &ChannelConfig) -> usize {
        config.split_length.unwrap_or(self.settings.max_message_length)
    }

    /// Delivers the authoritative reply: splits at the channel's limit,
    /// pins the first chunk (as a reply anchor when live-streaming, else as
    /// the plain `begin`), then sends the remaining parts. The live
    /// already-streaming message from [`Self::complete_live`] - its final
    /// edit pins the exact first chunk (the reference was set at its
    /// `begin`); without one, `begin` posts the first chunk directly.
    /// Returns whether delivery could start - the caller's fallback follows
    /// when it did not.
    async fn deliver_reply(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        channel_id: u64,
        content: &str,
        live_id: Option<MessageId>,
        services: &KernelServices,
    ) -> bool {
        let max_length = self.split_limit(config);
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
        let mut delivered = false;
        let first_message_id: Option<u64> = if let Some(id) = live_id {
            // The live message already shows prefixes of the answer; the
            // final edit pins it to the authoritative first chunk (reasoning
            // stripping may have shortened the raw deltas).
            if let Err(err) = stream.update(id, first_chunk.clone()).await {
                // The edit failed: the screen keeps the last revealed
                // prefix, so the un-revealed tail of this chunk would be
                // lost. Plain-send the full chunk instead - a duplicated
                // prefix beats a silently truncated answer. The duplicate
                // is neither the reply anchor (the live message is) nor
                // recorded twice (history holds the content once).
                tracing::warn!(
                    channel = channel_id,
                    %err,
                    "final stream update failed - sending the full first chunk plainly"
                );
                let resent = OutboundMessage::text(first_chunk.clone());
                if let Err(err) = services.chat_output.send(resent).await {
                    tracing::warn!(
                        channel = channel_id,
                        %err,
                        "plain resend of the first chunk failed too"
                    );
                }
            }
            delivered = true; // the live begin already put content on screen
            Some(id.get())
        } else {
            let mut begin = OutboundMessage::text(first_chunk.clone());
            if let Some(reply_to) = origin.message_id {
                begin = begin.replying_to(reply_to);
            }
            match stream.begin(begin).await {
                Ok(id) => {
                    delivered = true;
                    Some(id.get())
                }
                Err(err) => {
                    tracing::warn!(
                        channel = channel_id,
                        %err,
                        "stream begin failed - falling back to plain sends"
                    );
                    None
                }
            }
        };

        let delivered_first = usize::from(first_message_id.is_some());
        for (index, chunk) in chunks.iter().skip(delivered_first).enumerate() {
            let mut part = OutboundMessage::text(chunk.clone());
            // The degradation path keeps the reply anchor: when `begin`
            // failed, the first plain part carries the reference the
            // anchored path would have shown.
            if index == 0
                && first_message_id.is_none()
                && let Some(reply_to) = origin.message_id
            {
                part = part.replying_to(reply_to);
            }
            match services.chat_output.send(part).await {
                Ok(()) => delivered = true,
                Err(err) => {
                    tracing::warn!(channel = channel_id, %err, "failed to deliver reply part");
                }
            }
        }

        // Delivered = recorded: a turn the channel never saw must not enter
        // the history (a later reply to it would trigger an answer nobody
        // can see). Returning false hands the triggered-message case to the
        // caller's fallback; a total delivery failure is an infrastructure
        // outage, not a silent-answer path.
        if !delivered {
            tracing::error!(
                channel = channel_id,
                "reply delivery failed on every path - not recording a phantom bot turn"
            );
            return false;
        }

        let assistant = ConversationRecord {
            message_id: first_message_id,
            role: RecordRole::Assistant,
            author: None,
            sender_id: None,
            guild_name: None,
            content: content.to_owned(),
            reply_to: origin.message_id.map(MessageId::get),
            captured_at: unix_now(),
            images: Vec::new(),
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
    /// to `GlitchTip` as a log item).
    fn log_answer_audit(channel_id: u64, audit: &AnswerAudit<'_>) {
        let AnswerAudit {
            trigger,
            started,
            model,
            usage,
            timing,
            window,
            window_used,
            context_chars,
            calibrated,
            usage_source,
            cumulative,
        } = *audit;
        tracing::info!(
            channel = channel_id,
            model,
            trigger,
            elapsed_ms = started.elapsed().as_millis(),
            provider_ms = timing.total_ms,
            provider_timed = timing.endpoint_reported,
            prompt_tokens = usage.map_or(0, |usage| usage.prompt_tokens),
            completion_tokens = usage.map_or(0, |usage| usage.completion_tokens),
            cached_tokens = ?usage.and_then(|usage| usage.cached_tokens),
            reasoning_tokens = ?usage.and_then(|usage| usage.reasoning_tokens),
            usage_source,
            cumulative_requests = cumulative.requests,
            cumulative_prompt_tokens = cumulative.prompt_tokens,
            cumulative_completion_tokens = cumulative.completion_tokens,
            window,
            window_used,
            context_chars,
            calibrated,
            "LLM answer generated"
        );
    }

    /// Random chime decisions for one captured, non-triggering message: two
    /// INDEPENDENT rolls (reply and silent react), each with its own
    /// cooldown tracker behind the channel's `random_cooldown_secs` and its
    /// own RNG deck (purpose-separated scopes - shared decks would rebuild
    /// on every alternating draw and defeat the balancing). They coexist in
    /// parallel - a react neither consumes nor suppresses a reply - and when
    /// both fire, one LLM call serves both (the reply carries the reaction
    /// markers). Silent on every negative decision - only the roll traces
    /// at debug explain why the bot stayed quiet.
    async fn maybe_chime(
        &self,
        origin: &Origin,
        mut request: AnswerRequest<'_>,
        services: &KernelServices,
    ) {
        let config = request.config;
        let reply_scope = RandomScope {
            platform: self.platform_info.slug(),
            guild_id: origin.guild_id.map_or(0, GuildId::get),
            channel_id: origin.channel_id.get(),
            purpose: "reply",
        };
        let react_scope = RandomScope { purpose: "react", ..reply_scope };
        let reply_allowed = config.random_reply_chance_percent > 0.0
            && self.chime_allowed(origin, config.random_cooldown_secs);
        let react_allowed = config.react
            && config.random_react_chance_percent > 0.0
            && self.react_chime_allowed(origin, config.random_cooldown_secs);

        let mut reply_fire = false;
        if reply_allowed {
            reply_fire = self.rng.chance_percent(reply_scope, config.random_reply_chance_percent);
            tracing::debug!(
                channel = origin.channel_id.get(),
                chance = config.random_reply_chance_percent,
                rolled = reply_fire,
                "chime roll"
            );
        } else if config.random_reply_chance_percent > 0.0 {
            tracing::debug!(channel = origin.channel_id.get(), "chime roll suppressed by cooldown");
        }
        let mut react_fire = false;
        if react_allowed {
            react_fire = self.rng.chance_percent(react_scope, config.random_react_chance_percent);
            tracing::debug!(
                channel = origin.channel_id.get(),
                chance = config.random_react_chance_percent,
                rolled = react_fire,
                "react chime roll"
            );
        } else if config.react && config.random_react_chance_percent > 0.0 {
            tracing::debug!(
                channel = origin.channel_id.get(),
                "react chime roll suppressed by cooldown"
            );
        }

        if reply_fire {
            self.note_chime(origin);
            // Both rolls fired: the answer's own markers react - no separate
            // silent call. Both cooldowns are noted (both rolls happened).
            if react_fire {
                self.note_react_chime(origin);
            }
            request.trigger = "chime";
            self.answer(origin, request, services, None).await;
        } else if react_fire {
            self.note_react_chime(origin);
            self.react_chime(origin, request, services).await;
        }
    }

    /// The silent-react chime: one single-shot completion whose prose is
    /// DISCARDED - only reaction markers act. Never streamed (there is
    /// nothing to reveal), never delivered, never recorded as a bot turn
    /// ("delivered = recorded" forbids recording what nobody saw); usage
    /// still records, the call was real. Unprompted, so every failure stays
    /// silent: no marker, no reaction, no trace in the channel.
    async fn react_chime(
        &self,
        origin: &Origin,
        request: AnswerRequest<'_>,
        services: &KernelServices,
    ) {
        let AnswerRequest { config, state, live, usage_stats, guild_name, .. } = request;
        let channel_id = origin.channel_id.get();
        let vars = self.prompt_vars(guild_name, &config.model);
        let emoji_menu = self.react_emoji_menu(origin, config, services).await;
        let (mut request, context_chars, budget, _) =
            self.assemble_prompt(config, state, live, usage_stats, &vars, &emoji_menu);
        // This invocation is a reaction decision, not a reply: without a
        // dedicated instruction the model answers conversationally, the
        // prose is discarded, and markers stay rare. Appended last so the
        // context prefix stays byte-stable for provider caches.
        request.messages.push(ChatMessage::text(ChatRole::System, tools::REACT_CHIME_PROMPT));
        let response = match self.completion.complete(request).await {
            Ok(response) => response,
            Err(err) => {
                tracing::debug!(channel = channel_id, %err, "react chime completion failed");
                return;
            }
        };
        self.record_usage(
            services,
            origin,
            channel_id,
            &config.model,
            UsageRecord {
                usage: response.usage,
                timing: response.timing,
                context_chars,
                completion_chars: u64::try_from(response.content.chars().count())
                    .unwrap_or(u64::MAX),
                tokens_per_char: usage_stats.tokens_per_char,
                budget,
            },
        )
        .await;

        let (visible, calls) = tools::extract_tool_calls(&response.content);
        let reactions = tools::react_tokens_from(&calls, self.settings.react_max_per_message);
        if reactions.is_empty() {
            tracing::debug!(
                channel = channel_id,
                trigger = "react_chime",
                "react chime produced no marker - staying silent"
            );
            return;
        }
        if !visible.is_empty() {
            tracing::debug!(
                channel = channel_id,
                chars = visible.chars().count(),
                "react chime prose discarded - silent reaction only"
            );
        }
        tracing::debug!(
            channel = channel_id,
            trigger = "react_chime",
            count = reactions.len(),
            "react chime applied"
        );
        self.apply_reactions(origin, &reactions, services).await;
    }

    /// Fires the reaction tokens against the message this event's reply
    /// anchors to (the triggering message for mentions and chime-ins
    /// alike). Cosmetic per the port contract: failures are per-token,
    /// debug-logged, and never affect the answer that was already
    /// delivered.
    async fn apply_reactions(
        &self,
        origin: &Origin,
        reactions: &[String],
        services: &KernelServices,
    ) {
        let Some(target) = origin.message_id else {
            return;
        };
        let port = services.chat_output_factory.react(origin);
        for emoji in reactions {
            match port.add_reaction(target, emoji).await {
                Ok(()) => {
                    tracing::debug!(channel = origin.channel_id.get(), emoji = %emoji, "reaction applied");
                }
                Err(err) => {
                    tracing::debug!(
                        channel = origin.channel_id.get(),
                        emoji = %emoji,
                        %err,
                        "reaction skipped"
                    );
                }
            }
        }
    }

    /// The guaranteed-answer contract: a message that explicitly addresses
    /// the bot (a mention, or a reply to a bot turn) never disappears
    /// silently. When the generated answer is impossible - provider failure,
    /// reasoning-only response, untrustworthy history - the channel gets
    /// this generic notice instead, natively replying to the triggering
    /// message so it is unambiguous what failed. It is never recorded as a
    /// bot turn and never carries error detail.
    pub(crate) async fn send_fallback(&self, origin: &Origin, services: &KernelServices) {
        let mut notice = OutboundMessage::text(FALLBACK_MESSAGE.to_owned());
        if let Some(reply_to) = origin.message_id {
            notice = notice.replying_to(reply_to);
        }
        if let Err(err) = services.chat_output.send(notice).await {
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
    /// than bricking the channel until an admin intervenes - but salvages
    /// the cutoff when it parses: a corrupt summary must not resurrect
    /// compacted history, only the summary text is lost.
    async fn load_state(
        &self,
        storage: &Arc<dyn GuildStorage>,
        channel_id: u64,
    ) -> Option<ConversationState> {
        match storage.get(NAMESPACE, &channel_state_key(channel_id)).await {
            Ok(Some(raw)) => {
                if let Ok(state) = serde_json::from_value::<ConversationState>(raw.clone()) {
                    return Some(state);
                }
                tracing::warn!(
                    channel = channel_id,
                    "llm conversation state is malformed - starting fresh"
                );
                Some(ConversationState {
                    summary: None,
                    cutoff_seq: raw.get("cutoff_seq").and_then(Value::as_u64).unwrap_or(0),
                    cutoff_at: None,
                })
            }
            Ok(None) => Some(ConversationState::default()),
            Err(err) => {
                tracing::error!(channel = channel_id, %err, "llm conversation state unreadable - skipping message");
                None
            }
        }
    }

    /// The channel's live window: `(seq, record)` pairs after the cutoff,
    /// ascending. Malformed records are skipped (with a warning) instead of
    /// failing the whole window. The window is BOUNDED: the newest `window`
    /// records (depth + compaction tail - everything assembly and compaction
    /// can ever use), filtered to the post-cutoff range. The bound is the
    /// brake that keeps per-message cost flat even when compaction is off or
    /// failing: a log longer than the window serves its newest part, and the
    /// oldest uncompacted records beyond it stay out of the context
    /// (debug-logged). Records are otherwise append-only: the cutoff moves
    /// only through committed compactions, and the single deletion path is
    /// the moderator's [`Self::forget_message`]. An unreadable log is
    /// an error - the caller must skip the message rather than answer from a
    /// degraded context.
    async fn load_live_records(
        &self,
        storage: &Arc<dyn GuildStorage>,
        channel_id: u64,
        after_seq: u64,
        window: usize,
    ) -> Result<Vec<(u64, ConversationRecord)>, crate::kernel::models::StorageError> {
        let records_ns = records_namespace(channel_id);
        let limit = u32::try_from(window.max(1)).unwrap_or(u32::MAX);
        let stored = storage.list_last(&records_ns, limit).await?;
        let mut skipped = 0usize;
        let live: Vec<(u64, ConversationRecord)> = stored
            .into_iter()
            .filter(|record| record.seq > after_seq)
            .filter_map(|record| {
                match serde_json::from_value::<ConversationRecord>(record.payload) {
                    Ok(parsed) => Some((record.seq, parsed)),
                    Err(err) => {
                        skipped += 1;
                        tracing::warn!(
                            channel = channel_id,
                            %err,
                            "skipping malformed conversation record"
                        );
                        None
                    }
                }
            })
            .collect();
        if skipped > 0 {
            // A dropped record silently shrinks the window - the summary and
            // the depth budget both assume it is there. Naming the effective
            // size keeps the shrink visible instead of
            // discovered-much-later.
            tracing::warn!(
                channel = channel_id,
                skipped,
                kept = live.len(),
                "live window loaded with malformed records - the effective window is smaller"
            );
        }
        if live.len() >= window.max(1) {
            tracing::debug!(
                channel = channel_id,
                window,
                "live log at the operational window cap - older uncompacted records are \
                 outside the context"
            );
        }
        Ok(live)
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

    /// Moderator removal (`/llm_forget`): deletes every stored record of
    /// this channel carrying `message_id` and returns their sequence
    /// numbers (empty = nothing stored under that id). The ONE path that
    /// removes conversation records - a tombstone would keep the content
    /// in storage, defeating the moderation purpose. Must run under the
    /// channel's processing lock like every other state mutation. Records
    /// already below the compaction cutoff stay deleted from storage, but
    /// their gist may survive in the committed summary - the caller owns
    /// that caveat.
    pub async fn forget_message(
        &self,
        storage: &Arc<dyn GuildStorage>,
        channel_id: u64,
        message_id: u64,
    ) -> Result<Vec<u64>, crate::kernel::models::StorageError> {
        let records_ns = records_namespace(channel_id);
        // Paged whole-log scan: removal is a rare moderator action and must
        // find the message regardless of age, cutoff, or window bounds -
        // but materializing the entire log at once would spike memory on a
        // long-lived channel. Forward pagination is unaffected by the
        // deletions behind the cursor.
        const PAGE: u32 = 500;
        let mut seqs = Vec::new();
        let mut cursor = 0;
        loop {
            let page = storage.list_after(&records_ns, cursor, PAGE).await?;
            let Some(next) = page.last().map(|record| record.seq) else {
                break;
            };
            let full_page = page.len() >= PAGE as usize;
            for record in page {
                let matches = serde_json::from_value::<ConversationRecord>(record.payload)
                    .inspect_err(|err| {
                        tracing::warn!(
                            channel = channel_id,
                            %err,
                            "skipping malformed conversation record"
                        );
                    })
                    .ok()
                    .and_then(|parsed| parsed.message_id)
                    .is_some_and(|id| id == message_id);
                if matches {
                    storage.delete_record(&records_ns, record.seq).await?;
                    seqs.push(record.seq);
                }
            }
            if !full_page {
                break;
            }
            cursor = next;
        }
        if seqs.is_empty() {
            tracing::debug!(
                channel = channel_id,
                message_id,
                "forget: no stored record carries the id"
            );
        } else {
            tracing::info!(
                channel = channel_id,
                message_id,
                removed = seqs.len(),
                "stored conversation records removed by moderator"
            );
        }
        Ok(seqs)
    }

    /// Folds the oldest live messages into the summary once the window
    /// outgrows `context_messages`: everything but `compaction_keep_tail`
    /// newest records is summarized (via the compaction model) and the
    /// cutoff advances past them. Runs AFTER the reply - the triggering
    /// turn used the pre-compaction context. The commit is one document
    /// write: crash mid-way leaves the old state intact. Compaction is
    /// never a sliding window - the prompt prefix stays byte-stable
    /// between compactions, so provider prompt caches stay warm.
    // The parameter list mirrors the call site's locals one to one; a
    // grouping struct would just be repacked into the same names.
    #[allow(clippy::too_many_arguments)]
    async fn maybe_compact(
        &self,
        origin: &Origin,
        config: &ChannelConfig,
        state: &ConversationState,
        seqs: &[u64],
        records: &[ConversationRecord],
        tokens_per_char: f64,
        guild_name: Option<&str>,
        services: &KernelServices,
    ) {
        if !config.compaction_enabled {
            return;
        }
        let channel_id = origin.channel_id.get();
        let depth = usize::try_from(config.context_messages).unwrap_or(usize::MAX);
        if records.len() <= depth {
            return;
        }
        let keep_tail = usize::try_from(self.settings.compaction_keep_tail).unwrap_or(usize::MAX);
        if keep_tail == 0 || keep_tail >= records.len() {
            tracing::warn!(
                channel = channel_id,
                keep_tail = self.settings.compaction_keep_tail,
                live = records.len(),
                "compaction would make no progress - skipping"
            );
            return;
        }

        let split = records.len() - keep_tail;
        let chunk = records.get(..split).unwrap_or(records);
        let chunk_end_seq = seqs.get(split.saturating_sub(1)).copied().unwrap_or(0);
        let model = config
            .compaction_model
            .clone()
            .or_else(|| self.settings.compaction_model.clone())
            .unwrap_or_else(|| config.model.clone());
        // The summarizer model executes this prompt - `{{model}}` names it,
        // not the channel's chat model.
        let vars = self.prompt_vars(guild_name, &model);
        let resolved = config
            .compaction_prompt
            .clone()
            .unwrap_or_else(|| self.settings.default_compaction_prompt.clone());
        let prompt = render_prompt(&resolved, &vars);

        let messages = conversation::compaction_input(&prompt, state.summary.as_deref(), chunk);
        #[allow(clippy::cast_precision_loss)] // estimator: precision loss is fine
        let context_chars: u64 =
            messages.iter().map(|message| message.content.chars().count() as u64).sum();
        // The summarizer call is deliberately NOT recorded into the channel's
        // chat stats: the EWMA ratio and the "last request" report must
        // reflect chat completions only, not compaction traffic. The global
        // usage totals DO count it - compaction is real spend.
        let request = CompletionRequest {
            model: model.clone(),
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
        // Global totals: endpoint numbers when reported, the channel's
        // calibration as the estimate otherwise.
        let completion_chars = u64::try_from(response.content.chars().count()).unwrap_or(u64::MAX);
        let reported = response.usage.is_some();
        let sample =
            response.usage.as_ref().map(Sample::reported).unwrap_or_else(|| {
                Sample::estimated(context_chars, completion_chars, tokens_per_char)
            });
        let cumulative = self.usage.record(&model, origin.guild_id.map(GuildId::get), sample);
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
                chunk.len(),
                keep_tail,
                CompactionAudit { model, sample, reported, cumulative },
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
        audit: CompactionAudit,
        services: &KernelServices,
    ) {
        let channel_id = origin.channel_id.get();
        // A serialization failure must abort the commit: persisting `null`
        // would silently reset the channel's conversation state on next
        // load. Same guild-visible treatment as a storage error.
        let value = match serde_json::to_value(&new_state) {
            Ok(value) => value,
            Err(err) => {
                tracing::error!(
                    channel = channel_id,
                    %err,
                    "failed to serialize compacted state - not committed"
                );
                self.notify_service(
                    services,
                    origin,
                    storage,
                    "LLM compaction failed",
                    "Could not persist the summary (serialization error) - the window keeps \
                         growing; check the bot logs."
                        .to_owned(),
                    true,
                )
                .await;
                return;
            }
        };
        match storage.set(NAMESPACE, &channel_state_key(channel_id), value).await {
            Ok(()) => {
                tracing::info!(
                    channel = channel_id,
                    folded,
                    cutoff = new_state.cutoff_seq,
                    model = %audit.model,
                    prompt_tokens = audit.sample.prompt_tokens,
                    completion_tokens = audit.sample.completion_tokens,
                    usage_source = if audit.reported { "reported" } else { "estimated" },
                    cumulative_requests = audit.cumulative.requests,
                    cumulative_prompt_tokens = audit.cumulative.prompt_tokens,
                    cumulative_completion_tokens = audit.cumulative.completion_tokens,
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
                // Same privacy rule as LLM errors: the guild-visible embed
                // carries the classification only - driver/SQL error text
                // can name infrastructure internals; the raw error stays in
                // the log line above.
                self.notify_service(
                    services,
                    origin,
                    storage,
                    "LLM compaction failed",
                    "Could not persist the summary (storage error) - the window keeps growing; \
                         check the bot logs."
                        .to_owned(),
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
        let key = channel_key(self.platform_info.slug(), origin.guild_id, service_channel);
        let mut notices = self.notices.lock();
        match notices.get(&key) {
            Some(last) if last.elapsed() < NOTICE_COOLDOWN => {
                tracing::debug!(
                    channel = origin.channel_id.get(),
                    "error notice suppressed by cooldown"
                );
                false
            }
            _ => {
                notices.insert(key, Instant::now());
                true
            }
        }
    }

    fn chime_allowed(&self, origin: &Origin, cooldown_secs: u64) -> bool {
        let key = channel_key(self.platform_info.slug(), origin.guild_id, origin.channel_id.get());
        let cooldown = Duration::from_secs(cooldown_secs);
        !matches!(self.chimes.lock().get(&key), Some(last) if last.elapsed() < cooldown)
    }

    fn note_chime(&self, origin: &Origin) {
        let key = channel_key(self.platform_info.slug(), origin.guild_id, origin.channel_id.get());
        self.chimes.lock().insert(key, Instant::now());
    }

    /// Silent-react chime cooldown - the same `random_cooldown_secs`
    /// duration as the reply chime, but an independent tracker.
    fn react_chime_allowed(&self, origin: &Origin, cooldown_secs: u64) -> bool {
        let key = channel_key(self.platform_info.slug(), origin.guild_id, origin.channel_id.get());
        let cooldown = Duration::from_secs(cooldown_secs);
        !matches!(self.react_chimes.lock().get(&key), Some(last) if last.elapsed() < cooldown)
    }

    fn note_react_chime(&self, origin: &Origin) {
        let key = channel_key(self.platform_info.slug(), origin.guild_id, origin.channel_id.get());
        self.react_chimes.lock().insert(key, Instant::now());
    }

    /// Persists the last chat completion's response time and - when the
    /// endpoint reports usage - token stats, blending the observed
    /// tokens-per-character ratio into the channel's estimate. The same
    /// completion feeds the plugin-global totals: provider numbers when
    /// reported, the channel's calibration as the estimate otherwise.
    /// Compaction records only into the global totals (its transcript is not
    /// chat traffic). Best effort: a failed write only degrades the next
    /// estimate back to the previous ratio. A usage-less response keeps the
    /// previously reported usage: dropping it would silently disable
    /// token-budget context filling (the budget gate keys on a stored usage)
    /// after one omitted report. Returns the model's updated cumulative
    /// totals for the audit row.
    async fn record_usage(
        &self,
        services: &KernelServices,
        origin: &Origin,
        channel_id: u64,
        model: &str,
        record: UsageRecord,
    ) -> Dimension {
        let UsageRecord { usage, timing, context_chars, completion_chars, tokens_per_char, budget } =
            record;
        let sample = usage
            .as_ref()
            .map(Sample::reported)
            .unwrap_or_else(|| Sample::estimated(context_chars, completion_chars, tokens_per_char));
        let cumulative = self.usage.record(model, origin.guild_id.map(GuildId::get), sample);
        let Some(storage) = &services.guild_storage else {
            return cumulative;
        };
        let (last, tokens_per_char) = match usage {
            Some(usage) => {
                (Some(usage), blend_ratio(tokens_per_char, usage.prompt_tokens, context_chars))
            }
            None => (self.load_stats(storage, channel_id).await.last, tokens_per_char),
        };
        let stats =
            UsageStats { last, last_timing: Some(timing), tokens_per_char, last_budget: budget };
        // Same rule as the state doc: a serialization failure aborts the
        // persist instead of storing `null` (which would reset the stats).
        let value = match serde_json::to_value(&stats) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(
                    channel = channel_id,
                    %err,
                    "failed to serialize usage stats - not persisted"
                );
                return cumulative;
            }
        };
        if let Err(err) = storage.set(NAMESPACE, &channel_stats_key(channel_id), value).await {
            tracing::warn!(channel = channel_id, %err, "failed to persist token usage stats");
        }
        cumulative
    }

    /// The channel's calibration state; unreadable stats fall back to the
    /// default ratio (stats are observability, never reply-blocking).
    async fn load_stats(&self, storage: &Arc<dyn GuildStorage>, channel_id: u64) -> UsageStats {
        match storage.get(NAMESPACE, &channel_stats_key(channel_id)).await {
            Ok(Some(raw)) => serde_json::from_value(raw).unwrap_or_else(|err| {
                tracing::debug!(
                    channel = channel_id,
                    %err,
                    "usage stats malformed - default estimate applies"
                );
                UsageStats::default()
            }),
            Ok(None) => UsageStats::default(),
            Err(err) => {
                tracing::debug!(
                    channel = channel_id,
                    %err,
                    "usage stats unreadable - default estimate applies"
                );
                UsageStats::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{
        AttachmentPayload, ChannelId, GuildId, MessageId, OutboundError, StorageError, UserId,
    };
    use crate::kernel::spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, ChatTypingGuard, GuildEmojiPort,
        GuildStorage, ReactableEmoji, ReactionPort, StoragePort, StoredRecord,
    };
    use crate::plugins::llm::completion_port::{
        ChatRole, CompletionResponse, LlmError, ResponseTiming, TokenUsage,
    };
    use crate::plugins::llm::model::{CaptureMode, EmojiInject, channel_config_key};
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
                || {
                    Ok(CompletionResponse {
                        content: "canned".to_owned(),
                        usage,
                        timing: ResponseTiming::measured(0),
                    })
                },
                |response| {
                    response.map(|content| CompletionResponse {
                        content,
                        usage,
                        timing: ResponseTiming::measured(0),
                    })
                },
            )
        }
    }

    /// Factory whose stream port succeeds - mirrors the Discord adapter's
    /// `begin`-returns-a-handle behavior the engine relies on.
    struct StreamRecordingFactory {
        begins: Arc<Mutex<Vec<OutboundMessage>>>,
        updates: Arc<Mutex<Vec<String>>>,
        output: Arc<RecordingChatOutput>,
        typing_starts: AtomicUsize,
        reactions: Arc<Mutex<Vec<(u64, String)>>>,
        /// The custom emojis the emoji-listing port serves (default: none).
        emojis: Vec<ReactableEmoji>,
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

        fn react(&self, _origin: &Origin) -> Arc<dyn ReactionPort> {
            Arc::new(RecordingReactionPort { reactions: Arc::clone(&self.reactions) })
        }

        fn reactable_emojis(&self, _origin: &Origin) -> Arc<dyn GuildEmojiPort> {
            Arc::new(StaticEmojis(self.emojis.clone()))
        }
    }

    /// Serves a fixed emoji list - the prompt-injection source for tests.
    struct StaticEmojis(Vec<ReactableEmoji>);

    #[async_trait]
    impl GuildEmojiPort for StaticEmojis {
        async fn list(&self) -> Vec<ReactableEmoji> {
            self.0.clone()
        }
    }

    /// Records `(message_id, emoji)` pairs the engine fired.
    struct RecordingReactionPort {
        reactions: Arc<Mutex<Vec<(u64, String)>>>,
    }

    #[async_trait]
    impl ReactionPort for RecordingReactionPort {
        async fn add_reaction(
            &self,
            message_id: MessageId,
            emoji: &str,
        ) -> Result<(), OutboundError> {
            self.reactions.lock().push((message_id.get(), emoji.to_owned()));
            Ok(())
        }
    }

    /// Every reaction fails - the answer must not notice (R5).
    struct FailingReactionPort;

    #[async_trait]
    impl ReactionPort for FailingReactionPort {
        async fn add_reaction(
            &self,
            _message_id: MessageId,
            _emoji: &str,
        ) -> Result<(), OutboundError> {
            Err(OutboundError::Reaction("custom emoji not found in this server".to_owned()))
        }
    }

    /// Delivering output + failing reactions: the fixture for the R5 rule
    /// that reaction failures never touch the answer.
    struct FailingReactionFactory {
        output: Arc<RecordingChatOutput>,
    }

    #[async_trait]
    impl ChatOutputFactoryPort for FailingReactionFactory {
        fn chat_output(&self, _origin: &Origin) -> Arc<dyn ChatOutputPort> {
            Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
        }

        fn start_typing(&self, _origin: &Origin) -> ChatTypingGuard {
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
                begins: Arc::new(Mutex::new(Vec::new())),
                updates: Arc::new(Mutex::new(Vec::new())),
                counter: AtomicU64::new(100),
            })
        }

        fn react(&self, _origin: &Origin) -> Arc<dyn ReactionPort> {
            Arc::new(FailingReactionPort)
        }

        fn message_link(
            &self,
            _origin: &Origin,
            _channel_id: ChannelId,
            _message_id: MessageId,
        ) -> Option<String> {
            None
        }
    }

    struct RecordingStream {
        begins: Arc<Mutex<Vec<OutboundMessage>>>,
        updates: Arc<Mutex<Vec<String>>>,
        counter: AtomicU64,
    }

    #[async_trait]
    impl ChatStreamPort for RecordingStream {
        async fn begin(&self, message: OutboundMessage) -> Result<MessageId, OutboundError> {
            self.begins.lock().push(message);
            Ok(MessageId(self.counter.fetch_add(1, Ordering::Relaxed)))
        }

        async fn update(&self, _message: MessageId, content: String) -> Result<(), OutboundError> {
            self.updates.lock().push(content);
            Ok(())
        }
    }

    /// Every posted reveal edit fits the platform cap: the live message is
    /// always a prefix of the revealed text, never an oversized edit the
    /// platform would reject (the final delivery splits authoritatively).
    #[tokio::test]
    async fn live_reveal_clamps_every_posted_edit_to_the_cap() {
        let begins = Arc::new(Mutex::new(Vec::new()));
        let updates = Arc::new(Mutex::new(Vec::new()));
        let stream = Arc::new(RecordingStream {
            begins: Arc::clone(&begins),
            updates: Arc::clone(&updates),
            counter: AtomicU64::new(1),
        });
        let mut reveal = LiveReveal::new(stream, 7, None, Duration::from_millis(0), 10);

        reveal.push("0123456789ABCDE").await;
        reveal.push("FGH").await;
        reveal.push("\u{1f600}").await;

        let begin = begins.lock().first().expect("begin expected").clone();
        assert_eq!(begin.content, "0123456789", "the begin is clamped to the cap");
        let recorded = updates.lock().clone();
        assert!(!recorded.is_empty(), "at least one edit expected");
        for edit in &recorded {
            assert!(conversation::utf16_len(edit) <= 10, "edit over the cap: {edit:?}");
        }
        assert_eq!(recorded.last(), Some(&"0123456789".to_owned()));
    }

    /// Deterministic RNG for chime-in tests.
    struct FixedRandom(bool);

    #[async_trait]
    impl RandomPort for FixedRandom {
        fn chance_percent(&self, _scope: RandomScope, _percent: f64) -> bool {
            self.0
        }
    }

    /// Capture-path fake: records describe jobs (model ref, prompt, image
    /// count, effective max side) and hands out canned descriptions in
    /// order; exhausted queues yield undescribed images.
    #[derive(Default)]
    struct FakeDescriber {
        jobs: Mutex<Vec<(String, String, usize, u32)>>,
        results: Mutex<VecDeque<Option<String>>>,
    }

    impl FakeDescriber {
        fn with_results(results: Vec<Option<&str>>) -> Self {
            Self {
                jobs: Mutex::new(Vec::new()),
                results: Mutex::new(
                    results.into_iter().map(|line| line.map(str::to_owned)).collect(),
                ),
            }
        }

        fn job_count(&self) -> usize {
            self.jobs.lock().iter().map(|(_, _, count, _)| *count).sum()
        }
    }

    #[async_trait]
    impl ImageDescriber for FakeDescriber {
        async fn describe(
            &self,
            job: &ImageJob,
            images: Vec<ImageSource>,
            _usage: Option<UsageSink<'_>>,
        ) -> Vec<Option<String>> {
            self.jobs.lock().push((
                job.model.clone(),
                job.prompt.clone(),
                images.len(),
                job.max_side,
            ));
            let mut out = Vec::with_capacity(images.len());
            for _ in images {
                out.push(self.results.lock().pop_front().flatten());
            }
            out
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
            Ok(CompletionResponse {
                content: self.chunks.join(""),
                usage: None,
                timing: ResponseTiming::measured(0),
            })
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
            Ok(CompletionResponse {
                content: self.chunks.join(""),
                usage: None,
                timing: ResponseTiming::measured(0),
            })
        }
    }

    /// Wiring for tests that drive the streaming path directly - same
    /// fixtures as [`ctx`], but with a custom completion port.
    struct DeltaCtx {
        engine: ChatEngine,
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        begins: Arc<Mutex<Vec<OutboundMessage>>>,
        updates: Arc<Mutex<Vec<String>>>,
        reactions: ReactionLog,
        services: Arc<KernelServices>,
    }

    fn ctx_delta(settings: LlmSettings, completion: Arc<dyn LlmCompletionPort>) -> DeltaCtx {
        let engine = ChatEngine::new(
            Arc::new(settings),
            completion,
            Arc::new(FixedRandom(false)),
            Arc::new(FakeDescriber::default()),
            crate::test_support::test_platform_info(),
            crate::test_support::test_plugin_storage(),
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
            reactions: Arc::new(Mutex::new(Vec::new())),
            emojis: Vec::new(),
        });
        let services = Arc::new(KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::clone(&factory) as Arc<dyn ChatOutputFactoryPort>,
            guild_storage: Some(storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        });
        DeltaCtx {
            engine,
            storage,
            output,
            begins,
            updates,
            reactions: Arc::clone(&factory.reactions),
            services,
        }
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

        async fn list_last(
            &self,
            _namespace: &str,
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

        async fn delete_record(&self, _namespace: &str, _seq: u64) -> Result<u64, StorageError> {
            Err(StorageError::Database("records unavailable".to_owned()))
        }
    }

    #[async_trait]
    impl StoragePort for RecordsFailStorage {
        fn guild_scoped(&self, platform: &str, guild_id: GuildId) -> Arc<dyn GuildStorage> {
            Arc::new(RecordsFailView { guild: self.documents.guild_scoped(platform, guild_id) })
        }

        async fn list_guilds(&self) -> Result<Vec<(String, GuildId)>, StorageError> {
            self.documents.list_guilds().await
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

        async fn list_last(
            &self,
            namespace: &str,
            limit: u32,
        ) -> Result<Vec<StoredRecord>, StorageError> {
            self.guild.list_last(namespace, limit).await
        }

        async fn count_after(&self, namespace: &str, after_seq: u64) -> Result<u64, StorageError> {
            self.guild.count_after(namespace, after_seq).await
        }

        async fn delete_record(&self, namespace: &str, seq: u64) -> Result<u64, StorageError> {
            self.guild.delete_record(namespace, seq).await
        }
    }

    #[async_trait]
    impl StoragePort for AppendFailStorage {
        fn guild_scoped(&self, platform: &str, guild_id: GuildId) -> Arc<dyn GuildStorage> {
            Arc::new(AppendFailView { guild: self.documents.guild_scoped(platform, guild_id) })
        }

        async fn list_guilds(&self) -> Result<Vec<(String, GuildId)>, StorageError> {
            self.documents.list_guilds().await
        }
    }

    struct TestCtx {
        engine: ChatEngine,
        fake: Arc<FakeCompletion>,
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        begins: Arc<Mutex<Vec<OutboundMessage>>>,
        factory: Arc<StreamRecordingFactory>,
        services: KernelServices,
        describer: Arc<FakeDescriber>,
    }

    /// `(message_id, emoji)` pairs the engine fired through the factory.
    type ReactionLog = Arc<Mutex<Vec<(u64, String)>>>;

    fn reactions_log(factory: &StreamRecordingFactory) -> Vec<(u64, String)> {
        factory.reactions.lock().clone()
    }

    /// Flat text projection of recorded stream begins, for content assertions.
    fn begin_texts(begins: &Mutex<Vec<OutboundMessage>>) -> Vec<String> {
        begins.lock().iter().map(|message| message.content.clone()).collect()
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
        ctx_describer(settings, rng, responses, Arc::new(FakeDescriber::default()))
    }

    fn ctx_describer(
        settings: LlmSettings,
        rng: Arc<dyn RandomPort>,
        responses: Vec<Result<String, LlmError>>,
        describer: Arc<FakeDescriber>,
    ) -> TestCtx {
        ctx_describer_with_emojis(settings, rng, responses, describer, Vec::new())
    }

    /// Same fixture with a scripted server emoji list for the react tool's
    /// prompt injection.
    fn ctx_describer_with_emojis(
        settings: LlmSettings,
        rng: Arc<dyn RandomPort>,
        responses: Vec<Result<String, LlmError>>,
        describer: Arc<FakeDescriber>,
        emojis: Vec<ReactableEmoji>,
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
            Arc::clone(&describer) as Arc<dyn ImageDescriber>,
            crate::test_support::test_platform_info(),
            crate::test_support::test_plugin_storage(),
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
            reactions: Arc::new(Mutex::new(Vec::new())),
            emojis,
        });
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::clone(&factory) as Arc<dyn ChatOutputFactoryPort>,
            guild_storage: Some(storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        TestCtx { engine, fake, storage, output, begins, factory, services, describer }
    }

    /// Total-delivery-outage fixture for the delivered=recorded contract:
    /// every send fails, and the attempt counter proves the fallback was
    /// still attempted.
    #[derive(Default)]
    struct FailingOutput {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl ChatOutputPort for FailingOutput {
        async fn send(&self, _message: OutboundMessage) -> Result<(), OutboundError> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(OutboundError::Send("platform unreachable".to_owned()))
        }
    }

    struct FailingStream;

    #[async_trait]
    impl ChatStreamPort for FailingStream {
        async fn begin(&self, _message: OutboundMessage) -> Result<MessageId, OutboundError> {
            Err(OutboundError::Send("platform unreachable".to_owned()))
        }

        async fn update(&self, _message: MessageId, _content: String) -> Result<(), OutboundError> {
            Err(OutboundError::Send("platform unreachable".to_owned()))
        }
    }

    struct FailingDeliveryFactory {
        output: Arc<FailingOutput>,
    }

    #[async_trait]
    impl ChatOutputFactoryPort for FailingDeliveryFactory {
        fn chat_output(&self, _origin: &Origin) -> Arc<dyn ChatOutputPort> {
            Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
        }

        fn start_typing(&self, _origin: &Origin) -> ChatTypingGuard {
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
            Arc::new(FailingStream)
        }

        fn react(&self, _origin: &Origin) -> Arc<dyn ReactionPort> {
            Arc::new(FailingReactionPort)
        }

        fn message_link(
            &self,
            _origin: &Origin,
            _channel_id: ChannelId,
            _message_id: MessageId,
        ) -> Option<String> {
            None
        }
    }

    /// Begin-fails stream + recording plain output: the degradation path
    /// (`begin` fails, every part goes out as a plain send) with observable
    /// content and reference fields.
    struct DegradedStreamFactory {
        output: Arc<RecordingChatOutput>,
    }

    #[async_trait]
    impl ChatOutputFactoryPort for DegradedStreamFactory {
        fn chat_output(&self, _origin: &Origin) -> Arc<dyn ChatOutputPort> {
            Arc::clone(&self.output) as Arc<dyn ChatOutputPort>
        }

        fn start_typing(&self, _origin: &Origin) -> ChatTypingGuard {
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
            Arc::new(FailingStream)
        }

        fn react(&self, _origin: &Origin) -> Arc<dyn ReactionPort> {
            Arc::new(FailingReactionPort)
        }

        fn message_link(
            &self,
            _origin: &Origin,
            _channel_id: ChannelId,
            _message_id: MessageId,
        ) -> Option<String> {
            None
        }
    }

    /// When the streaming `begin` fails, the plain-send fallback keeps the
    /// reply anchor: the first plain part references the triggering message
    /// exactly like the anchored begin would have.
    #[tokio::test]
    async fn begin_failure_fallback_keeps_the_reply_anchor() {
        let ctx = ctx(vec![Ok("first part, quite long\nsecond part".to_owned())]);
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(DegradedStreamFactory { output: Arc::clone(&output) })
                as Arc<dyn ChatOutputFactoryPort>,
            guild_storage: Some(ctx.storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        let config = ChannelConfig { split_length: Some(12), ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &services, None).await;

        let sent = output.sent();
        assert!(sent.len() >= 2, "the split answer goes out as plain parts");
        assert_eq!(sent.first().expect("parts expected").reply_to, Some(MessageId(77)));
        assert_eq!(
            sent.iter().skip(1).map(|part| part.reply_to).collect::<Vec<_>>(),
            vec![None; sent.len() - 1],
            "only the first part carries the anchor"
        );
    }

    #[tokio::test]
    async fn total_delivery_failure_records_no_phantom_bot_turn() {
        let ctx = ctx(vec![Ok("the answer".to_owned())]);
        let output = Arc::new(FailingOutput::default());
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(FailingDeliveryFactory { output: Arc::clone(&output) })
                as Arc<dyn ChatOutputFactoryPort>,
            guild_storage: Some(ctx.storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &services, None)
            .await;

        // Nothing was delivered - and exactly two sends were attempted:
        // the answer, then the guaranteed-answer fallback.
        assert_eq!(output.attempts.load(Ordering::SeqCst), 2);
        let records = stored_records(&ctx).await;
        assert!(records.iter().all(|record| record.role == RecordRole::User));
    }

    /// The operational-window brake: a log longer than depth + compaction
    /// tail serves its newest part - old records stay out of the context
    /// (and out of memory) even with compaction disabled.
    #[tokio::test]
    async fn live_window_caps_history_loading() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        let config =
            ChannelConfig { context_messages: 2, compaction_enabled: false, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        for seq in 1..=15 {
            append_record(&ctx.storage, &user_record(seq, "alice", &format!("m{seq}"))).await;
        }

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let rendered: Vec<&str> =
            request.messages.iter().map(|message| message.content.as_str()).collect();
        // The newest `context_messages` records (m14 is already beyond depth) + the
        // capture - never the old tail.
        assert!(rendered.iter().any(|text| text.contains("m15")));
        assert!(rendered.iter().any(|text| text.contains("hello bot")));
        assert!(rendered.iter().all(|text| !text.contains("m14")));
        assert!(rendered.iter().all(|text| !text.contains("m13")));
    }

    fn assigned_config() -> ChannelConfig {
        // Chime chance is zero unless a test opts in: the chime-path tests
        // set an explicit percent next to their FixedRandom. Every other
        // untriggered capture then skips the roll entirely - the default
        // ctx() wires a real 2% coin (RandRandom), which made capture tests
        // flake when the coin landed.
        ChannelConfig {
            random_reply_chance_percent: 0.0,
            ..ChannelConfig::assigned("local/gemma".to_owned())
        }
    }

    fn seed_config(storage: &InMemoryStorage, config: &ChannelConfig) {
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            &channel_config_key(2),
            serde_json::to_value(config).expect("config expected to serialize"),
        );
    }

    fn origin() -> Origin {
        Origin {
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
            attachments: Vec::new(),
            author_name: Some("alice".to_owned()),
            guild_name: None,
            author_roles: Vec::new(),
            author_permissions: 0,
            reply_to: reply_to.map(MessageId),
            mentions_bot,
        }
    }

    /// A mention payload carrying image attachments (PNG CDN links).
    fn payload_with_images(urls: &[&str]) -> MessagePayload {
        let mut payload = payload(true, None);
        payload.attachments = urls
            .iter()
            .map(|url| AttachmentPayload {
                url: (*url).to_owned(),
                content_type: Some("image/png".to_owned()),
                file_name: Some("pic.png".to_owned()),
                size_bytes: 100,
                width: Some(10),
                height: Some(10),
            })
            .collect();
        payload
    }

    fn user_record(message_id: u64, author: &str, content: &str) -> ConversationRecord {
        ConversationRecord {
            message_id: Some(message_id),
            role: RecordRole::User,
            author: Some(author.to_owned()),
            sender_id: None,
            guild_name: None,
            content: content.to_owned(),
            reply_to: None,
            captured_at: 0,
            images: Vec::new(),
        }
    }

    async fn append_record(storage: &Arc<InMemoryStorage>, record: &ConversationRecord) {
        let payload = serde_json::to_value(record).expect("record expected to serialize");
        storage
            .guild_scoped("test", GuildId(1))
            .append(&records_namespace(2), payload)
            .await
            .expect("append expected to succeed");
    }

    async fn stored_records(ctx: &TestCtx) -> Vec<ConversationRecord> {
        stored_records_in(&ctx.storage).await
    }

    async fn stored_records_in(storage: &Arc<InMemoryStorage>) -> Vec<ConversationRecord> {
        storage
            .guild_scoped("test", GuildId(1))
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
            "test",
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
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
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
        // New default turn template: the capture-baked tag style (user id 3
        // from the origin).
        assert_eq!(
            request.messages.get(2).map(|m| m.content.as_str()),
            Some("[alice](<@3>): hello bot")
        );

        // Reply delivered via the stream begin - as a native reply to the
        // triggering message (origin message id 77); assistant turn recorded
        // with the returned handle for future reply-chain detection.
        assert_eq!(begin_texts(&ctx.begins), vec!["hi alice".to_owned()]);
        assert_eq!(ctx.begins.lock().first().and_then(|m| m.reply_to), Some(MessageId(77)));
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
        // The record keeps what the answer replied to - the triggering turn.
        assert_eq!(assistant.reply_to, Some(77));
    }

    /// Turn-template fields are baked into the record at capture (same
    /// immutability principle as image descriptions): sender id from the
    /// origin, guild name from the payload, capture time present - the
    /// rendered prompt can never change retroactively.
    #[tokio::test]
    async fn capture_bakes_the_template_fields() {
        let ctx = ctx(vec![Ok("hi".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());
        let mut payload = payload(true, None);
        payload.guild_name = Some("Crafters".to_owned());

        ctx.engine
            .handle_message(&origin(), &payload, &assigned_config(), &ctx.services, None)
            .await;

        let records = stored_records(&ctx).await;
        let user = records.first().expect("user record expected");
        assert_eq!(user.role, RecordRole::User);
        assert_eq!(user.sender_id, Some(3));
        assert_eq!(user.guild_name.as_deref(), Some("Crafters"));
        assert!(user.captured_at > 0, "capture time expected");
    }

    /// Every generated answer holds the platform typing indicator across
    /// generation and delivery - users see the bot composing, not frozen.
    #[tokio::test]
    async fn answer_holds_the_typing_indicator() {
        let ctx = ctx(vec![Ok("hi alice".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        assert_eq!(ctx.factory.typing_starts(), 1);

        // The next answer starts it again - one guard per answer.
        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;
        assert_eq!(ctx.factory.typing_starts(), 2);
    }

    /// A guard the caller started before the channel lock is reused by the
    /// triggered answer - one typing start covers the queue wait and the
    /// generation, no second indicator fires.
    #[tokio::test]
    async fn triggered_answer_reuses_the_caller_started_typing_guard() {
        let ctx = ctx(vec![Ok("hi alice".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());
        let pre_typing = ctx.factory.start_typing(&origin());

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                Some(pre_typing),
            )
            .await;

        assert_eq!(ctx.factory.typing_starts(), 1);
    }

    #[tokio::test]
    async fn unrelated_message_is_ignored_in_bot_related_mode() {
        let ctx = ctx(vec![]);
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(
                &origin(),
                &payload(false, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        assert!(ctx.fake.requests().is_empty());
        assert!(ctx.begins.lock().is_empty());
        assert!(stored_records(&ctx).await.is_empty());
    }

    #[tokio::test]
    async fn all_messages_mode_captures_without_triggering() {
        let ctx = ctx(vec![]);
        let config = ChannelConfig { capture: CaptureMode::AllMessages, ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
            .await;

        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().map(|r| r.role), Some(RecordRole::User));
        assert!(ctx.fake.requests().is_empty());
        assert!(ctx.begins.lock().is_empty());
    }

    fn images_enabled_config(model: &str) -> ChannelConfig {
        let mut config = assigned_config();
        config.images = true;
        config.image_model = Some(model.to_owned());
        config
    }

    #[tokio::test]
    async fn captured_images_are_described_and_baked_into_the_record() {
        let ctx = ctx_describer(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("cute".to_owned())],
            Arc::new(FakeDescriber::with_results(vec![Some("a tabby cat")])),
        );
        let config = images_enabled_config("local/vision");
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/1.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        // The recognition call went to the channel's image model override
        // with the built-in default prompt (no override configured).
        let (model, prompt, count, _) =
            ctx.describer.jobs.lock().first().expect("job expected").clone();
        assert_eq!((model.as_str(), count), ("local/vision", 1));
        assert!(prompt.contains("Describe this image"));
        // The stored record carries the description, rendered into the
        // answer context as a markdown image reference.
        let records = stored_records(&ctx).await;
        assert_eq!(
            records.first().and_then(|r| r.images.first()).map(|image| image.description.clone()),
            Some(Some("a tabby cat".to_owned()))
        );
        let context = ctx.fake.requests().first().expect("chat request expected").clone();
        let turn = context.messages.iter().find(|m| m.role == ChatRole::User).expect("user turn");
        assert!(turn.content.contains("![a tabby cat](image.png)"));
    }

    #[tokio::test]
    async fn channel_image_prompt_override_reaches_the_recognition_call() {
        let ctx = ctx_describer(
            LlmSettings {
                image_prompt: Some("plugin default".to_owned()),
                ..LlmSettings::default()
            },
            Arc::new(RandRandom),
            vec![Ok("ok".to_owned())],
            Arc::new(FakeDescriber::with_results(vec![Some("desc")])),
        );
        let mut config = images_enabled_config("local/vision");
        config.image_prompt = Some("channel prompt".to_owned());
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/1.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        // Channel override > plugin `[llm] image_prompt` > built-in default.
        let (_, prompt, _, _) = ctx.describer.jobs.lock().first().expect("job expected").clone();
        assert_eq!(prompt, "channel prompt");
    }

    /// The per-channel image size meets the operator ceiling at capture
    /// time: stored values above the cap (hand-edited docs, or a cap that
    /// dropped after the value was set) clamp down before reaching the
    /// recognition call.
    #[tokio::test]
    async fn channel_image_max_side_clamps_to_the_operator_cap() {
        let ctx = ctx_describer(
            LlmSettings::default(), // image_max_side_cap: 768
            Arc::new(RandRandom),
            vec![Ok("ok".to_owned())],
            Arc::new(FakeDescriber::with_results(vec![Some("desc")])),
        );
        let mut config = images_enabled_config("local/vision");
        config.image_max_side = 4096; // as a hand-edited doc could carry
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/1.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        let (_, _, _, max_side) = ctx.describer.jobs.lock().first().expect("job expected").clone();
        assert_eq!(max_side, 768);

        // Below the cap the channel value wins untouched.
        let mut config = images_enabled_config("local/vision");
        config.image_max_side = 320;
        seed_config(&ctx.storage, &config);
        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/2.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        let (_, _, _, max_side) = ctx.describer.jobs.lock().last().expect("job expected").clone();
        assert_eq!(max_side, 320);
    }

    #[tokio::test]
    async fn images_off_records_placeholders_without_describe_calls() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        let config = assigned_config(); // images: false (the default)
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/1.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        assert_eq!(ctx.describer.job_count(), 0);
        let records = stored_records(&ctx).await;
        let images = records.first().map(|r| r.images.clone()).unwrap_or_default();
        assert_eq!(images.len(), 1);
        assert_eq!(images.first().and_then(|image| image.description.clone()), None);
    }

    /// The placeholder carries the attachment's real extension, resolved at
    /// capture from the content type - here even undescribed (feature off),
    /// so the model sees the animated-image hint.
    #[tokio::test]
    async fn gif_attachment_bakes_its_extension_into_the_placeholder() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        let config = assigned_config(); // images: false (the default)
        seed_config(&ctx.storage, &config);
        let mut payload = payload(true, None);
        payload.attachments = vec![AttachmentPayload {
            url: "https://cdn.example/1.gif".to_owned(),
            content_type: Some("image/gif".to_owned()),
            file_name: Some("cat.gif".to_owned()),
            size_bytes: 100,
            width: Some(10),
            height: Some(10),
        }];

        ctx.engine.handle_message(&origin(), &payload, &config, &ctx.services, None).await;

        let records = stored_records(&ctx).await;
        let images = records.first().map(|r| r.images.clone()).unwrap_or_default();
        assert_eq!(images.len(), 1);
        assert_eq!(images.first().and_then(|image| image.ext.clone()), Some("gif".to_owned()));
        let context = ctx.fake.requests().first().expect("chat request expected").clone();
        let turn = context.messages.iter().find(|m| m.role == ChatRole::User).expect("user turn");
        assert!(turn.content.contains("![image without description](image.gif)"));
    }

    #[tokio::test]
    async fn describe_failure_still_answers_with_an_undescribed_placeholder() {
        let ctx = ctx_describer(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("answered anyway".to_owned())],
            Arc::new(FakeDescriber::with_results(vec![None])), // recognition fails
        );
        let config = images_enabled_config("local/vision");
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/1.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        // Best-effort contract: the record shows the image, undescribed,
        // and the guaranteed answer still goes out.
        let records = stored_records(&ctx).await;
        assert_eq!(
            records.first().and_then(|r| r.images.first()).map(|image| image.description.clone()),
            Some(None)
        );
        assert_eq!(begin_texts(&ctx.begins), vec!["answered anyway".to_owned()]);
    }

    #[tokio::test]
    async fn per_message_cap_limits_describe_calls_but_keeps_every_image() {
        let settings = LlmSettings { max_images_per_message: 2, ..LlmSettings::default() };
        let ctx = ctx_describer(
            settings,
            Arc::new(RandRandom),
            vec![Ok("ok".to_owned())],
            Arc::new(FakeDescriber::with_results(vec![Some("first"), Some("second")])),
        );
        let config = images_enabled_config("local/vision");
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/1.png", "/2.png", "/3.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        // Two describe calls (the cap), three record entries (fidelity).
        assert_eq!(ctx.describer.job_count(), 2);
        let records = stored_records(&ctx).await;
        let images = records.first().map(|r| r.images.clone()).unwrap_or_default();
        assert_eq!(images.len(), 3);
        assert_eq!(
            images.first().and_then(|image| image.description.clone()),
            Some("first".to_owned())
        );
        assert_eq!(
            images.get(1).and_then(|image| image.description.clone()),
            Some("second".to_owned())
        );
        assert_eq!(images.get(2).and_then(|image| image.description.clone()), None);
    }

    #[tokio::test]
    async fn enabled_images_without_any_model_store_undescribed() {
        let ctx = ctx_describer(
            LlmSettings::default(), // no image_model configured
            Arc::new(RandRandom),
            vec![Ok("ok".to_owned())],
            Arc::new(FakeDescriber::default()),
        );
        let mut config = assigned_config();
        config.images = true; // ... but no override either
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(
                &origin(),
                &payload_with_images(&["https://cdn.example/1.png"]),
                &config,
                &ctx.services,
                None,
            )
            .await;

        assert_eq!(ctx.describer.job_count(), 0);
        let records = stored_records(&ctx).await;
        let images = records.first().map(|r| r.images.clone()).unwrap_or_default();
        assert_eq!(images.first().and_then(|image| image.description.clone()), None);
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
                sender_id: None,
                guild_name: None,
                content: "hello!".to_owned(),
                reply_to: None,
                captured_at: 0,
                images: Vec::new(),
            },
        )
        .await;

        // Bob replies to the bot's message (id 11): no mention, still triggers.
        ctx.engine
            .handle_message(
                &origin(),
                &payload(false, Some(11)),
                &assigned_config(),
                &ctx.services,
                None,
            )
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
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        // The generated answer is gone, but the triggered message gets the
        // visible generic fallback - and no bot turn is recorded.
        assert!(ctx.begins.lock().is_empty());
        assert_eq!(ctx.output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
        assert_eq!(ctx.output.sent().first().and_then(|m| m.reply_to), Some(MessageId(77)));
        let records = stored_records(&ctx).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records.first().map(|r| r.role), Some(RecordRole::User));
    }

    #[tokio::test]
    async fn oversized_replies_split_across_messages() {
        let ctx = ctx(vec![Ok("aaa\nbbb".to_owned())]);
        let config = ChannelConfig { split_length: Some(5), ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // First chunk via begin (as a native reply to the trigger),
        // remainder via plain sends.
        assert_eq!(begin_texts(&ctx.begins), vec!["aaa".to_owned()]);
        assert_eq!(ctx.begins.lock().first().and_then(|m| m.reply_to), Some(MessageId(77)));
        assert_eq!(ctx.output.messages(), vec!["bbb".to_owned()]);
        // Follow-up split parts are plain - only the first message is a
        // reply; the rest are continuation.
        assert!(ctx.output.sent().first().is_none_or(|m| m.reply_to.is_none()));
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
            "test",
            GuildId(1),
            NAMESPACE,
            &channel_state_key(2),
            serde_json::json!({"summary": "the gist", "cutoff_seq": 0}),
        );

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
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
            "test",
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
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        // The context holds only post-cutoff turns plus the triggering one.
        let request = ctx.fake.requests().first().expect("request expected").clone();
        let turns: Vec<&str> =
            request.messages.iter().map(|message| message.content.as_str()).collect();
        assert!(turns.iter().any(|turn| turn.contains("[new1](<@>): m3")));
        assert!(!turns.iter().any(|turn| turn.contains("m1") || turn.contains("m2")));
        // The cutoff moved nothing: all records are still stored
        // (4 seeded + the captured trigger + the assistant turn).
        assert_eq!(stored_records(&ctx).await.len(), 6);
    }

    /// `/llm_forget` removes every stored record carrying the message id -
    /// the one deletion path - and leaves neighboring records untouched.
    #[tokio::test]
    async fn forget_message_removes_every_record_with_the_message_id() {
        let ctx = ctx(vec![]);
        let storage = ctx.storage.guild_scoped("test", GuildId(1));
        append_record(&ctx.storage, &user_record(11, "alice", "keep me")).await;
        append_record(&ctx.storage, &user_record(42, "alice", "bad take")).await;
        append_record(&ctx.storage, &user_record(42, "alice", "dup capture")).await;
        append_record(&ctx.storage, &user_record(13, "bob", "unrelated")).await;

        let removed =
            ctx.engine.forget_message(&storage, 2, 42).await.expect("forget expected to succeed");
        assert_eq!(removed.len(), 2, "every stored record with the id goes");

        let contents: Vec<String> =
            stored_records_in(&ctx.storage).await.into_iter().map(|r| r.content).collect();
        assert_eq!(contents, ["keep me", "unrelated"]);
    }

    /// An unknown id reports empty - nothing deleted, no error.
    #[tokio::test]
    async fn forget_message_reports_empty_for_an_unknown_id() {
        let ctx = ctx(vec![]);
        let storage = ctx.storage.guild_scoped("test", GuildId(1));
        append_record(&ctx.storage, &user_record(11, "alice", "stay")).await;

        let removed =
            ctx.engine.forget_message(&storage, 2, 999).await.expect("forget expected to succeed");
        assert!(removed.is_empty());
        assert_eq!(stored_records_in(&ctx.storage).await.len(), 1);
    }

    /// The paged scan crosses page boundaries: matches spread far beyond
    /// one page all go, and pagination never rescans or skips records.
    #[tokio::test]
    async fn forget_message_pages_through_long_logs() {
        let ctx = ctx(vec![]);
        let storage = ctx.storage.guild_scoped("test", GuildId(1));
        // 1_200 records - well past the 500-record page - with the target
        // id sprinkled near each page boundary (including the very ends).
        // The keepers' ids (100..) never collide with the target id 7.
        for seq in 0..1200u64 {
            let target = seq == 0 || seq == 499 || seq == 500 || seq == 999 || seq == 1199;
            let record = if target {
                user_record(7, "alice", "goes")
            } else {
                user_record(seq + 100, "alice", "stays")
            };
            append_record(&ctx.storage, &record).await;
        }

        let removed =
            ctx.engine.forget_message(&storage, 2, 7).await.expect("forget expected to succeed");

        assert_eq!(removed.len(), 5, "every match across every page goes");
        // The whole log, past the helper's 100-record window.
        let survivors = storage
            .list_after(&records_namespace(2), 0, u32::MAX)
            .await
            .expect("records readable")
            .into_iter()
            .map(|stored| {
                serde_json::from_value::<ConversationRecord>(stored.payload)
                    .expect("record expected to deserialize")
            })
            .collect::<Vec<_>>();
        assert_eq!(survivors.len(), 1195);
        assert!(survivors.iter().all(|record| record.content == "stays"));
    }

    /// A log that is an EXACT page multiple ends on a full page: the scan
    /// must fetch one further (empty) page and terminate cleanly instead
    /// of looping or stopping early. The timeout guard turns a paging
    /// regression into a test failure, not a hang.
    #[tokio::test]
    async fn forget_message_exits_on_an_exact_page_multiple() {
        let ctx = ctx(vec![]);
        let storage = ctx.storage.guild_scoped("test", GuildId(1));
        // Exactly 500 records (one full page), the target at the very end.
        for seq in 0..499u64 {
            append_record(&ctx.storage, &user_record(seq + 100, "alice", "stays")).await;
        }
        append_record(&ctx.storage, &user_record(7, "alice", "goes")).await;

        let removed = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            ctx.engine.forget_message(&storage, 2, 7),
        )
        .await
        .expect("the scan terminates")
        .expect("forget expected to succeed");

        assert_eq!(removed.len(), 1, "the last-record match goes");
        let survivors = storage
            .list_after(&records_namespace(2), 0, u32::MAX)
            .await
            .expect("records readable")
            .len();
        assert_eq!(survivors, 499);
    }

    /// An empty log terminates with an empty report - the zero-record
    /// branch of the paged scan.
    #[tokio::test]
    async fn forget_message_on_an_empty_log_reports_empty() {
        let ctx = ctx(vec![]);
        let storage = ctx.storage.guild_scoped("test", GuildId(1));

        let removed = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            ctx.engine.forget_message(&storage, 2, 7),
        )
        .await
        .expect("the scan terminates")
        .expect("forget expected to succeed");

        assert!(removed.is_empty());
    }

    #[tokio::test]
    async fn context_messages_clamps_the_window() {
        let ctx = ctx(vec![Ok("ok".to_owned())]);
        let config = ChannelConfig { context_messages: 2, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        // Fixed slots (system + summary) plus the newest 2 turns only.
        assert_eq!(request.messages.len(), 4);
        assert_eq!(request.messages.get(2).map(|m| m.content.as_str()), Some("[a3](<@>): m3"));
        assert_eq!(
            request.messages.get(3).map(|m| m.content.as_str()),
            Some("[alice](<@3>): hello bot")
        );
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
            Arc::new(FakeDescriber::default()) as Arc<dyn ImageDescriber>,
            crate::test_support::test_platform_info(),
            crate::test_support::test_plugin_storage(),
        );
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(crate::test_support::RecordingChatOutputFactory::new(
                Arc::clone(&output),
            )),
            guild_storage: Some(
                crate::test_support::FailingStorage.guild_scoped("test", GuildId(1)),
            ),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };

        engine
            .handle_message(&origin(), &payload(true, None), &assigned_config(), &services, None)
            .await;

        assert!(fake.requests().is_empty());
        // A mention is decidable without the state doc - the guarantee holds.
        assert_eq!(output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
    }

    #[tokio::test]
    async fn compaction_runs_after_reply_when_window_outgrows_depth() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx =
            ctx_with(settings, vec![Ok("summary text".to_owned()), Ok("the answer".to_owned())]);
        let config = ChannelConfig { context_messages: 3, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

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
            .guild_scoped("test", GuildId(1))
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
        assert_eq!(begin_texts(&ctx.begins), vec!["the answer".to_owned()]);
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
        let config = ChannelConfig { context_messages: 3, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // Both completions ran (chat + compaction), but the stored "last
        // request" stays the CHAT call's usage - the summarizer never writes.
        assert_eq!(ctx.fake.requests().len(), 2);
        let stats_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, &channel_stats_key(2))
            .await
            .expect("stats readable")
            .expect("stats expected");
        let stats: UsageStats =
            serde_json::from_value(stats_raw).expect("stats expected to deserialize");
        assert_eq!(stats.last.map(|usage| usage.prompt_tokens), Some(100));
    }

    /// The same two completions DO land in the plugin-global totals: the
    /// chat answer and the summarizer are both real spend, attributed to
    /// their model and the originating guild. Channel stats stay unpolluted
    /// (see `compaction_does_not_pollute_chat_stats`).
    #[tokio::test]
    async fn global_usage_totals_count_chat_and_compaction() {
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
        let config = ChannelConfig { context_messages: 3, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let model = config.model.as_str();
        let (all_time, today) = ctx.engine.usage().guild_snapshot(1).await;
        assert_eq!(all_time.total.requests, 2, "chat + compaction");
        assert_eq!(all_time.total.prompt_tokens, 100 + 50_000);
        assert_eq!(all_time.total.completion_tokens, 30);
        // Both completions share the channel's model (no compaction_model
        // override): one per-model breakdown, same totals.
        let model_totals =
            all_time.models.iter().map(|(name, dim)| (name.as_str(), *dim)).collect::<Vec<_>>();
        assert_eq!(model_totals.len(), 1);
        assert_eq!(model_totals.first().expect("model expected").0, model);
        assert_eq!(today.requests, 2);
        // Other guilds stay untouched.
        let (other, _) = ctx.engine.usage().guild_snapshot(2).await;
        assert_eq!(other.total.requests, 0);
    }

    /// A completion without an endpoint usage report still counts: the
    /// channel's calibrated tokens-per-character ratio estimates both sides,
    /// so the totals keep tracking real spend (approximate by contract).
    #[tokio::test]
    async fn usageless_completion_falls_back_to_the_estimate() {
        let ctx = ctx(vec![Ok("an answer of some length".to_owned())]);
        ctx.fake.set_usage(None);
        let config = assigned_config();
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let (all_time, today) = ctx.engine.usage().guild_snapshot(1).await;
        assert_eq!(all_time.total.requests, 1);
        assert!(all_time.total.prompt_tokens > 0, "context estimated");
        assert!(all_time.total.completion_tokens > 0, "answer estimated");
        assert_eq!(today.requests, 1);
    }

    #[tokio::test]
    async fn compaction_failure_keeps_state_and_reports_to_service_channel() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx = ctx_with(
            settings,
            vec![Err(LlmError::Request("summarizer down".to_owned())), Ok("the answer".to_owned())],
        );
        let config = ChannelConfig { context_messages: 3, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        seed_service_channel(&ctx);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // The reply went out; the compaction failure left the state intact.
        assert_eq!(begin_texts(&ctx.begins), vec!["the answer".to_owned()]);
        let state_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable");
        assert_eq!(state_raw, None);
        assert!(ctx.output.messages().iter().any(|message| message.contains("compaction failed")));
    }

    /// The exact boundary is stable: live records at exactly
    /// `context_messages` compact nothing (`maybe_compact` early-returns on
    /// `<=`) - no summarizer call, and the cutoff never moves.
    #[tokio::test]
    async fn compaction_is_stable_at_the_exact_depth_boundary() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx = ctx_with(settings, vec![Ok("the answer".to_owned())]);
        let config = ChannelConfig { context_messages: 4, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // 3 seeded records + this capture = 4 live records == depth: the
        // reply went out, only the chat completion ran, and no state
        // document (and with it no cutoff) was ever written.
        assert_eq!(begin_texts(&ctx.begins), vec!["the answer".to_owned()]);
        assert_eq!(ctx.fake.requests().len(), 1);
        let state_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable");
        assert_eq!(state_raw, None);
    }

    /// A `keep_tail` that would keep the whole live window makes no
    /// progress: the skip branch fires before any summarizer call, the
    /// state stays put, and - unlike a compaction FAILURE - no service
    /// notice is due.
    #[tokio::test]
    async fn compaction_skips_when_keep_tail_makes_no_progress() {
        let settings = LlmSettings { compaction_keep_tail: 10, ..LlmSettings::default() };
        let ctx = ctx_with(settings, vec![Ok("the answer".to_owned())]);
        let config = ChannelConfig { context_messages: 2, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        seed_service_channel(&ctx);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;
        append_record(&ctx.storage, &user_record(2, "a2", "m2")).await;
        append_record(&ctx.storage, &user_record(3, "a3", "m3")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // 4 live records outgrow depth 2, but keep_tail 10 >= 4: only the
        // chat completion ran, nothing was committed, nothing reported.
        assert_eq!(ctx.fake.requests().len(), 1);
        let state_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable");
        assert_eq!(state_raw, None);
        assert!(
            !ctx.output.messages().iter().any(|message| message.contains("compaction")),
            "a skip is not a failure - no notice: {:?}",
            ctx.output.messages()
        );
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
                .guild_scoped("test", GuildId(1)),
        );
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
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
                .guild_scoped("test", GuildId(1)),
        );
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
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
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
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
                .handle_message(
                    &origin(),
                    &payload(true, None),
                    &assigned_config(),
                    &ctx.services,
                    None,
                )
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
                    None,
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

    /// Without a configured service channel an error notice has nowhere to
    /// go: `notify_service` returns right after the lookup, so the only
    /// guild-visible output for a failed mention is the guaranteed fallback
    /// - no embed, no classification text, and no stream ever began.
    #[tokio::test]
    async fn error_notices_stay_silent_without_a_service_channel() {
        let ctx = ctx(vec![Err(LlmError::Request("endpoint down".to_owned()))]);
        seed_config(&ctx.storage, &assigned_config());
        // Deliberately NO seed_service_channel.

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        assert_eq!(ctx.output.messages(), vec![FALLBACK_MESSAGE.to_owned()]);
        assert!(begin_texts(&ctx.begins).is_empty());
    }

    #[tokio::test]
    async fn compaction_disabled_lets_the_window_grow() {
        let settings = LlmSettings { compaction_keep_tail: 2, ..LlmSettings::default() };
        let ctx = ctx_with(settings, vec![Ok("the answer".to_owned())]);
        let config =
            ChannelConfig { context_messages: 1, compaction_enabled: false, ..assigned_config() };
        seed_config(&ctx.storage, &config);
        append_record(&ctx.storage, &user_record(1, "a1", "m1")).await;

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // Only the chat completion ran; no state was ever written.
        assert_eq!(ctx.fake.requests().len(), 1);
        let state_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
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
            capture: CaptureMode::AllMessages,
            random_reply_chance_percent: 2.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
            .await;

        assert_eq!(ctx.fake.requests().len(), 1);
        assert_eq!(begin_texts(&ctx.begins), vec!["random thought".to_owned()]);
        // The chime answer natively replies to the message that triggered it.
        assert_eq!(ctx.begins.lock().first().and_then(|m| m.reply_to), Some(MessageId(77)));
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
            capture: CaptureMode::AllMessages,
            random_reply_chance_percent: 2.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
            .await;

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
            capture: CaptureMode::AllMessages,
            random_reply_chance_percent: 2.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
            .await;

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
            .handle_message(
                &origin(),
                &payload(false, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
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
            capture: CaptureMode::AllMessages,
            random_reply_chance_percent: 2.0,
            // Explicit long cooldown: back-to-back messages can never both
            // roll, independent of how fast the test machine runs.
            random_cooldown_secs: 300,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        for _ in 0..2 {
            ctx.engine
                .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
                .await;
        }

        // First message chimes; the second lands inside the cooldown window.
        assert_eq!(ctx.fake.requests().len(), 1);
        // Two captures + one assistant turn.
        assert_eq!(stored_records(&ctx).await.len(), 3);
    }

    /// `random_cooldown = 0` disables the gate entirely: consecutive
    /// eligible messages may each roll (and with a always-true deck, each
    /// chime).
    #[tokio::test]
    async fn zero_cooldown_lets_consecutive_messages_chime() {
        let ctx = ctx_random(
            LlmSettings::default(),
            Arc::new(FixedRandom(true)),
            // Popped back-to-front: the first answer handed out is "chime 1".
            vec![Ok("chime 2".to_owned()), Ok("chime 1".to_owned())],
        );
        let config = ChannelConfig {
            capture: CaptureMode::AllMessages,
            random_reply_chance_percent: 2.0,
            random_cooldown_secs: 0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        for _ in 0..2 {
            ctx.engine
                .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
                .await;
        }

        assert_eq!(ctx.fake.requests().len(), 2);
        assert_eq!(begin_texts(&ctx.begins), vec!["chime 1".to_owned(), "chime 2".to_owned()]);
    }

    /// Cooldown expiry: a chime recorded longer ago than the cooldown no
    /// longer blocks - the "still cooling" side is covered by the cooldown
    /// tests. Seeded directly (no sleeps, no virtual clock).
    #[tokio::test]
    async fn chime_cooldown_expires() {
        let ctx = ctx(vec![]);
        let key: ChannelKey = ("discord".to_owned(), 1, 2);
        ctx.engine.chimes.lock().insert(
            key,
            Instant::now()
                .checked_sub(Duration::from_secs(2))
                .expect("cooldown offset expected to fit"),
        );

        assert!(ctx.engine.chime_allowed(&origin(), 1));
    }

    /// The notice rate limit RESETS after the window: a notice older than
    /// the cooldown un-blocks the next one and re-arms the timer.
    #[tokio::test]
    async fn error_notice_rate_limit_resets_after_the_window() {
        let ctx = ctx(vec![]);
        let key: ChannelKey = ("discord".to_owned(), 1, 9);
        let expired = Instant::now()
            .checked_sub(NOTICE_COOLDOWN)
            .and_then(|instant| instant.checked_sub(Duration::from_secs(5)))
            .expect("cooldown offset expected to fit");
        ctx.engine.notices.lock().insert(key, expired);

        assert!(ctx.engine.error_notice_allowed(&origin(), 9));
        // Re-armed: an immediate second notice is blocked again.
        assert!(!ctx.engine.error_notice_allowed(&origin(), 9));
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

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        assert_eq!(begin_texts(&ctx.begins), vec!["Answer".to_owned()]);
        // The live message opened as a native reply to the trigger.
        assert_eq!(ctx.begins.lock().first().and_then(|m| m.reply_to), Some(MessageId(77)));
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

    /// A stream longer than one message never edits past the cap: every
    /// posted reveal is a clamped prefix, and the final edit re-pins the
    /// authoritative first chunk while the overflow rides plain parts.
    #[tokio::test]
    async fn streaming_over_cap_keeps_every_edit_within_the_limit() {
        let settings = LlmSettings { stream_interval_ms: 1, ..LlmSettings::default() };
        let ctx = ctx_delta(
            settings,
            Arc::new(DeltaCompletion {
                chunks: vec!["0123456789A".to_owned(), "BCDE".to_owned()],
                delay_ms: 5,
                fail_at_start: false,
                fail_after: None,
            }),
        );
        let config = ChannelConfig { streaming: true, split_length: Some(10), ..assigned_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        for begin in ctx.begins.lock().iter() {
            assert!(conversation::utf16_len(&begin.content) <= 10);
        }
        let updates = ctx.updates.lock().clone();
        assert!(!updates.is_empty());
        for edit in &updates {
            assert!(conversation::utf16_len(edit) <= 10, "edit over the cap: {edit:?}");
        }
        // The authoritative final edit pins the exact first chunk.
        assert_eq!(updates.last().map(String::as_str), Some("0123456789"));
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

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

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

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // No fallback: partial text is already on screen.
        assert!(ctx.output.messages().is_empty());
        assert_eq!(begin_texts(&ctx.begins), vec!["partial answer".to_owned()]);
        assert_eq!(ctx.begins.lock().first().and_then(|m| m.reply_to), Some(MessageId(77)));
        let updates = ctx.updates.lock().clone();
        assert_eq!(updates.last().map(String::as_str), Some("partial answer"));
        let records = stored_records_in(&ctx.storage).await;
        let last = records.last().expect("bot turn recorded");
        assert_eq!(last.role, RecordRole::Assistant);
        assert_eq!(last.content, "partial answer");
    }

    // ---- Reaction tool (see `tools` for the protocol rules R1-R6) ----

    fn react_config() -> ChannelConfig {
        ChannelConfig { react: true, ..assigned_config() }
    }

    fn emoji_menu_emojis() -> Vec<ReactableEmoji> {
        vec![
            ReactableEmoji { name: "dorkiS".to_owned(), token: "<:dorkiS:9>".to_owned() },
            ReactableEmoji { name: "ashuu".to_owned(), token: "<a:ashuu:7>".to_owned() },
        ]
    }

    /// `all` inject mode lists every server emoji as exact wire forms inside
    /// the react tool block of the system prompt.
    #[tokio::test]
    async fn react_emoji_menu_injects_server_emojis_into_the_prompt() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        let config = ChannelConfig {
            react: true,
            react_emoji_inject: EmojiInject::All,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(
            system
                .content
                .contains("Custom emojis of this server (exact forms): <:dorkiS:9> <a:ashuu:7>"),
            "unexpected: {}",
            system.content
        );
    }

    /// `whitelist` inject mode keeps only whitelisted names - the guild-wide
    /// list when the channel has none of its own; an empty effective list
    /// injects no menu at all. Off channels never see a menu either way.
    #[tokio::test]
    async fn react_emoji_menu_whitelist_filters_by_scope_fallback() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        let config = ChannelConfig {
            react: true,
            react_emoji_inject: EmojiInject::Whitelist,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            GUILD_EMOJI_WHITELIST_KEY,
            serde_json::json!(["ashuu"]),
        );

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(system.content.contains("<a:ashuu:7>"), "unexpected: {}", system.content);
        assert!(!system.content.contains("dorkiS"), "unexpected: {}", system.content);

        // A non-empty channel list replaces the guild baseline.
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            &channel_emoji_whitelist_key(2),
            serde_json::json!(["dorkiS"]),
        );
        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;
        let request = ctx.fake.requests().last().expect("second request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(system.content.contains("<:dorkiS:9>"), "unexpected: {}", system.content);
        assert!(!system.content.contains("ashuu"), "unexpected: {}", system.content);
    }

    /// Described whitelist entries render as sorted sub-lines under the
    /// header; undescribed entries stay bare tokens.
    #[tokio::test]
    async fn react_emoji_menu_renders_descriptions_as_sub_lines() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        let config = ChannelConfig {
            react: true,
            react_emoji_inject: EmojiInject::Whitelist,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            GUILD_EMOJI_WHITELIST_KEY,
            serde_json::json!([
                { "name": "ashuu" },
                { "name": "dorkiS", "description": "smug face, for mockery" }
            ]),
        );

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(
            system.content.contains(
                "Custom emojis of this server (exact forms):\n  <a:ashuu:7>\
                 \n  <:dorkiS:9> \u{2014} smug face, for mockery"
            ),
            "byte-exact sub-lines in name order: {}",
            system.content
        );
    }

    /// Descriptions ride the channel override: a non-empty channel list
    /// replaces the guild baseline, descriptions included - the guild's
    /// described entry must not leak.
    #[tokio::test]
    async fn react_emoji_menu_channel_override_carries_descriptions() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        let config = ChannelConfig {
            react: true,
            react_emoji_inject: EmojiInject::Whitelist,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            GUILD_EMOJI_WHITELIST_KEY,
            serde_json::json!([{ "name": "ashuu", "description": "guild hint" }]),
        );
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            &channel_emoji_whitelist_key(2),
            serde_json::json!([{ "name": "dorkiS", "description": "channel hint" }]),
        );

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(
            system.content.contains("  <:dorkiS:9> \u{2014} channel hint"),
            "channel override applied: {}",
            system.content
        );
        assert!(!system.content.contains("ashuu"), "guild baseline replaced: {}", system.content);
        assert!(
            !system.content.contains("guild hint"),
            "guild hint not merged: {}",
            system.content
        );
    }

    /// A whitelist without any description keeps the historical single
    /// line - the byte-stable shape provider caches were keyed on.
    #[tokio::test]
    async fn react_emoji_menu_without_descriptions_stays_one_line() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        let config = ChannelConfig {
            react: true,
            react_emoji_inject: EmojiInject::Whitelist,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            GUILD_EMOJI_WHITELIST_KEY,
            serde_json::json!([{ "name": "ashuu" }]),
        );

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(
            system.content.contains("Custom emojis of this server (exact forms): <a:ashuu:7>"),
            "single line kept: {}",
            system.content
        );
        assert!(!system.content.contains("exact forms):\n"), "no sub-lines: {}", system.content);
    }

    /// `all` mode serves the raw platform listing - whitelist descriptions
    /// never leak into it.
    #[tokio::test]
    async fn react_emoji_menu_all_mode_ignores_descriptions() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        let config = ChannelConfig {
            react: true,
            react_emoji_inject: EmojiInject::All,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            GUILD_EMOJI_WHITELIST_KEY,
            serde_json::json!([{ "name": "dorkiS", "description": "smug face" }]),
        );

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(
            system.content.contains("Custom emojis of this server (exact forms): <:dorkiS:9>"),
            "unexpected: {}",
            system.content
        );
        assert!(!system.content.contains("smug face"), "no descriptions in all mode");
    }

    /// React-on with the inject off keeps the prompt free of the menu - and
    /// so does inject-on with react off (the menu rides the react block).
    #[tokio::test]
    async fn react_emoji_menu_stays_off_without_opt_in() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        let request = ctx.fake.requests().first().expect("one request expected").clone();
        let system = request.messages.first().expect("system slot expected");
        assert!(!system.content.contains("Custom emojis of this server"));
        assert!(!system.content.contains("dorkiS"));
    }

    /// The default `whitelist` mode is a no-op while the effective
    /// whitelist is empty: no menu at all, byte-identical to `none` - the
    /// whitelist itself is the opt-in.
    #[tokio::test]
    async fn react_emoji_menu_default_whitelist_with_empty_list_is_a_no_op() {
        let ctx = ctx_describer_with_emojis(
            LlmSettings::default(),
            Arc::new(RandRandom),
            vec![Ok("hi".to_owned())],
            Arc::new(FakeDescriber::default()),
            emoji_menu_emojis(),
        );
        let config = ChannelConfig {
            react: true,
            react_emoji_inject: EmojiInject::default(),
            ..assigned_config()
        };
        assert_eq!(config.react_emoji_inject, EmojiInject::Whitelist);

        let menu = ctx.engine.react_emoji_menu(&origin(), &config, &ctx.services).await;
        assert!(menu.is_empty(), "empty whitelist injects nothing: {menu}");

        let none = ChannelConfig { react_emoji_inject: EmojiInject::None, ..config };
        let none_menu = ctx.engine.react_emoji_menu(&origin(), &none, &ctx.services).await;
        assert_eq!(menu, none_menu, "default mode must match `none` byte-for-byte");
    }

    /// Markers are stripped from the delivered text and the recorded bot
    /// turn; the tokens fire against the triggering message (target of the
    /// reply), in order, deduplicated, invalid ones dropped individually.
    #[tokio::test]
    async fn react_markers_are_stripped_and_reactions_fire() {
        let ctx = ctx(vec![Ok("nice! [[react: 🤓 :dorkiS: word 🤓]]".to_owned())]);
        let config = react_config();
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        // The channel sees only the cleaned answer.
        assert_eq!(begin_texts(&ctx.begins), vec!["nice!".to_owned()]);
        // Valid tokens fired against the reply target; the prose word and
        // the duplicate were dropped.
        assert_eq!(
            reactions_log(&ctx.factory),
            vec![(77, "🤓".to_owned()), (77, ":dorkiS:".to_owned())]
        );
        // The recorded bot turn carries the cleaned text (R4).
        let records = stored_records(&ctx).await;
        let last = records.last().expect("bot turn recorded");
        assert_eq!(last.content, "nice!");
    }

    /// A triggered answer that is ONLY a marker still honors the
    /// guaranteed-answer contract: the reactions are the visible response -
    /// no fallback, and no phantom bot turn in the history.
    #[tokio::test]
    async fn marker_only_answer_reacts_without_fallback_or_record() {
        let ctx = ctx(vec![Ok("[[react: 🤓]]".to_owned())]);
        let config = react_config();
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        assert!(ctx.begins.lock().is_empty());
        assert!(ctx.output.messages().is_empty(), "no fallback: the emoji was the answer");
        assert_eq!(reactions_log(&ctx.factory), vec![(77, "🤓".to_owned())]);
        let records = stored_records(&ctx).await;
        assert!(records.iter().all(|record| record.role == RecordRole::User));
    }

    /// R5: reaction failures are cosmetic - the delivered answer and its
    /// record are unaffected (a foreign-guild custom emoji skips, the reply
    /// stays).
    #[tokio::test]
    async fn reaction_failure_never_touches_the_answer() {
        let ctx = ctx(vec![Ok("still delivered [[react: :foreignGuild:]]".to_owned())]);
        let config = react_config();
        seed_config(&ctx.storage, &config);
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(FailingReactionFactory { output: Arc::clone(&output) })
                as Arc<dyn ChatOutputFactoryPort>,
            guild_storage: Some(ctx.storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };

        ctx.engine.handle_message(&origin(), &payload(true, None), &config, &services, None).await;

        // Delivered = recorded: the assistant turn with the cleaned text
        // proves the answer went out despite the reaction failure.
        let records = stored_records(&ctx).await;
        let last = records.last().expect("bot turn recorded");
        assert_eq!(last.role, RecordRole::Assistant);
        assert_eq!(last.content, "still delivered");
    }

    /// A react-off channel strips hallucinated markers (never shows them)
    /// but fires nothing.
    #[tokio::test]
    async fn react_disabled_strips_markers_but_never_fires() {
        let ctx = ctx(vec![Ok("text [[react: 🤓]]".to_owned())]);
        let config = assigned_config();
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        assert_eq!(begin_texts(&ctx.begins), vec!["text".to_owned()]);
        assert!(reactions_log(&ctx.factory).is_empty());
    }

    /// A stream that died mid-answer skips reactions entirely (Q4): the
    /// partial text is delivered, no reaction fires even though the full
    /// content carried a marker.
    #[tokio::test]
    async fn partial_stream_never_fires_reactions() {
        let ctx = ctx_delta(
            LlmSettings::default(),
            Arc::new(DeltaCompletion {
                chunks: vec!["partial".to_owned(), " [[react: 🤓]]".to_owned()],
                delay_ms: 1,
                fail_at_start: false,
                fail_after: Some(1),
            }),
        );
        let config = ChannelConfig { streaming: true, ..react_config() };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(true, None), &config, &ctx.services, None)
            .await;

        assert!(ctx.output.messages().is_empty());
        assert_eq!(ctx.updates.lock().last().map(String::as_str), Some("partial"));
        assert!(ctx.reactions.lock().is_empty());
    }

    /// The silent-react chime: one single-shot call, prose discarded, no bot
    /// turn recorded ("delivered = recorded"), reactions still fire.
    #[tokio::test]
    async fn react_chime_reacts_silently_without_a_bot_turn() {
        let ctx = ctx_random(
            LlmSettings::default(),
            Arc::new(FixedRandom(true)),
            vec![Ok("prose nobody sees [[react: 🐻]]".to_owned())],
        );
        let config = ChannelConfig {
            capture: CaptureMode::AllMessages,
            react: true,
            random_reply_chance_percent: 0.0, // reply roll off - react-only chime
            random_react_chance_percent: 100.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
            .await;

        assert_eq!(ctx.fake.requests().len(), 1);
        assert!(ctx.begins.lock().is_empty(), "no reply is delivered");
        assert!(ctx.output.messages().is_empty());
        assert_eq!(reactions_log(&ctx.factory), vec![(77, "🐻".to_owned())]);
        // Only the capture - no phantom assistant turn.
        assert_eq!(stored_records(&ctx).await.len(), 1);
        // The call was real: usage stats recorded despite the silence.
        let stats = ctx
            .storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, &channel_stats_key(2))
            .await
            .expect("stats readable");
        assert!(stats.is_some(), "react chime usage must be recorded");
    }

    /// The silent-react call carries a dedicated reaction-only instruction
    /// (appended after the context): the model must know this invocation is
    /// a reaction decision, or it answers conversationally and its prose is
    /// thrown away unused.
    #[tokio::test]
    async fn react_chime_tells_the_model_to_react_not_reply() {
        let ctx = ctx_random(
            LlmSettings::default(),
            Arc::new(FixedRandom(true)),
            vec![Ok("[[react: 🐻]]".to_owned())],
        );
        let config = ChannelConfig {
            capture: CaptureMode::AllMessages,
            react: true,
            random_reply_chance_percent: 0.0,
            random_react_chance_percent: 100.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
            .await;

        let requests = ctx.fake.requests();
        assert_eq!(requests.len(), 1);
        let messages = &requests.first().expect("request expected").messages;
        let last = messages.last().expect("hint message expected");
        assert!(
            last.content.contains("reaction to the newest message"),
            "react-only hint expected as the final message: {:?}",
            messages
        );
        // A normal reply call must NOT carry the hint - it rides only on
        // the silent-react invocation.
        let hint_count =
            messages.iter().filter(|m| m.content.contains("reaction to the newest")).count();
        assert_eq!(hint_count, 1);
    }

    /// Without a marker the silent-react chime stays invisible: no reply, no
    /// reaction, no record - the roll trace at debug is the only trace.
    #[tokio::test]
    async fn react_chime_without_marker_stays_silent() {
        let ctx = ctx_random(
            LlmSettings::default(),
            Arc::new(FixedRandom(true)),
            vec![Ok("just prose, no marker".to_owned())],
        );
        let config = ChannelConfig {
            capture: CaptureMode::AllMessages,
            react: true,
            random_reply_chance_percent: 0.0,
            random_react_chance_percent: 100.0,
            ..assigned_config()
        };
        seed_config(&ctx.storage, &config);

        ctx.engine
            .handle_message(&origin(), &payload(false, None), &config, &ctx.services, None)
            .await;

        assert_eq!(ctx.fake.requests().len(), 1);
        assert!(ctx.begins.lock().is_empty());
        assert!(ctx.output.messages().is_empty());
        assert!(reactions_log(&ctx.factory).is_empty());
        assert_eq!(stored_records(&ctx).await.len(), 1);
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
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        let stats_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
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

    /// A usage-less endpoint still leaves the response time (observability
    /// only). On a fresh channel (no prior usage) the usage-dependent parts
    /// of the stats doc stay empty, so the estimate stays uncalibrated.
    #[tokio::test]
    async fn stats_keep_only_the_response_time_when_usage_is_missing() {
        let ctx = ctx(vec![Ok("ok".to_owned())]); // no usage reported
        seed_config(&ctx.storage, &assigned_config());

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        let stats_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, &channel_stats_key(2))
            .await
            .expect("stats readable")
            .expect("timing-only stats expected to be recorded");
        let stats: UsageStats =
            serde_json::from_value(stats_raw).expect("stats expected to deserialize");
        assert_eq!(stats.last, None);
        assert_eq!(stats.last_timing, Some(ResponseTiming::measured(0)));
        // The estimate stays uncalibrated without usage.
        assert!((stats.tokens_per_char - 0.25).abs() < f64::EPSILON);
        assert_eq!(stats.last_budget, None);
    }

    /// A usage-less completion must not wipe the channel's calibration: the
    /// previously reported usage stays stored - token-budget context filling
    /// keeps working - until a newer report replaces it.
    #[tokio::test]
    async fn usage_less_completion_preserves_the_previous_usage() {
        let ctx = ctx(vec![Ok("first".to_owned()), Ok("second".to_owned())]);
        ctx.fake.set_usage_per_call(vec![
            Some(TokenUsage {
                prompt_tokens: 1000,
                completion_tokens: 10,
                total_tokens: 1010,
                cached_tokens: None,
                reasoning_tokens: None,
            }),
            None,
        ]);
        seed_config(&ctx.storage, &assigned_config());

        for _ in 0..2 {
            ctx.engine
                .handle_message(
                    &origin(),
                    &payload(true, None),
                    &assigned_config(),
                    &ctx.services,
                    None,
                )
                .await;
        }

        let stats_raw = ctx
            .storage
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, &channel_stats_key(2))
            .await
            .expect("stats readable")
            .expect("stats expected after two completions");
        let stats: UsageStats =
            serde_json::from_value(stats_raw).expect("stats expected to deserialize");
        assert_eq!(
            stats.last.map(|usage| usage.prompt_tokens),
            Some(1000),
            "the usage-less second completion must keep the first report"
        );
        assert!(stats.last_timing.is_some());
    }

    /// A malformed state document resets the channel to a fresh window (the
    /// summary text is lost) but salvages the cutoff, so compacted history
    /// does not resurrect into the context - and the message is still
    /// answered.
    #[tokio::test]
    async fn malformed_state_salvages_the_cutoff_and_still_answers() {
        let ctx = ctx(vec![Ok("recovered".to_owned())]);
        seed_config(&ctx.storage, &assigned_config());

        // A compacted past: one old user record folded away by a cutoff at
        // seq 1 - and a state doc whose summary field is the wrong type.
        let scoped = ctx.storage.guild_scoped("test", GuildId(1));
        let old_record = ConversationRecord {
            message_id: None,
            role: RecordRole::User,
            author: Some("bob".to_owned()),
            sender_id: Some(9),
            guild_name: None,
            content: "compacted away".to_owned(),
            reply_to: None,
            captured_at: 1,
            images: Vec::new(),
        };
        scoped
            .append(
                &records_namespace(2),
                serde_json::to_value(&old_record).expect("record serializes"),
            )
            .await
            .expect("append expected to succeed");
        ctx.storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            &channel_state_key(2),
            serde_json::json!({ "summary": 42, "cutoff_seq": 1 }),
        );

        ctx.engine
            .handle_message(
                &origin(),
                &payload(true, None),
                &assigned_config(),
                &ctx.services,
                None,
            )
            .await;

        // The answer went out (the channel is not bricked) - answers deliver
        // through the factory's stream begin, not the plain output port.
        let delivered = begin_texts(&ctx.begins);
        assert!(
            delivered.iter().any(|m| m.contains("recovered")),
            "answer expected, got: {delivered:?}"
        );
        // ... and the salvaged cutoff kept the compacted record out of the
        // prompt: with a wiped cutoff it would have re-entered the window.
        let requests = ctx.fake.requests();
        let request = requests.last().expect("a completion was requested");
        let prompt: String =
            request.messages.iter().map(|message| message.content.as_str()).collect();
        assert!(
            !prompt.contains("compacted away"),
            "salvaged cutoff must keep compacted history out of the context: {prompt}"
        );
    }
}
