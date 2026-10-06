//! Domain model and storage schema of the LLM chat plugin. Configuration is
//! per-channel (the channel is the scope users think in); conversation state
//! is per-channel too; the service channel is per-guild.

use serde::{Deserialize, Serialize};

use crate::kernel::models::GuildId;
use crate::plugins::llm::completion_port::{ResponseTiming, TokenUsage};

/// Guild storage namespace owned by this plugin (the plugin's slug).
pub const NAMESPACE: &str = "llm";

/// Storage key of the per-guild service channel (snowflake string): where
/// LLM errors and service notices are reported. Absent = tracing only.
pub const SERVICE_CHANNEL_KEY: &str = "service_channel";

/// Document key of a channel's chat configuration: `channel:{id}`.
#[must_use]
pub fn channel_config_key(channel_id: u64) -> String {
    format!("channel:{channel_id}")
}

/// Document key of a channel's conversation state: `channel:{id}:state`.
/// Kept separate from the config so admin tuning and conversation progress
/// never overwrite each other; the state itself is a single document so a
/// compaction commit is atomic (summary + cutoff advance together).
#[must_use]
pub fn channel_state_key(channel_id: u64) -> String {
    format!("channel:{channel_id}:state")
}

/// Document key of a channel's cutoff-undo stash: `channel:{id}:state_undo`.
/// Holds the state document exactly as it was before the last
/// `/llm_cutoff` (JSON `null` = the channel had no state document, i.e.
/// the full history was live). Consumed and cleared by `/llm_cutoff_undo`;
/// overwritten by every new cutoff - one undo level.
#[must_use]
pub fn channel_state_undo_key(channel_id: u64) -> String {
    format!("channel:{channel_id}:state_undo")
}

/// Document key of a channel's completion statistics:
/// `channel:{id}:stats` - last reported token usage + the calibration
/// estimate derived from it. Observability only: unreadable stats never
/// affect replies.
#[must_use]
pub fn channel_stats_key(channel_id: u64) -> String {
    format!("channel:{channel_id}:stats")
}

/// Builds the [`ChannelKey`] for one origin under the deployment's slug.
#[must_use]
pub(super) fn channel_key(
    slug: &'static str,
    guild_id: Option<GuildId>,
    channel_id: u64,
) -> ChannelKey {
    (slug.to_owned(), guild_id.map_or(0, GuildId::get), channel_id)
}

/// Identity of one chat channel across the plugin's state maps (locks,
/// permits, cooldown trackers, error notices): platform slug, guild id
/// (0 for DMs), channel id.
pub(super) type ChannelKey = (String, u64, u64);

/// Record namespace of one channel's conversation log: the plugin slug,
/// sub-partitioned per channel. Guild isolation comes from the storage
/// handle; this adds channel isolation, so one channel's context can never
/// pick up another channel's messages (the doc namespace stays unpartitioned
/// - config/state/service-channel keys are channel-addressed by key).
#[must_use]
pub fn records_namespace(channel_id: u64) -> String {
    format!("{NAMESPACE}:c:{channel_id}")
}

/// Which inbound messages enter a channel's conversation history. The bot's
/// own turns are always recorded (at send time, not off the gateway).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMode {
    /// Only messages related to the bot: mentions, replies into the captured
    /// conversation, and the bot's own turns. The rest of the channel stays
    /// untracked - the privacy-friendly default for multi-purpose channels.
    #[default]
    BotRelated,
    /// Every non-bot message in the channel enters the history.
    AllMessages,
}

/// Sampling/behavior parameters sent with completion requests. Absent fields
/// are simply not sent to the provider, so strict endpoints reject nothing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GenParams {
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub top_k: Option<f64>,
    pub min_p: Option<f64>,
    pub frequency_penalty: Option<f64>,
    pub presence_penalty: Option<f64>,
    pub max_tokens: Option<u32>,
    /// Reasoning/thinking effort hint. `Some(value)` with anything but
    /// `off` is sent as-is (effort style) or enables thinking (switch
    /// style); `Some("off")` requests an explicit disable where the wire
    /// supports one; `None` sends no reasoning parameter at all - the
    /// provider default applies (Z.ai GLM defaults to `max` effort). Sent
    /// only when the requested model declares reasoning support.
    pub reasoning_effort: Option<String>,
}

/// Per-channel chat configuration, stored as a JSON document in the plugin's
/// namespace. Channel scoping comes from the document key; the document only
/// describes behavior.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelConfig {
    /// Provider/model reference (`provider/model`), validated against the
    /// operator-declared model registry at request time - guild admins pick
    /// among operator-configured models, never configure endpoints.
    pub model: String,
    #[serde(default)]
    pub params: GenParams,
    /// Channel system prompt; `None` = plugin-wide default.
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default = "default_true")]
    pub compaction_enabled: bool,
    /// Compaction model override; `None` = plugin-wide compaction model.
    #[serde(default)]
    pub compaction_model: Option<String>,
    /// Compaction prompt override; `None` = plugin-wide compaction prompt.
    #[serde(default)]
    pub compaction_prompt: Option<String>,
    /// Live-window size in messages. Reaching it triggers compaction - the
    /// window is never slid, so the prompt prefix stays byte-stable between
    /// compactions (provider prompt caches stay warm).
    #[serde(default = "default_history_depth")]
    pub history_depth: u32,
    #[serde(default)]
    pub capture_mode: CaptureMode,
    /// Progressive rendering: create the answer, edit it in place as text
    /// arrives.
    #[serde(default)]
    pub streaming: bool,
    /// Chance the bot chimes in on an unrelated user message, percent
    /// (`0` = off).
    #[serde(default = "default_random_chance")]
    pub random_chance_percent: f64,
    /// Minimum seconds between random chime-ins in this channel; `0` =
    /// every eligible message may roll. The deck already balances hits
    /// out per cycle - this prevents two chime-ins on consecutive
    /// messages.
    #[serde(default = "default_random_cooldown")]
    pub random_cooldown_secs: u64,
    /// Reply splitting limit override; `None` = plugin-wide default.
    #[serde(default)]
    pub max_length: Option<usize>,
    /// User-turn rendering in the context; `{sender}`, `{user_id}`,
    /// `{guild_name}`, `{time}` (unix seconds) and `{message}` are
    /// substituted. `None` = `[{sender}](<@{user_id}>): {message}`.
    #[serde(default)]
    pub turn_template: Option<String>,
    /// Estimated token budget for the assembled context. When set, turns
    /// fill newest-first by estimated tokens (calibrated from the
    /// endpoint's own usage reports) and `history_depth` remains the
    /// secondary cap; `None` = count-only filling. Completions always keep
    /// room for the reply on top - this budgets the prompt side only.
    #[serde(default)]
    pub context_budget_tokens: Option<u32>,
    /// Image recognition for this channel's captured messages:
    /// attachments are described by the image model at capture time and
    /// the descriptions are baked into the records. Requires the operator
    /// to have configured `[llm] image_model`. Off by default - image-
    /// flooded channels are the reason this is a per-channel choice.
    #[serde(default)]
    pub images: bool,
    /// Recognition model override; `None` = plugin-wide `image_model`.
    #[serde(default)]
    pub image_model: Option<String>,
    /// Recognition prompt override; `None` = plugin-wide `image_prompt`,
    /// then the built-in default. Descriptions land in the context as
    /// markdown alt-text, so a channel can pick the language/style its
    /// conversations need.
    #[serde(default)]
    pub image_prompt: Option<String>,
    /// Emoji-reaction tool for this channel: the model may decorate the
    /// message it replies to by emitting a `[[react: ...]]` marker, which is
    /// stripped before delivery (see `tools`). Off by default - the marker
    /// instruction joins the system prompt only where the feature is on.
    #[serde(default)]
    pub react: bool,
    /// Chance the bot silently reacts (no reply) to an unrelated captured
    /// message, percent (`0` = off). Independent of `random_chance_percent`
    /// and its own cooldown - the two rolls coexist in parallel.
    #[serde(default = "default_random_react_chance")]
    pub random_react_chance_percent: f64,
}

impl ChannelConfig {
    /// Deserializes a stored channel config, normalizing fields that must
    /// not be degenerate. `/llm_set` guards `depth` at the command layer,
    /// but a hand-edited storage document can still carry `0` - which would
    /// drop every turn (even the newest) from the context. Every parse site
    /// goes through here so the guard cannot drift.
    pub(crate) fn from_stored(raw: serde_json::Value) -> serde_json::Result<Self> {
        let mut config: Self = serde_json::from_value(raw)?;
        if config.history_depth == 0 {
            config.history_depth = 1;
        }
        Ok(config)
    }

    /// Fresh configuration with defaults and the given model - what
    /// `/llm_assign` stores before any per-channel tuning exists.
    #[must_use]
    pub fn assigned(model: String) -> Self {
        Self {
            model,
            params: GenParams::default(),
            system_prompt: None,
            compaction_enabled: true,
            compaction_model: None,
            compaction_prompt: None,
            history_depth: default_history_depth(),
            capture_mode: CaptureMode::default(),
            streaming: false,
            random_chance_percent: default_random_chance(),
            random_cooldown_secs: default_random_cooldown(),
            max_length: None,
            turn_template: None,
            context_budget_tokens: None,
            images: false,
            image_model: None,
            image_prompt: None,
            react: false,
            random_react_chance_percent: default_random_react_chance(),
        }
    }
}

/// Per-channel completion statistics and context calibration state
/// (`channel:{id}:stats`). Observability + estimation only: unreadable
/// stats never affect replies, they only degrade the token estimate back
/// to the default ratio.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct UsageStats {
    /// Token usage of the last completion, when the endpoint reports it.
    pub last: Option<TokenUsage>,
    /// Complete response time of the last chat completion and its source.
    /// Recorded for every completed answer - also from endpoints that do
    /// not report token usage.
    pub last_timing: Option<ResponseTiming>,
    /// Rolling estimate of tokens per character of assembled context,
    /// blended from the endpoint's own usage reports (EWMA). The estimate
    /// sizes the token-budget fill; the default is typical English prose
    /// (~4 chars/token) and self-corrects after the first request.
    pub tokens_per_char: f64,
    /// The effective prompt budget the engine last enforced (channel
    /// override or model-window derived); `None` while filling is
    /// count-only. `/llm_status` shows it so admins can see which
    /// mechanism is active.
    pub last_budget: Option<u64>,
}

impl Default for UsageStats {
    fn default() -> Self {
        Self { last: None, last_timing: None, tokens_per_char: 0.25, last_budget: None }
    }
}

/// Blends a newly observed ratio into the rolling estimate. 30% weight on
/// the newest request keeps the estimate responsive to topic/language
/// switches while staying stable against single outliers; clamped to
/// ratios that can plausibly occur in chat text.
pub(crate) fn blend_ratio(previous: f64, prompt_tokens: u64, context_chars: u64) -> f64 {
    if context_chars == 0 {
        return previous;
    }
    // Lossy on purpose: a ratio estimate tolerates precision loss by
    // definition, and chat-sized counts are far below f64's exact range.
    #[allow(clippy::cast_precision_loss)]
    let observed = prompt_tokens as f64 / context_chars as f64;
    (previous * 0.7 + observed * 0.3).clamp(0.05, 2.0)
}

fn default_true() -> bool {
    true
}

fn default_history_depth() -> u32 {
    100
}

fn default_random_chance() -> f64 {
    2.0
}

/// Plugin default of the per-channel silent-react chime chance - higher than
/// the reply chime: an emoji is a much lighter interruption than a message.
fn default_random_react_chance() -> f64 {
    10.0
}

/// Plugin default of the per-channel chime cooldown (`/llm_set
/// random_cooldown clear` resets to this).
pub(crate) fn default_random_cooldown() -> u64 {
    5
}

/// Live conversation state of one channel. One document per channel, so the
/// compaction commit (new summary + advanced cutoff) is a single atomic
/// write: a crash mid-compaction leaves the old state intact.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConversationState {
    /// Compacted context so far; `None` until the first compaction ran.
    pub summary: Option<String>,
    /// Records up to and including this sequence are represented by the
    /// summary (or were cut off by `/llm_cutoff`); the live window starts
    /// strictly after it. Records are never deleted.
    pub cutoff_seq: u64,
    /// Unix seconds when the cutoff last moved (compaction or cutoff);
    /// `None` while the full history is the live window.
    #[serde(default)]
    pub cutoff_at: Option<u64>,
}

/// Unix seconds right now - capture timestamps and cutoff markers.
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assigned_config_survives_storage_roundtrip_with_defaults() {
        let config = ChannelConfig::assigned("local/gemma".to_owned());
        let json = serde_json::to_value(&config).expect("config expected to serialize");
        let back: ChannelConfig =
            serde_json::from_value(json).expect("config expected to deserialize");
        assert_eq!(config, back);
        assert_eq!(config.history_depth, 100);
        assert!(config.compaction_enabled);
        assert!((config.random_chance_percent - 2.0).abs() < f64::EPSILON);
        assert_eq!(config.random_cooldown_secs, 5);
        assert_eq!(config.capture_mode, CaptureMode::BotRelated);
        assert_eq!(config.context_budget_tokens, None);
        assert!(!config.images);
        assert_eq!(config.image_model, None);
        assert!(!config.react);
        assert!((config.random_react_chance_percent - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn config_deserializes_minimal_document_with_defaults() {
        let config: ChannelConfig = serde_json::from_value(serde_json::json!({
            "model": "zai/glm-5.3-flash"
        }))
        .expect("minimal config expected to deserialize");
        assert_eq!(config.model, "zai/glm-5.3-flash");
        assert_eq!(config.params, GenParams::default());
        assert_eq!(config.history_depth, 100);
        assert!(config.compaction_enabled);
        assert!(!config.streaming);
        assert!((config.random_chance_percent - 2.0).abs() < f64::EPSILON);
        assert_eq!(config.random_cooldown_secs, 5);
        assert_eq!(config.max_length, None);
        // Stored before the feature existed: serde defaults keep it loadable.
        assert!(!config.react);
        assert!((config.random_react_chance_percent - 10.0).abs() < f64::EPSILON);
    }

    /// A hand-edited storage doc can carry `depth: 0` (the `/llm_set` guard
    /// cannot see it): the stored-parse path must clamp it, or the context
    /// would assemble with zero turns - not even the newest one.
    #[test]
    fn from_stored_clamps_a_hand_edited_zero_depth() {
        let config = ChannelConfig::from_stored(serde_json::json!({
            "model": "zai/glm-5.3-flash",
            "history_depth": 0
        }))
        .expect("config expected to deserialize");
        assert_eq!(config.history_depth, 1);
    }

    #[test]
    fn capture_mode_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(CaptureMode::BotRelated).expect("serializable"),
            serde_json::json!("bot_related")
        );
        assert_eq!(
            serde_json::to_value(CaptureMode::AllMessages).expect("serializable"),
            serde_json::json!("all_messages")
        );
    }

    #[test]
    fn keys_scope_by_channel() {
        assert_eq!(channel_config_key(42), "channel:42");
        assert_eq!(channel_state_key(42), "channel:42:state");
        assert_eq!(channel_stats_key(42), "channel:42:stats");
        assert_ne!(channel_config_key(42), channel_state_key(42));
        assert_ne!(channel_state_key(42), channel_stats_key(42));
        // Record namespaces are plugin-prefixed and channel-partitioned.
        assert_eq!(records_namespace(42), "llm:c:42");
        assert_ne!(records_namespace(42), records_namespace(43));
    }

    #[test]
    fn state_deserializes_without_cutoff_at() {
        let state: ConversationState =
            serde_json::from_value(serde_json::json!({"summary": null, "cutoff_seq": 7}))
                .expect("state expected to deserialize");
        assert_eq!(state.cutoff_seq, 7);
        assert_eq!(state.cutoff_at, None);
    }

    #[test]
    fn usage_stats_default_and_blend() {
        let stats = UsageStats::default();
        assert_eq!(stats.last, None);
        assert!((stats.tokens_per_char - 0.25).abs() < f64::EPSILON);

        // Blend moves 30% toward the observation, clamped to plausible chat
        // ratios; Cyrillic-heavy channels drift up, English stays low.
        let blended = blend_ratio(0.25, 300, 1000);
        assert!((blended - (0.25 * 0.7 + 0.3 * 0.3)).abs() < 1e-9);
        assert!((blend_ratio(0.25, 300, 0) - 0.25).abs() < 1e-9); // nothing observed
        assert!((blend_ratio(0.25, 10_000, 1000) - 2.0).abs() < 1e-9); // clamped high
        // One observation only moves the EWMA 30% - the low clamp bites when
        // an already-low estimate observes an even lower ratio.
        assert!((blend_ratio(0.05, 1, 1000) - 0.05).abs() < 1e-9); // clamped low
        assert!((blend_ratio(2.0, 10, 1000) - 1.403).abs() < 1e-9); // inside: pure EWMA
    }

    #[test]
    fn usage_stats_survive_storage_roundtrip() {
        let stats = UsageStats {
            last: Some(TokenUsage {
                prompt_tokens: 100,
                completion_tokens: 10,
                total_tokens: 110,
                cached_tokens: Some(40),
                reasoning_tokens: None,
            }),
            last_timing: Some(ResponseTiming::reported(50_237)),
            tokens_per_char: 0.31,
            last_budget: Some(6176),
        };
        let json = serde_json::to_value(&stats).expect("stats expected to serialize");
        let back: UsageStats = serde_json::from_value(json).expect("stats expected to deserialize");
        assert_eq!(back, stats);

        // Old stats documents (no last_budget, no last_timing) load with
        // those fields cleared.
        let legacy: UsageStats = serde_json::from_value(serde_json::json!({
            "last": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            "tokens_per_char": 0.3
        }))
        .expect("legacy stats expected to deserialize");
        assert_eq!(legacy.last_budget, None);
        assert_eq!(legacy.last_timing, None);
    }
}
