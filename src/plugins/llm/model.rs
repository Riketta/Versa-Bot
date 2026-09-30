//! Domain model and storage schema of the LLM chat plugin. Configuration is
//! per-channel (the channel is the scope users think in); conversation state
//! is per-channel too; the service channel is per-guild.

use serde::{Deserialize, Serialize};

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
    /// Reasoning/thinking effort hint (e.g. `low`/`medium`/`high`). Sent
    /// only when the requested model declares reasoning support; ignored
    /// otherwise.
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
    /// (`0` = off). Cooldown-guarded by the plugin.
    #[serde(default = "default_random_chance")]
    pub random_chance_percent: f64,
    /// Reply splitting limit override; `None` = plugin-wide default.
    #[serde(default)]
    pub max_length: Option<usize>,
}

impl ChannelConfig {
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
            max_length: None,
        }
    }
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
        assert_eq!(config.capture_mode, CaptureMode::BotRelated);
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
        assert_eq!(config.max_length, None);
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
        assert_ne!(channel_config_key(42), channel_state_key(42));
    }
}
