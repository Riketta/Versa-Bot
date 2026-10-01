use std::collections::BTreeMap;

use serde::Deserialize;

/// Global `[llm]` section: LLM provider connections, model capability
/// declarations and engine-wide defaults. This is the bot operator's domain
/// - guild admins only pick among the declared models, never configure
/// endpoints. Startup-only: provider clients and API keys are built once at
/// boot, so `[llm]` changes require a restart (same class as token/storage).
///
/// Mapped onto the plugin-facing `LlmSettings` at the composition root.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct LlmConfig {
    /// System prompt for channels without a per-channel override.
    pub default_system_prompt: String,
    /// Compaction instruction for channels without an override.
    pub default_compaction_prompt: String,
    /// Compaction model fallback (`provider/model`); absent = compact with
    /// the channel's chat model.
    pub compaction_model: Option<String>,
    /// Live messages kept after each compaction.
    pub compaction_keep_tail: u32,
    /// Reply splitting limit; per-channel `max_length` overrides this.
    pub max_message_length: usize,
    /// Streaming edit cadence in milliseconds.
    pub stream_interval_ms: u64,
    /// Cap for `/llm_prompt_file` attachment downloads, in bytes.
    pub max_prompt_file_bytes: u64,
    /// Diagnostic dump of raw LLM request/response bodies at DEBUG level
    /// (stdout only, never Sentry). Off by default: the bodies carry full
    /// conversation content.
    pub log_raw_traffic: bool,
    /// Declared providers (`[llm.providers.<name>`).
    pub providers: BTreeMap<String, LlmProviderConfig>,
    /// Declared model capabilities (`[llm.models."<provider/model>"]`).
    /// Undeclared models are usable but get default (no reasoning)
    /// capabilities.
    pub models: BTreeMap<String, LlmModelConfig>,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            default_system_prompt: "You are a helpful chat assistant.".to_owned(),
            default_compaction_prompt: "Summarize the conversation above, preserving facts, \
                 decisions, names and open questions. Be concise."
                .to_owned(),
            compaction_model: None,
            compaction_keep_tail: 10,
            max_message_length: 2000,
            stream_interval_ms: 2000,
            max_prompt_file_bytes: 131_072,
            log_raw_traffic: false,
            providers: BTreeMap::new(),
            models: BTreeMap::new(),
        }
    }
}

/// One declared OpenAI-compatible provider endpoint.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub struct LlmProviderConfig {
    /// Base URL, e.g. `https://api.z.ai/api/coding/paas/v4`.
    pub api_url: String,
    /// Environment variable holding the API key; absent = keyless (local
    /// llama.cpp). Keys never live in config files or guild storage.
    pub api_key_env: Option<String>,
    /// Optional per-provider proxy (the Discord proxy does not apply here).
    pub proxy: Option<String>,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
    pub reasoning_style: LlmReasoningStyle,
}

impl Default for LlmProviderConfig {
    fn default() -> Self {
        Self {
            api_url: String::new(),
            api_key_env: None,
            proxy: None,
            timeout_secs: 120,
            reasoning_style: LlmReasoningStyle::default(),
        }
    }
}

/// How the reasoning/thinking parameter is rendered on the wire.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LlmReasoningStyle {
    /// `reasoning_effort: "<value>"` (`OpenAI` o-series, Z.ai GLM). No off
    /// wire value: omitted = provider default (Z.ai GLM defaults to `max`).
    #[default]
    OpenaiEffort,
    /// `thinking: {"type": "enabled"|"disabled"}` (GLM boolean thinking
    /// switch; `off` renders an explicit disable). GLM-5.3 series thinks
    /// forcibly regardless - throttle it via `reasoning_effort` instead.
    GlmThinking,
}

/// Capabilities of one declared model.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct LlmModelConfig {
    /// The model accepts a reasoning/thinking parameter. Per-channel
    /// `reasoning_effort` is ignored for models without this.
    pub reasoning: bool,
    /// Total context window in tokens; enables token-budget context filling
    /// for channels that do not set their own budget.
    pub context_window: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_section_deserializes_to_defaults() {
        let config = serde_json::from_str::<LlmConfig>("{}").expect("empty section deserializes");
        assert_eq!(config.compaction_keep_tail, 10);
        assert_eq!(config.max_message_length, 2000);
        assert!(config.providers.is_empty());
        assert!(config.default_system_prompt.contains("chat assistant"));
    }

    #[test]
    fn full_section_deserializes() {
        let config = serde_json::from_str::<LlmConfig>(
            r#"{
                "default_system_prompt": "custom",
                "compaction_model": "zai/glm-5.3-flash",
                "compaction_keep_tail": 5,
                "max_message_length": 1500,
                "stream_interval_ms": 1500,
                "max_prompt_file_bytes": 4096,
                "log_raw_traffic": true,
                "providers": {
                    "zai": {
                        "api_url": "https://api.z.ai/api/coding/paas/v4",
                        "api_key_env": "VERSABOT_LLM_ZAI_KEY",
                        "reasoning_style": "glm_thinking"
                    },
                    "local": { "api_url": "http://127.0.0.1:8001/v1" }
                },
                "models": { "zai/glm-5.3-flash": { "reasoning": true } }
            }"#,
        )
        .expect("section expected to deserialize");

        assert_eq!(config.default_system_prompt, "custom");
        assert_eq!(config.compaction_model.as_deref(), Some("zai/glm-5.3-flash"));
        assert_eq!(config.compaction_keep_tail, 5);
        assert_eq!(config.max_prompt_file_bytes, 4096);
        assert!(config.log_raw_traffic);
        let zai = config.providers.get("zai").expect("zai provider expected");
        assert_eq!(zai.reasoning_style, LlmReasoningStyle::GlmThinking);
        assert_eq!(zai.timeout_secs, 120);
        assert!(config.models.get("zai/glm-5.3-flash").is_some_and(|model| model.reasoning));
    }
}
