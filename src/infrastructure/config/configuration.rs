use serde::Deserialize;

use super::discord::DiscordConfig;
use super::llm::LlmConfig;
use super::sentry::SentryConfig;
use super::status::StatusConfig;
use super::storage::StorageConfig;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Configuration {
    /// Verbose logging fallback when `RUST_LOG` is unset: the bot's own
    /// internals at debug, third-party HTTP stack (reqwest/hyper) at warn.
    #[serde(default)]
    pub debug: bool,
    pub discord: DiscordConfig,
    /// Absent or empty `dsn` disables Sentry reporting entirely.
    pub sentry: Option<SentryConfig>,
    /// Optional LLM runtime (`[llm]` section): providers, model capabilities
    /// and engine defaults. Startup-only - provider clients and API keys are
    /// built once at boot; changes require a restart.
    #[serde(default)]
    pub llm: Option<LlmConfig>,
    /// Optional bot status rotation (`[status]` section). Global concern:
    /// interval and status list are bot-wide, not per-guild.
    pub status: Option<StatusConfig>,
    pub storage: StorageConfig,
}
