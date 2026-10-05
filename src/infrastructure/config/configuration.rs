use serde::Deserialize;

use super::discord::DiscordConfig;
use super::llm::LlmConfig;
use super::logging::LoggingConfig;
use super::lol_leaderboard::LolLeaderboardConfig;
use super::lol_store::LolStoreConfig;
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
    /// Optional persistent file logging (`[logging]` section). Startup-only:
    /// the tracing subscriber is installed once at boot; changes require a
    /// restart. Absent section keeps stdout (and Sentry) only.
    #[serde(default)]
    pub logging: Option<LoggingConfig>,
    /// Optional LLM runtime (`[llm]` section): providers, model capabilities
    /// and engine defaults. Startup-only - provider clients and API keys are
    /// built once at boot; changes require a restart.
    #[serde(default)]
    pub llm: Option<LlmConfig>,
    /// Optional bot status rotation (`[status]` section). Global concern:
    /// interval and status list are bot-wide, not per-guild.
    pub status: Option<StatusConfig>,
    /// Optional League-client store watcher (`[lol_store]` section).
    /// Startup-only: absent section - or an empty `lockfile_path` - keeps
    /// the watcher off.
    #[serde(default)]
    pub lol_store: Option<LolStoreConfig>,
    /// Optional leaderboard command (`[lol_leaderboard]` section).
    /// Startup-only: an absent section - or an empty `regions` list - keeps
    /// the command in "not configured" mode.
    #[serde(default)]
    pub lol_leaderboard: Option<LolLeaderboardConfig>,
    pub storage: StorageConfig,
}
