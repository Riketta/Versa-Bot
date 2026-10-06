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
    /// Deployment-global bot-owner identities (`owners = ["..."]`):
    /// platform user IDs as strings, matched exactly after trimming. Owners
    /// sit above every guild-side access tier and cannot be modified through
    /// the bot - the list lives only here (hot-reloadable). Absent or empty
    /// means no owners.
    #[serde(default)]
    pub owners: Vec<String>,
    pub storage: StorageConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_json() -> String {
        r#"{ "discord": { "token": "t" }, "storage": { "url": "sqlite://db.sqlite3" } }"#.to_owned()
    }

    #[test]
    fn owners_default_to_empty() {
        let config = serde_json::from_str::<Configuration>(&minimal_json())
            .expect("minimal config deserializes");
        assert!(config.owners.is_empty());
    }

    #[test]
    fn owners_deserialize_from_list() {
        let config = serde_json::from_str::<Configuration>(
            r#"{
                "discord": { "token": "t" },
                "storage": { "url": "sqlite://db.sqlite3" },
                "owners": ["162549842042683392", "42"]
            }"#,
        )
        .expect("config with owners deserializes");

        assert_eq!(config.owners, vec!["162549842042683392".to_owned(), "42".to_owned()]);
    }
}
