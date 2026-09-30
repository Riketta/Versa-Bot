use serde::Deserialize;

use super::discord::DiscordConfig;
use super::sentry::SentryConfig;
use super::status::StatusConfig;
use super::storage::StorageConfig;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct Configuration {
    /// Verbose (debug-level) logging fallback when `RUST_LOG` is unset.
    #[serde(default)]
    pub debug: bool,
    pub discord: DiscordConfig,
    /// Absent or empty `dsn` disables Sentry reporting entirely.
    pub sentry: Option<SentryConfig>,
    /// Optional bot status rotation (`[status]` section). Global concern:
    /// interval and status list are bot-wide, not per-guild.
    pub status: Option<StatusConfig>,
    pub storage: StorageConfig,
}
