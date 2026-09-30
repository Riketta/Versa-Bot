use serde::Deserialize;

use super::discord::DiscordConfig;
use super::sentry::SentryConfig;
use super::storage::StorageConfig;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Configuration {
    pub debug: bool,
    pub discord: DiscordConfig,
    /// Absent or empty `dsn` disables Sentry reporting entirely.
    pub sentry: Option<SentryConfig>,
    pub storage: StorageConfig,
}
