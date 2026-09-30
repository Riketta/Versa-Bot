use serde::Deserialize;

/// Bot status rotation: the presence activity cycles through `statuses`
/// every `interval_seconds`. Presence is a global concern, so this lives in
/// the bot configuration, not in guild storage. An absent section, an empty
/// list, or a zero interval disables the rotation.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct StatusConfig {
    pub interval_seconds: u64,
    pub statuses: Vec<String>,
}
