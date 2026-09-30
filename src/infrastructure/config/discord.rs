use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct DiscordConfig {
    pub token: String,
    pub proxy: Option<String>,
}
