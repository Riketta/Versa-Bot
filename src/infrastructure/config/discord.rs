use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct DiscordConfig {
    pub token: String,
    pub proxy: Option<String>,
}
