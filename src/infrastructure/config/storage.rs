use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct StorageConfig {
    pub url: String,
}
