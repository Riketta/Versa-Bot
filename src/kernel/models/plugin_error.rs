#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("plugin init failed: {0}")]
    Init(String),
    #[error("plugin start failed: {0}")]
    Start(String),
    #[error("plugin stop failed: {0}")]
    Stop(String),
    #[error("invalid kernel configuration: {0}")]
    Invalid(String),
}
