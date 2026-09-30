#[derive(Debug, thiserror::Error)]
pub enum OutboundError {
    #[error("failed to send message: {0}")]
    Send(String),
}
