#[derive(Debug, thiserror::Error)]
pub enum OutboundError {
    #[error("failed to send message: {0}")]
    Send(String),
    /// A reaction could not be applied (unknown/unresolvable emoji, missing
    /// permissions, undeliverable origin). Cosmetic per contract - callers
    /// log and continue with the remaining tokens.
    #[error("reaction failed: {0}")]
    Reaction(String),
}
