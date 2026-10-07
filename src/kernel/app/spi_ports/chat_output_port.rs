use async_trait::async_trait;

use crate::kernel::models::{OutboundError, OutboundMessage};

/// Event-scoped outbound port: `send` lands in the origin channel/guild of
/// the event being processed. The kernel binds it per event via
/// `ChatOutputFactoryPort` and carries it in the event-scoped service
/// context - this is how a plugin knows where to reply without a returned
/// response. An event may yield zero, one, or many sends.
#[async_trait]
pub trait ChatOutputPort: Send + Sync {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError>;

    /// Resolves an open transactional slot (e.g. a deferred interaction)
    /// without delivering any output: the pending acknowledgment is
    /// consumed and cleaned up silently. For outputs without a pending
    /// slot this is a no-op. Never sends content - callers use it instead
    /// of a denial when silence itself is the policy (bans).
    async fn dismiss(&self) {}
}
