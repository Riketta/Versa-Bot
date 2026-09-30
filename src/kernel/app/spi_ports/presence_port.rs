use async_trait::async_trait;

use crate::kernel::models::{OutboundError, Presence};

/// Driven port for updating the bot's own presence (status/activity).
/// Presence is a global concern - it is never guild-scoped.
#[async_trait]
pub trait PresencePort: Send + Sync {
    /// Replaces the current presence. Fails when the platform connection
    /// cannot accept the update yet (e.g. gateway not ready); callers treat
    /// that as best effort.
    async fn set(&self, presence: Presence) -> Result<(), OutboundError>;
}
