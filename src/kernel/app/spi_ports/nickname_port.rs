use async_trait::async_trait;

use crate::kernel::models::{GuildId, OutboundError};

/// Driven port for the bot's guild-local display name (Discord maps this to
/// the member nickname). Every guild can carry its own name - unlike
/// [`PresencePort`](super::PresencePort), presence is global while this is
/// guild-scoped. The platform owns the stored value; nothing is kept
/// kernel-side.
#[async_trait]
pub trait NicknamePort: Send + Sync {
    /// Sets the name, or resets it to the bot's platform-wide name when
    /// `None`. Fails when the platform cannot apply the change yet (gateway
    /// not ready) or rejects it (e.g. the bot lacks the nickname permission
    /// in that guild) - callers answer the invoking member accordingly.
    async fn set(&self, guild: GuildId, name: Option<&str>) -> Result<(), OutboundError>;
}
