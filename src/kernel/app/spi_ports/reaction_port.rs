use async_trait::async_trait;

use crate::kernel::models::{MessageId, OutboundError};

/// Event-scoped outbound port bound to the event's origin channel: reacts to
/// a message the plugin just dealt with (e.g. an LLM answer decorating the
/// message that provoked it). Obtained per origin via
/// [`crate::kernel::app::spi_ports::ChatOutputFactoryPort::react`]; origins
/// that cannot react (DMs, channel-less events, platforms without the
/// concept) yield the undeliverable implementation.
///
/// Cosmetic side effect by contract: failures are per-token and never fatal -
/// a caller decorates an already-delivered result, so one skipped emoji must
/// not touch the answer itself.
#[async_trait]
pub trait ReactionPort: Send + Sync {
    /// Reacts to a message in the origin event's channel. `emoji` is the raw
    /// platform-facing token: a Unicode emoji, a platform custom form
    /// (Discord `:name:` - resolved by the adapter against the guild's
    /// emojis - or the fully qualified `<:name:id>` / `<a:name:id>`).
    /// Unresolvable tokens (e.g. a custom emoji from another guild) fail
    /// per-token; valid sibling tokens are unaffected.
    async fn add_reaction(&self, message_id: MessageId, emoji: &str) -> Result<(), OutboundError>;
}

/// The port handed out for origins that cannot react (DMs, channel-less
/// member lifecycle events, adapters that never override the factory
/// default). Every call fails - visibly in debug logs, never as a panic.
pub struct UndeliverableReactionPort;

#[async_trait]
impl ReactionPort for UndeliverableReactionPort {
    async fn add_reaction(
        &self,
        _message_id: MessageId,
        _emoji: &str,
    ) -> Result<(), OutboundError> {
        Err(OutboundError::Reaction("this origin has no reactable message".to_owned()))
    }
}
