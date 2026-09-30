use std::sync::Arc;

use super::ChatOutputPort;
use crate::kernel::models::{ChannelId, Origin};

/// Driven port the kernel calls per event to obtain outbound ports bound to
/// that event's origin. Implemented by the driving adapter (e.g. the Discord
/// gateway adapter), which owns the platform connection; the kernel stays
/// platform-agnostic.
pub trait ChatOutputFactoryPort: Send + Sync + 'static {
    /// Port bound to the event's origin: a plain send lands in the source
    /// channel; an origin carrying a reply token goes through the platform's
    /// transactional reply endpoint (interaction followup) instead.
    fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort>;

    /// Port bound to an arbitrary channel *of the origin event's platform and
    /// guild* - for plugins that log to a configured channel (audit, activity
    /// tracking) instead of replying to the source. Scope stays guild-bound:
    /// a plugin cannot reach outside the event's own guild.
    fn channel_output(&self, origin: &Origin, channel_id: ChannelId) -> Arc<dyn ChatOutputPort>;
}
