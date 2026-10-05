use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{ChatOutputPort, ChatStreamPort, ReactionPort, UndeliverableReactionPort};
use crate::kernel::models::{ChannelId, MessageId, Origin};

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

    /// Streaming port bound to the event's origin channel - for plugins that
    /// render long answers progressively (create once, edit as content
    /// arrives). Origins that cannot stream (transactional reply tokens,
    /// channel-less events) yield a non-deliverable port; a plain
    /// `chat_output` remains available for those origins.
    fn stream_output(&self, origin: &Origin) -> Arc<dyn ChatStreamPort>;

    /// Platform URL pointing at a message inside the origin event's guild
    /// (Discord: a clickable message link), or `None` when the platform has
    /// no such concept or the origin carries no guild. Cosmetic data - the
    /// adapter owns URL formats, plugins stay platform-blind.
    fn message_link(
        &self,
        origin: &Origin,
        channel_id: ChannelId,
        message_id: MessageId,
    ) -> Option<String>;

    /// Display name of the platform this factory serves, as the adapter
    /// wants it rendered in user-facing text ("Discord") - prompt-template
    /// material, not a kernel concept. Same ownership rule as
    /// `message_link`: the adapter owns presentation, plugins stay
    /// platform-blind.
    fn platform_name(&self) -> &str;

    /// Starts the platform typing indicator for the event's origin channel
    /// and keeps refreshing it on the platform's cadence until the returned
    /// guard is dropped - long operations (LLM answer generation) hold it so
    /// users see the bot composing instead of frozen. Channel-less origins
    /// (member lifecycle) yield an already-dead guard. Fire-and-forget:
    /// indicator failures are the adapter's log concern, never the caller's.
    fn start_typing(&self, origin: &Origin) -> ChatTypingGuard;

    /// Reaction port bound to the event's origin channel - for plugins that
    /// decorate a delivered message with emoji reactions (the LLM plugin's
    /// tool protocol). Origins that cannot react (DMs, channel-less events)
    /// and platforms without the concept yield the undeliverable default;
    /// callers treat per-token failures as cosmetic (log, continue).
    fn react(&self, origin: &Origin) -> Arc<dyn ReactionPort> {
        let _ = origin;
        Arc::new(UndeliverableReactionPort)
    }
}

/// RAII token for a platform typing indicator: dropping it stops the
/// adapter's refresh. Deliberately opaque - callers hold it, adapters own
/// everything behind it.
#[derive(Debug)]
pub struct ChatTypingGuard {
    cancel: CancellationToken,
}

impl ChatTypingGuard {
    pub(crate) fn new(cancel: CancellationToken) -> Self {
        Self { cancel }
    }

    /// A guard that never started typing (and stopping it is a no-op).
    pub(crate) fn dead() -> Self {
        let cancel = CancellationToken::new();
        cancel.cancel();
        Self { cancel }
    }
}

impl Drop for ChatTypingGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
