use async_trait::async_trait;

use crate::kernel::models::{MessageId, OutboundError, OutboundMessage};

/// Streaming outbound port bound to one channel: creates a message and
/// updates it in place while content arrives (progressive rendering of long
/// answers). The handle is the platform message id - updates are stateless
/// for the adapter. Built per origin via
/// `ChatOutputFactoryPort::stream_output`; content-only by design (embeds
/// have no meaningful role in a progressively edited message).
#[async_trait]
pub trait ChatStreamPort: Send + Sync {
    /// Creates the initial message and returns its handle. The initial
    /// content must be non-empty (platforms reject empty messages).
    async fn begin(&self, message: OutboundMessage) -> Result<MessageId, OutboundError>;

    /// Replaces the message content in place. The final update carries the
    /// complete text; oversized content is the caller's concern (split into
    /// follow-up plain sends), not the port's.
    async fn update(&self, message: MessageId, content: String) -> Result<(), OutboundError>;
}
