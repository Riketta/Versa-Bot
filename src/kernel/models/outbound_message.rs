/// Outbound chat message delivered via an event-scoped `ChatOutputPort`.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub content: String,
}

impl OutboundMessage {
    #[must_use]
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
        }
    }
}
