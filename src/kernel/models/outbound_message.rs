use super::MessageId;

/// Minimal embed payload. Deliberately platform-blind: two text fields an
/// adapter can render natively (Discord embeds) or flatten into plain text
/// where the platform has no embed concept.
#[derive(Debug, Clone)]
pub struct Embed {
    pub title: String,
    pub description: String,
}

/// Outbound chat message delivered via an event-scoped `ChatOutputPort`.
///
/// `ephemeral` is a visibility *hint*: only transactional replies (interaction
/// followups) can truly be shown to a single user, so adapters honor it there
/// (Discord: the `EPHEMERAL` message flag) and ignore it for plain channel
/// sends, which are always public.
///
/// `reply_to` is a native platform reply reference (Discord: a reply-chain
/// header, no mention) to a message *in the destination channel* - typically
/// the inbound message the answer answers. Adapters degrade gracefully where
/// the reference is unusable (platform without the concept, deleted target):
/// the send goes out as a normal message, never an error.
#[derive(Debug, Clone, Default)]
pub struct OutboundMessage {
    pub content: String,
    pub embeds: Vec<Embed>,
    pub ephemeral: bool,
    pub reply_to: Option<MessageId>,
}

impl OutboundMessage {
    #[must_use]
    pub fn text(content: impl Into<String>) -> Self {
        Self { content: content.into(), ..Self::default() }
    }

    /// Embed-only message (no plain-text body).
    #[must_use]
    pub fn embed(embed: Embed) -> Self {
        Self { embeds: vec![embed], ..Self::default() }
    }

    /// Marks the message as visible to the event's actor only (see the
    /// `ephemeral` docs for platform caveats).
    #[must_use]
    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }

    /// Marks the message as a native platform reply to `message_id` (a
    /// message in the destination channel).
    #[must_use]
    pub fn replying_to(mut self, message_id: MessageId) -> Self {
        self.reply_to = Some(message_id);
        self
    }

    /// True when neither content nor embeds would be rendered. Adapters must
    /// not send such messages - Discord rejects them with a 400.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.content.is_empty() && self.embeds.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emptiness_is_content_and_embeds() {
        assert!(OutboundMessage::default().is_empty());
        assert!(!OutboundMessage::text("hi").is_empty());
        assert!(
            !OutboundMessage::embed(Embed { title: "t".to_owned(), description: "d".to_owned() })
                .is_empty()
        );
    }
}
