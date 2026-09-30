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
#[derive(Debug, Clone, Default)]
pub struct OutboundMessage {
    pub content: String,
    pub embeds: Vec<Embed>,
    pub ephemeral: bool,
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
}
