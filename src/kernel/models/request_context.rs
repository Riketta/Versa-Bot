use super::{EventKind, EventPayload, MessagePayload, MessagePurgePayload, Origin};

/// Chat-agnostic inbound EVENT, not a request: there is no response value.
/// A driving adapter normalizes platform events onto this taxonomy and pushes
/// each through the middleware pipeline. A plugin produces output (replies,
/// reactions, presence) by calling event-scoped outbound ports; an event no
/// plugin handles simply yields no output.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub kind: EventKind,
    pub origin: Origin,
    pub payload: EventPayload,
}

impl RequestContext {
    #[must_use]
    pub fn message_received(origin: Origin, content: impl Into<String>) -> Self {
        Self {
            kind: EventKind::MessageReceived,
            origin,
            payload: EventPayload::Message(MessagePayload {
                content: content.into(),
                attachments: Vec::new(),
                author_name: None,
                guild_name: None,
                author_roles: Vec::new(),
                // 0 = unknown; the driving adapter overwrites this with the
                // platform's permission bits when it has them.
                author_permissions: 0,
                reply_to: None,
                mentions_bot: false,
            }),
        }
    }

    /// A text edit of a previously seen message: same payload shape as
    /// [`RequestContext::message_received`], with the content holding the
    /// NEW text. The adapter fills the descriptive fields after building.
    #[must_use]
    pub fn message_edited(origin: Origin, content: impl Into<String>) -> Self {
        let mut event = Self::message_received(origin, content);
        event.kind = EventKind::MessageEdited;
        event
    }

    /// A single message deletion: no payload - the origin names the message.
    #[must_use]
    pub fn message_deleted(origin: Origin) -> Self {
        Self { kind: EventKind::MessageDeleted, origin, payload: EventPayload::Empty }
    }

    /// A mass deletion: only the count is known.
    #[must_use]
    pub fn message_purged(origin: Origin, count: usize) -> Self {
        Self {
            kind: EventKind::MessagePurged,
            origin,
            payload: EventPayload::Purge(MessagePurgePayload { count }),
        }
    }
}
