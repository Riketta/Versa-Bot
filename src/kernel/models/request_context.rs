use super::{EventKind, EventPayload, MessagePayload, Origin};

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
                author_roles: Vec::new(),
                // 0 = unknown; the driving adapter overwrites this with the
                // platform's permission bits when it has them.
                author_permissions: 0,
                reply_to: None,
                mentions_bot: false,
            }),
        }
    }
}
