use std::any::Any;

use super::{ChannelId, GuildId, MessageId, UserId};

/// Chat platform an event originated from.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Platform {
    Discord,
}

impl Platform {
    /// Stable storage/telemetry key for the platform.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Discord => "discord",
        }
    }
}

/// Where an inbound event came from. Scopes event-driven outbound ports:
/// a `ChatOutputPort` built from an origin sends to `channel_id` inside
/// `guild_id`, so a plugin replies without a returned response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Origin {
    pub platform: Platform,
    /// `None` for direct messages.
    pub guild_id: Option<GuildId>,
    pub channel_id: ChannelId,
    /// Actor that triggered the event (message author, joined member, etc.).
    pub user_id: UserId,
    /// Present for message lifecycle events.
    pub message_id: Option<MessageId>,
}

/// Chat-agnostic inbound event taxonomy. Driving adapters normalize native
/// platform events onto these kinds; middleware plugins match on the kinds
/// they care about and ignore the rest.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    MessageReceived,
    MessageEdited,
    MessageDeleted,
    MemberJoined,
    MemberLeft,
    PresenceUpdate,
}

/// Normalized per-kind event data.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum EventPayload {
    Message(MessagePayload),
    Member(MemberPayload),
    /// Kinds that carry no normalized payload yet (e.g. `PresenceUpdate`).
    Empty,
}

#[derive(Debug, Clone)]
pub struct MessagePayload {
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct MemberPayload {
    pub username: Option<String>,
}

/// Base trait for plugin-owned domain events published over `EventBusPort`.
/// Plugins own their events (e.g. `UserJoinedGuild` lives in the tracker
/// plugin); the kernel routes them but never defines their meaning.
/// The bus never carries raw inbound events - only these derived events.
pub trait Event: Any + Send + Sync {
    fn name(&self) -> &'static str;

    fn as_any(&self) -> &dyn Any;
}
