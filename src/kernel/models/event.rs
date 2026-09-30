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
#[derive(Debug, Clone)]
pub struct Origin {
    pub platform: Platform,
    /// `None` for direct messages.
    pub guild_id: Option<GuildId>,
    /// Channel the event belongs to and where an origin-bound reply lands.
    /// `ChannelId(0)` marks channel-less events (member join/leave, presence):
    /// they carry no channel, and replying to them is meaningless - plugins
    /// that need a real channel pick a configured one via
    /// `ChatOutputFactoryPort::channel_output`.
    pub channel_id: ChannelId,
    /// Actor that triggered the event (message author, joined member, etc.).
    pub user_id: UserId,
    /// Present for message lifecycle events and command invocations
    /// (the interaction id).
    pub message_id: Option<MessageId>,
    /// Opaque platform reply token for transactional events (e.g. a Discord
    /// interaction token: replies must go through the interaction callback,
    /// not a plain channel message). Set by the driving adapter; consumed by
    /// its own response factory. The kernel treats it as opaque bytes.
    pub reply_token: Option<String>,
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
    /// A native platform command was invoked (e.g. Discord slash command).
    /// Prefix parsing for platforms without native commands is an adapter
    /// concern - it synthesizes this kind too, so the dispatcher is shared.
    CommandInvoked,
}

/// Normalized per-kind event data.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum EventPayload {
    Message(MessagePayload),
    Member(MemberPayload),
    /// A native platform command invocation (see [`EventKind::CommandInvoked`]).
    Command(CommandPayload),
    /// Kinds that carry no normalized payload yet (e.g. `PresenceUpdate`).
    Empty,
}

/// A resolved native command: its registered name and string arguments.
/// Platform value types (users, channels, roles) arrive as their IDs; typed
/// resolution is a plugin concern via its own platform knowledge.
#[derive(Debug, Clone)]
pub struct CommandPayload {
    pub name: String,
    pub args: Vec<(String, String)>,
    /// Opaque platform role identifiers of the invoking user (same contract
    /// as `MessagePayload::author_roles`).
    pub author_roles: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MessagePayload {
    pub content: String,
    /// Opaque platform role identifiers of the author, filled by driving
    /// adapters when the platform provides them (empty otherwise). Lets
    /// guild plugins do role checks without platform-specific types.
    pub author_roles: Vec<String>,
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
