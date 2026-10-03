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
///
/// Currently emitted kinds: `MessageReceived`, `MemberJoined`, `MemberLeft`,
/// `CommandInvoked`. The remaining variants are reserved for future adapters
/// - no adapter emits them yet, so a plugin matching on them never fires
/// (listed here so the taxonomy is not read as a false contract).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    MessageReceived,
    /// Reserved: no adapter emits this yet. Editing is append-hostile for
    /// the LLM record log, so a real implementation needs a consumer-driven
    /// policy (e.g. record rewrites vs. compensating events) first.
    MessageEdited,
    /// Reserved: no adapter emits this yet.
    MessageDeleted,
    MemberJoined,
    MemberLeft,
    /// Reserved: no adapter emits inbound presence updates yet - the bot
    /// only SETS its own presence (`PresencePort`), it does not consume
    /// anyone else's.
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
    /// Platform permission bits of the author (`0` = unknown); opaque
    /// pass-through data the auth plugin interprets.
    pub author_permissions: u64,
}

/// One file attached to an inbound message, normalized platform-blind. The
/// URL is the platform's own CDN link, filled by the driving adapter - the
/// only host guild input may ever name (the adapter guarantees it points at
/// the platform CDN; plugins fetch it under their own size-cap policy).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentPayload {
    pub url: String,
    /// MIME type when the platform reported one.
    pub content_type: Option<String>,
    pub file_name: Option<String>,
    pub size_bytes: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct MessagePayload {
    pub content: String,
    /// Files attached to the message (images feed the LLM plugin's image
    /// recognition); empty for platforms/events without attachments.
    pub attachments: Vec<AttachmentPayload>,
    /// Author display name at capture time (channel nick, else platform
    /// username; best effort). Lets history consumers render `{sender}:
    /// {message}` context lines without a gateway cache.
    pub author_name: Option<String>,
    /// Opaque platform role identifiers of the author, filled by driving
    /// adapters when the platform provides them (empty otherwise). Lets
    /// guild plugins do role checks without platform-specific types.
    pub author_roles: Vec<String>,
    /// Platform permission bits of the author (`0` = unknown); opaque
    /// pass-through data the auth plugin interprets.
    pub author_permissions: u64,
    /// Message this one replies to, when the platform provides the
    /// reference. Lets conversation-history consumers follow reply chains.
    pub reply_to: Option<MessageId>,
    /// True when the message mentions this bot - resolved by the driving
    /// adapter, which owns the bot identity. Core plugins stay blind to
    /// platform mention syntax.
    pub mentions_bot: bool,
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
