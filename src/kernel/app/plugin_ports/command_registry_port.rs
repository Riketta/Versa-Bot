//! Plugin-facing contract for the command registry: plugins register command
//! descriptors (meaning) during `init()`; the kernel aggregates them without
//! interpreting them. Platform command registration (e.g. Discord slash
//! sync) consumes the registry from the adapter side.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use async_trait::async_trait;

use crate::kernel::{models::RequestContext, services::KernelServices};

/// A registered command's executable half. Implemented by the owning plugin;
/// runs inside the pipeline with the event itself (origin: channel, guild,
/// user - for channel-anchored commands like assign-in-place), its string
/// arguments, and event-scoped services.
#[async_trait]
pub trait CommandHandler: Send + Sync {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()>;
}

/// String arguments of a resolved command invocation.
#[derive(Debug, Clone, Default)]
pub struct CommandArgs(pub Vec<(String, String)>);

impl CommandArgs {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.iter().find(|(arg_name, _)| arg_name == name).map(|(_, value)| value.as_str())
    }
}

/// Meaning-free aggregation of plugin commands. Single instance per kernel,
/// shared with plugins at construction (same pattern as the event bus).
pub trait CommandRegistryPort: Send + Sync {
    /// Registers (or replaces) a command by its descriptor's name.
    fn register(&self, descriptor: CommandDescriptor, handler: Arc<dyn CommandHandler>);

    fn lookup(&self, name: &str) -> Option<Arc<dyn CommandHandler>>;

    /// A registered command's descriptor by name - how the auth plugin reads
    /// a command's declared tier at dispatch time without owning its meaning.
    fn descriptor(&self, name: &str) -> Option<CommandDescriptor>;

    fn descriptors(&self) -> Vec<CommandDescriptor>;
}

#[derive(Debug, Clone)]
pub struct CommandDescriptor {
    pub plugin_id: String,
    pub name: String,
    pub description: String,
    pub arguments: Vec<ArgDescriptor>,
    /// Platform-presentation gate (`default_member_permissions` on Discord).
    /// Only commands whose audience matches a native platform permission
    /// set it; tier-based access uses `required_tier`. No core command
    /// declares one today: platform-derived ranks (`GuildAdmin`) are
    /// enforced by the auth plugin instead, keeping tier-gated commands
    /// visible to everyone.
    pub required_permission: Option<Permission>,
    /// Kernel-side ACL data: the minimum [`AccessTier`] a caller needs. The
    /// kernel never interprets it - the auth plugin compares it against the
    /// caller's effective tier and denies with an ephemeral notice. Every
    /// command declares a tier; absent means any non-banned caller.
    pub required_tier: Option<AccessTier>,
    /// Command is guild-scoped: adapters hide it from direct messages
    /// (Discord: `dm_permission: false`).
    pub guild_only: bool,
}

/// Access ladder, ordered from least to most privileged (derive order is
/// the rank). Declared by plugins on `CommandDescriptor` as ACL data;
/// interpreted exclusively by the auth plugin, which computes each caller's
/// effective tier and compares. `Owner` is deployment-global (operator
/// config) and `GuildAdmin` is derived from the platform's server-management
/// permission - everything below is per-guild policy. Neither of the two can
/// be produced by stored guild data: the auth plugin's resolution clamps
/// server managers up to `GuildAdmin`, and sanitization keeps stored tiers
/// at `Admin` - those guarantees live there, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
// `snake_case`, not `lowercase`: single-word variants are unaffected, and
// it keeps the JSON spelling of `GuildAdmin` equal to [`AccessTier::as_str`]
// (`guild_admin`) - hand-edited policy documents using the documented
// spelling must deserialize.
#[serde(rename_all = "snake_case")]
pub enum AccessTier {
    /// Ignored entirely in guild contexts: messages and commands are
    /// dropped without any output - bans never announce themselves. DMs
    /// carry no guild policy, so the few guild-legal commands remain
    /// invocable there.
    Banned,
    /// May talk to the bot (chat interactions) but runs no commands.
    Guest,
    /// Basic interactions: chat plus read-only/basic commands.
    User,
    /// Service management: every plugin's operational commands, plus the
    /// limited member-facing slice of `/auth` (limited `show`, tier writes
    /// to lower-ranked members only).
    Moderator,
    /// Everything except policy writes touching `Admin`-ranked members.
    Admin,
    /// Guild administration, derived from the platform - never stored.
    /// Holders of the platform's server-management permission (Discord:
    /// Manage Server; the guild owner has it by construction) resolve here,
    /// above every stored rank: no `/auth` write can grant or revoke it.
    /// Never offered as a command requirement - `Admin` is the highest
    /// assignable tier.
    GuildAdmin,
    /// Bot deployment owner. Injected from the operator's config (never
    /// guild data), above every guild-side rank: bans, assignments, and the
    /// permission clamps never reach an owner. Only the config-injected
    /// identity list can produce this tier - it is not assignable through
    /// `/auth`.
    Owner,
}

impl AccessTier {
    /// JSON spelling - also the Discord choice value for `/auth tier=`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AccessTier::Banned => "banned",
            AccessTier::Guest => "guest",
            AccessTier::User => "user",
            AccessTier::Moderator => "moderator",
            AccessTier::Admin => "admin",
            AccessTier::GuildAdmin => "guild_admin",
            AccessTier::Owner => "owner",
        }
    }
}

impl fmt::Display for AccessTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AccessTier::Banned => "Banned",
            AccessTier::Guest => "Guest",
            AccessTier::User => "User",
            AccessTier::Moderator => "Moderator",
            AccessTier::Admin => "Admin",
            AccessTier::GuildAdmin => "Guild Admin",
            AccessTier::Owner => "Owner",
        })
    }
}

#[derive(Debug, Clone)]
pub struct ArgDescriptor {
    pub name: String,
    pub description: String,
    pub required: bool,
    /// What kind of value the argument takes. Adapters map it onto native
    /// option types (Discord: user/role pickers); the resolved entity arrives
    /// in `args` as its ID string.
    pub kind: ArgKind,
    /// Fixed value set for a string argument; adapters render it as a native
    /// dropdown (Discord: option `choices`).
    pub choices: Option<Vec<String>>,
}

/// Platform-blind argument typing, mapped by adapters onto native option
/// types. Grows on demand (integer, channel, ...).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgKind {
    String,
    User,
    Role,
    /// A guild channel. The adapter renders a native channel picker
    /// (Discord: option type `CHANNEL`); the resolved entity arrives in
    /// `args` as the channel's ID string. Same-guild by construction -
    /// pickers only offer channels of the invoking guild.
    Channel,
    /// An uploaded file. The adapter hands the plugin a platform URL the
    /// plugin may fetch - Discord: the attachment's CDN URL, a pinned
    /// trusted host (never an arbitrary guild-chosen URL).
    Attachment,
}

/// A native platform permission name, mapped by adapters onto platform
/// mechanics (Discord: `default_member_permissions`). No core command
/// declares one today - see [`CommandDescriptor::required_permission`].
/// to gate command *presentation*. Kernel-side access control is not this -
/// it is `required_tier`, enforced by the auth plugin.
#[derive(Debug, Clone)]
pub struct Permission {
    pub name: String,
}
