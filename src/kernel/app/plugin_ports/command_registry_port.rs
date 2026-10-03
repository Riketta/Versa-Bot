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
    /// (today: only `/auth`) set it; tier-based access uses `required_tier`.
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

/// Per-guild access ladder, ordered from most to least privileged (derive
/// order is the rank). Declared by plugins on `CommandDescriptor` as ACL
/// data; interpreted exclusively by the auth plugin, which computes each
/// caller's effective tier from the guild policy and compares. Discord
/// guild administrators are `Admin` by construction (the auth plugin's
/// resolution clamps them up) - that guarantee lives there, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccessTier {
    /// Ignored entirely: messages and commands are dropped without any
    /// output - bans never announce themselves.
    Banned,
    /// May talk to the bot (chat interactions) but runs no commands.
    Guest,
    /// Basic interactions: chat plus read-only/basic commands.
    User,
    /// Service management: every plugin's operational commands, but no
    /// access-policy management.
    Moderator,
    /// Everything, including `/auth` tier management.
    Admin,
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
    /// An uploaded file. The adapter hands the plugin a platform URL the
    /// plugin may fetch - Discord: the attachment's CDN URL, a pinned
    /// trusted host (never an arbitrary guild-chosen URL).
    Attachment,
}

/// A native platform permission name (`manage_guild` so far), mapped by
/// adapters onto platform mechanics (Discord: `default_member_permissions`)
/// to gate command *presentation*. Kernel-side access control is not this -
/// it is `required_tier`, enforced by the auth plugin.
#[derive(Debug, Clone)]
pub struct Permission {
    pub name: String,
}
