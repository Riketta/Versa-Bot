//! Plugin-facing contract for the command registry: plugins register command
//! descriptors (meaning) during `init()`; the kernel aggregates them without
//! interpreting them. Platform command registration (e.g. Discord slash
//! sync) consumes the registry from the adapter side.

use std::sync::Arc;

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

    fn descriptors(&self) -> Vec<CommandDescriptor>;
}

#[derive(Debug, Clone)]
pub struct CommandDescriptor {
    pub plugin_id: String,
    pub name: String,
    pub description: String,
    pub arguments: Vec<ArgDescriptor>,
    pub required_permission: Option<Permission>,
    /// Command is guild-scoped: adapters hide it from direct messages
    /// (Discord: `dm_permission: false`).
    pub guild_only: bool,
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

/// Placeholder until `AuthPlugin` grows per-command permission checks.
#[derive(Debug, Clone)]
pub struct Permission {
    pub name: String,
}
