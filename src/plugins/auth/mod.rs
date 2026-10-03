use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::kernel::{
    models::{Embed, EventKind, EventPayload, OutboundMessage, PluginError, RequestContext},
    plugin_ports::{
        AccessTier, ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort,
        MiddlewarePluginPort, Next, Permission, PluginPort,
    },
    services::KernelServices,
};

mod command;

pub use command::AuthCommandHandler;

/// Guild storage namespace owned by this plugin (the plugin's slug).
pub(crate) const NAMESPACE: &str = "auth";
/// Per-guild storage key holding the serialized [`AuthConfig`].
pub(crate) const CONFIG_KEY: &str = "config";

/// Discord's `ADMINISTRATOR` permission bit. The taxonomy carries author
/// permissions as opaque platform data; this bit value is Discord's, passed
/// through by the driving adapter, and nothing interprets the other bits.
const GUILD_ADMINISTRATOR_BIT: u64 = 0x8;

/// Per-guild tier policy, stored as a JSON document in the plugin's guild
/// storage namespace. The `/auth` management command (same plugin,
/// interactive half) writes this document. Schema v2 - tier assignments
/// instead of flat allow-lists; there is no legacy migration: a document
/// that does not carry the new fields deserializes to the open default.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Tier for members with no explicit assignment and no qualifying role.
    /// `User` keeps a fresh guild open by default - otherwise the gate would
    /// deny the very commands that configure it.
    pub default_tier: AccessTier,
    /// Explicit per-user tier assignments (platform user IDs as strings).
    /// A `Banned` assignment wins over every role grant.
    pub users: BTreeMap<String, AccessTier>,
    /// Per-role tier assignments (platform role IDs as strings): holding the
    /// role grants at least that tier.
    pub roles: BTreeMap<String, AccessTier>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { default_tier: AccessTier::User, users: BTreeMap::new(), roles: BTreeMap::new() }
    }
}

/// Per-guild bot access control - the first short-circuiting middleware.
///
/// Policy:
/// - Gates bot *invocation* (`MessageReceived` and `CommandInvoked`).
///   Passive events (member join/leave, presence) flow to their plugins
///   regardless - auth is about who may use the bot, not about what happens
///   in the guild. Direct messages pass too: no guild scope to protect.
/// - Every member has an effective [`AccessTier`] (see [`Self::effective_tier`]).
///   `Banned` members are dropped silently - messages and commands alike,
///   without any denial output: bans never announce themselves.
/// - Commands are tier-gated by their descriptor's `required_tier` (looked
///   up in the command registry); a member below the requirement gets an
///   ephemeral denial on transactional origins. Unknown commands pass -
///   the dispatcher ignores them anyway.
/// - Plain messages only need to not be banned: chatting with the bot is
///   the `Guest` floor.
/// - A malformed config document fails closed - corruption never widens
///   access. A storage failure fails closed the same way.
pub struct AuthPlugin {
    registry: Arc<dyn CommandRegistryPort>,
}

impl AuthPlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>) -> Self {
        Self { registry }
    }
}

impl PluginPort for AuthPlugin {
    fn name(&self) -> &'static str {
        "auth"
    }

    fn init(&self) -> Result<(), PluginError> {
        // The interactive half of this plugin. Discord additionally hides
        // the command behind Manage Server (`default_member_permissions`) -
        // defense in depth on top of the `Admin` tier check below.
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "auth".to_owned(),
                description: "Manage member tiers for bot access in this server".to_owned(),
                arguments: vec![
                    ArgDescriptor {
                        name: "action".to_owned(),
                        description: "show, set, clear, or default the tier policy".to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: Some(vec![
                            "show".to_owned(),
                            "set".to_owned(),
                            "clear".to_owned(),
                            "default".to_owned(),
                        ]),
                    },
                    ArgDescriptor {
                        name: "tier".to_owned(),
                        description: "Tier for `set`/`default`: banned, guest, user, moderator, \
                             admin"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::String,
                        choices: Some(vec![
                            "banned".to_owned(),
                            "guest".to_owned(),
                            "user".to_owned(),
                            "moderator".to_owned(),
                            "admin".to_owned(),
                        ]),
                    },
                    ArgDescriptor {
                        name: "user".to_owned(),
                        description: "User to assign a tier to (`set`/`clear`; one target per \
                             call)"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::User,
                        choices: None,
                    },
                    ArgDescriptor {
                        name: "role".to_owned(),
                        description: "Role to grant a tier (`set`/`clear`; one target per call)"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::Role,
                        choices: None,
                    },
                ],
                required_permission: Some(Permission { name: "manage_guild".to_owned() }),
                required_tier: Some(AccessTier::Admin),
                guild_only: true,
            },
            Arc::new(AuthCommandHandler::default()),
        );
        Ok(())
    }
}

#[async_trait]
impl MiddlewarePluginPort for AuthPlugin {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        if !matches!(event.kind, EventKind::MessageReceived | EventKind::CommandInvoked) {
            return Next::Continue;
        }

        // Direct messages: no guild scope to protect.
        let Some(storage) = &services.guild_storage else {
            return Next::Continue;
        };

        let loaded = match storage.get(NAMESPACE, CONFIG_KEY).await {
            // Unconfigured = the open default policy (default tier `User`).
            Ok(None) => None,
            Ok(Some(raw)) => Some(serde_json::from_value::<AuthConfig>(raw)),
            // Unreadable = fail closed, same as a malformed policy: a storage
            // failure must never widen access, for any guild.
            Err(err) => {
                tracing::error!(namespace = NAMESPACE, %err, "auth config unreadable - failing closed");
                self.answer_denial(services, event, Self::policy_unavailable_embed(event)).await;
                return Next::Stop;
            }
        };

        let config = match loaded {
            Some(Ok(config)) => config,
            Some(Err(_)) => {
                tracing::warn!(namespace = NAMESPACE, "auth config is malformed - failing closed");
                self.answer_denial(services, event, Self::policy_unavailable_embed(event)).await;
                return Next::Stop;
            }
            None => AuthConfig::default(),
        };

        let tier = Self::effective_tier(&config, event);

        if tier == AccessTier::Banned {
            tracing::info!(user = %event.origin.user_id, guild_id = ?event.origin.guild_id, "event denied by auth: banned (ignored silently)");
            return Next::Stop;
        }

        if let EventPayload::Command(command) = &event.payload {
            match self.registry.descriptor(&command.name) {
                None => {} // Unknown command: the dispatcher ignores it anyway.
                Some(descriptor) => {
                    let required = descriptor.required_tier.unwrap_or(AccessTier::Guest);
                    if tier < required {
                        tracing::info!(
                            user = %event.origin.user_id,
                            guild_id = ?event.origin.guild_id,
                            command = %command.name,
                            required = %required,
                            effective = %tier,
                            "command denied by auth tier"
                        );
                        let denial = Self::denial_embed(&command.name, required, tier);
                        self.answer_denial(services, event, denial).await;
                        return Next::Stop;
                    }
                }
            }
        }

        Next::Continue
    }
}

impl AuthPlugin {
    /// Effective tier of the event's author:
    /// - Discord guild administrators are `Admin` by construction - the
    ///   clamp IS the "cannot be removed" guarantee; there is no stored
    ///   entry to lose, and no code path anywhere needs a special case.
    /// - An explicit `Banned` user assignment wins over every role grant.
    /// - Otherwise: the user's assignment (or the guild default) lifted by
    ///   the best qualifying role - an explicit assignment below the
    ///   default (e.g. `Guest`) stays below it until a role lifts it.
    fn effective_tier(config: &AuthConfig, event: &RequestContext) -> AccessTier {
        let (author_roles, author_permissions) = match &event.payload {
            EventPayload::Message(message) => (&message.author_roles, message.author_permissions),
            EventPayload::Command(command) => (&command.author_roles, command.author_permissions),
            _ => return config.default_tier,
        };

        if author_permissions & GUILD_ADMINISTRATOR_BIT != 0 {
            return AccessTier::Admin;
        }

        let user_id = event.origin.user_id.get().to_string();
        if config.users.get(&user_id) == Some(&AccessTier::Banned) {
            return AccessTier::Banned;
        }

        let base = config.users.get(&user_id).copied().unwrap_or(config.default_tier);
        let role_tier =
            author_roles.iter().filter_map(|role| config.roles.get(role)).copied().max();
        base.max(role_tier.unwrap_or(base))
    }

    /// Delivers a denial to transactional events only (slash commands owe
    /// the invoker an answer; make it ephemeral so it never spams the
    /// channel or exposes the policy). Plain messages are not interactions -
    /// ephemeral is impossible there and a public reply would be a spam
    /// vector - so they stay silent (bans are silent everywhere).
    async fn answer_denial(&self, services: &KernelServices, event: &RequestContext, embed: Embed) {
        if event.origin.reply_token.is_none() {
            return;
        }
        let denial = OutboundMessage::embed(embed).ephemeral();
        if let Err(err) = services.chat_output.send(denial).await {
            tracing::warn!(%err, "failed to deliver auth denial");
        }
    }

    /// Denial when the policy itself is unreadable: no tiers to quote,
    /// point at the broken configuration instead.
    fn policy_unavailable_embed(event: &RequestContext) -> Embed {
        let what = match &event.payload {
            EventPayload::Command(command) => {
                format!("Command `/{}` could not be authorized.", command.name)
            }
            _ => "The action could not be authorized.".to_owned(),
        };
        Embed {
            title: "⛔ Not authorized".to_owned(),
            description: format!(
                "{what}\nThis guild's access policy is unreadable (malformed), so the request was denied.\nAsk a guild admin to fix the bot configuration."
            ),
        }
    }

    fn denial_embed(command_name: &str, required: AccessTier, effective: AccessTier) -> Embed {
        Embed {
            title: "⛔ Not authorized".to_owned(),
            description: format!(
                "Command `/{command_name}` requires the {required} tier (you have {effective}).\nAsk a guild admin if you think this is a mistake."
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryCommandRegistry;
    use crate::kernel::{
        models::{
            ChannelId, CommandPayload, GuildId, MemberPayload, MessageId, MessagePayload, Origin,
            Platform, UserId,
        },
        plugin_ports::{CommandArgs, CommandHandler},
        spi_ports::{GUILD_SETTINGS, StoragePort},
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
    use serde_json::json;
    use std::sync::Arc;

    /// Registry fixture carrying the descriptors the gate looks up. The
    /// `/auth` handler tests live in `command.rs`; handlers here are no-ops.
    fn test_plugin(commands: &[(&str, AccessTier)]) -> AuthPlugin {
        struct NoopHandler;

        #[async_trait]
        impl CommandHandler for NoopHandler {
            async fn invoke(
                &self,
                _event: &RequestContext,
                _args: &CommandArgs,
                _services: &KernelServices,
            ) -> anyhow::Result<()> {
                Ok(())
            }
        }

        let registry = Arc::new(InMemoryCommandRegistry::new());
        for (name, tier) in commands {
            registry.register(
                CommandDescriptor {
                    plugin_id: "test".to_owned(),
                    name: (*name).to_owned(),
                    description: "test command".to_owned(),
                    arguments: Vec::new(),
                    required_permission: None,
                    required_tier: Some(*tier),
                    guild_only: true,
                },
                Arc::new(NoopHandler),
            );
        }
        AuthPlugin::new(registry)
    }

    fn origin(user_id: u64) -> Origin {
        Origin {
            platform: Platform::Discord,
            guild_id: Some(GuildId(1)),
            channel_id: ChannelId(2),
            user_id: UserId(user_id),
            message_id: Some(MessageId(4)),
            reply_token: None,
        }
    }

    fn command_event(user_id: u64, name: &str, roles: &[&str]) -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: origin(user_id),
            payload: EventPayload::Command(CommandPayload {
                name: name.to_owned(),
                args: Vec::new(),
                author_roles: roles.iter().map(|role| (*role).to_owned()).collect(),
                author_permissions: 0,
            }),
        }
    }

    fn message_event(user_id: u64, roles: &[&str]) -> RequestContext {
        RequestContext {
            kind: EventKind::MessageReceived,
            origin: origin(user_id),
            payload: EventPayload::Message(MessagePayload {
                content: "!ping".to_owned(),
                attachments: Vec::new(),
                author_name: None,
                author_roles: roles.iter().map(|role| (*role).to_owned()).collect(),
                author_permissions: 0,
                reply_to: None,
                mentions_bot: false,
            }),
        }
    }

    /// Command event carrying explicit platform permission bits (opaque
    /// pass-through data; `0x8` marks a Discord guild administrator).
    fn command_event_with_permissions(user_id: u64, author_permissions: u64) -> RequestContext {
        let mut event = command_event(user_id, "auth", &[]);
        if let EventPayload::Command(payload) = &mut event.payload {
            payload.author_permissions = author_permissions;
        }
        event
    }

    fn join_event(user_id: u64) -> RequestContext {
        RequestContext {
            kind: EventKind::MemberJoined,
            origin: origin(user_id),
            payload: EventPayload::Member(MemberPayload { username: Some("someone".to_owned()) }),
        }
    }

    fn test_services(storage: &InMemoryStorage) -> (KernelServices, Arc<RecordingChatOutput>) {
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        };
        (services, output)
    }

    fn configured_storage(
        default_tier: AccessTier,
        users: &[(&str, AccessTier)],
        roles: &[(&str, AccessTier)],
    ) -> Arc<InMemoryStorage> {
        let storage = InMemoryStorage::new();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            json!({
                "default_tier": default_tier.as_str(),
                "users": users.iter().map(|(id, tier)| (id.to_string(), tier.as_str()))
                    .collect::<BTreeMap<_, _>>(),
                "roles": roles.iter().map(|(id, tier)| (id.to_string(), tier.as_str()))
                    .collect::<BTreeMap<_, _>>(),
            }),
        );
        Arc::new(storage)
    }

    #[tokio::test]
    async fn unconfigured_guild_allows_default_tier() {
        let storage = InMemoryStorage::new();
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut message = message_event(3, &[]);
        let mut command = command_event(3, "ping", &[]);

        assert!(matches!(plugin.pre(&mut message, &services).await, Next::Continue));
        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn user_tier_cannot_run_moderator_commands() {
        let storage = configured_storage(AccessTier::User, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);
        let mut event = command_event(3, "assign_tracker", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        assert!(sent.first().expect("denial expected").ephemeral);
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("⛔ Not authorized"));
        assert!(text.contains("Command `/assign_tracker` requires the Moderator tier"));
        assert!(text.contains("you have User"));
    }

    #[tokio::test]
    async fn moderator_assignment_runs_moderator_commands() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Moderator)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);
        let mut event = command_event(3, "assign_tracker", &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn role_mapping_lifts_tier() {
        let storage = configured_storage(AccessTier::User, &[], &[("42", AccessTier::Moderator)]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("assign_tracker", AccessTier::Moderator)]);
        let mut event = command_event(3, "assign_tracker", &["42"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// Guests may chat (plain messages pass) but commands above Guest tier
    /// are denied with the tier named.
    #[tokio::test]
    async fn guest_default_allows_chat_but_denies_commands() {
        let storage = configured_storage(AccessTier::Guest, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut message = message_event(3, &[]);
        let mut command = command_event(3, "ping", &[]);
        command.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut message, &services).await, Next::Continue));
        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Stop));
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("requires the User tier"));
        assert!(text.contains("you have Guest"));
    }

    /// An explicit assignment below the default survives: `max()` must not
    /// let the guild default lift a deliberate demotion.
    #[tokio::test]
    async fn explicit_guest_assignment_beats_open_default() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Guest)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = command_event(3, "ping", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("you have Guest"));
    }

    /// Banned members are dropped without any output - bans never announce
    /// themselves, on messages and on commands alike (even transactional).
    #[tokio::test]
    async fn banned_members_are_dropped_silently() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut message = message_event(3, &[]);
        let mut command = command_event(3, "ping", &[]);
        command.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut message, &services).await, Next::Stop));
        assert!(matches!(plugin.pre(&mut command, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// An explicit ban wins over role grants: a banned user holding a
    /// moderator role stays banned.
    #[tokio::test]
    async fn ban_beats_role_grants() {
        let storage = configured_storage(
            AccessTier::User,
            &[("3", AccessTier::Banned)],
            &[("42", AccessTier::Moderator)],
        );
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = message_event(3, &["42"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// Discord guild administrators are Admin by construction: even a
    /// policy that never mentions them lets them run `/auth`.
    #[tokio::test]
    async fn guild_admin_is_admin_by_construction() {
        let storage = configured_storage(AccessTier::User, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("auth", AccessTier::Admin)]);
        let mut event = command_event_with_permissions(3, 0x8);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// The clamp beats an explicit ban, too - removing a guild admin's
    /// access is impossible by construction, in every code path.
    #[tokio::test]
    async fn guild_admin_clamp_beats_explicit_ban() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("auth", AccessTier::Admin)]);
        let mut event = command_event_with_permissions(3, 0x8);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// Non-admins cannot run admin-tier commands; the denial names both
    /// sides of the comparison.
    #[tokio::test]
    async fn non_admin_cannot_run_auth() {
        let storage = configured_storage(AccessTier::User, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("auth", AccessTier::Admin)]);
        let mut event = command_event(3, "auth", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("Command `/auth` requires the Admin tier"));
        assert!(text.contains("you have User"));
    }

    /// Commands without a registered descriptor pass: the dispatcher has no
    /// handler for them, so gating them would change nothing.
    #[tokio::test]
    async fn unknown_command_passes() {
        let storage = configured_storage(AccessTier::Guest, &[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[]);
        let mut event = command_event(3, "nonexistent", &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// A legacy pre-tier policy document has no migration: it deserializes
    /// to the open default (unknown fields ignored, defaults filled in).
    #[tokio::test]
    async fn legacy_document_behaves_as_open_default() {
        let storage = InMemoryStorage::new();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            json!({ "allowed_users": ["999"], "allowed_roles": [] }),
        );
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn malformed_config_fails_closed() {
        let storage = InMemoryStorage::new();
        storage.seed(Platform::Discord, GuildId(1), NAMESPACE, CONFIG_KEY, json!("not an object"));
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// A broken policy cannot quote tiers, but a slash command still owes
    /// the invoker an ephemeral answer pointing at the configuration.
    #[tokio::test]
    async fn malformed_config_denies_commands_with_policy_unavailable_embed() {
        let storage = InMemoryStorage::new();
        storage.seed(Platform::Discord, GuildId(1), NAMESPACE, CONFIG_KEY, json!("not an object"));
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[("ping", AccessTier::User)]);
        let mut event = command_event(3, "ping", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        assert!(sent.first().expect("denial expected").ephemeral);
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("⛔ Not authorized"));
        assert!(text.contains("Command `/ping` could not be authorized."));
        assert!(text.contains("unreadable (malformed)"));
        assert!(!text.contains("tier"), "no policy, no reasons");
    }

    /// Passive events are not gated - even a banned member's join flows to
    /// the plugins that need it.
    #[tokio::test]
    async fn non_message_events_pass_through() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin(&[]);
        let mut event = join_event(3);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn direct_messages_pass_through() {
        let storage = configured_storage(AccessTier::User, &[("3", AccessTier::Banned)], &[]);
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: None,
        };
        let plugin = test_plugin(&[]);
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
        drop(storage);
    }

    /// Guards the reserved namespace constant the guild settings plugin will
    /// rely on; auth must never collide with it.
    #[test]
    fn auth_namespace_is_not_the_reserved_guild_namespace() {
        assert_ne!(NAMESPACE, GUILD_SETTINGS);
    }

    /// A storage failure must fail closed (deny, like a malformed policy),
    /// never silently fail open - and transactional events owe the invoker
    /// the ephemeral "policy unavailable" notice.
    #[tokio::test]
    async fn unreadable_policy_fails_closed() {
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: Some(
                crate::test_support::FailingStorage.guild_scoped(Platform::Discord, GuildId(1)),
            ),
        };
        let plugin = test_plugin(&[]);
        let mut event = command_event(3, "ping", &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        let messages = output.messages();
        assert_eq!(messages.len(), 1, "denial embed expected");
        assert!(messages.first().is_some_and(|m| m.contains("unreadable")));
        assert!(output.sent().first().is_some_and(|m| m.ephemeral));
    }
}
