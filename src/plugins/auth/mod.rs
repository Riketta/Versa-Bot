use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::kernel::{
    models::{Embed, EventKind, EventPayload, OutboundMessage, PluginError, RequestContext},
    plugin_ports::{
        ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort, MiddlewarePluginPort, Next,
        Permission, PluginPort,
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

/// Per-guild authorization policy, stored as a JSON document in the
/// plugin's guild storage namespace. The `/auth` management command (same
/// plugin, interactive half) writes this document.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub allowed_users: Vec<String>,
    #[serde(default)]
    pub allowed_roles: Vec<String>,
}

/// Per-guild bot access control - the first short-circuiting middleware.
///
/// Policy:
/// - Gates bot *invocation* only (`MessageReceived` and `CommandInvoked`).
///   Passive events (member join/leave, presence) flow to their plugins
///   regardless - auth is about who may command the bot, not about what
///   happens in the guild.
/// - A user passes when listed in `allowed_users`, or when any of their
///   roles (provided by the driving adapter, best effort) appears in
///   `allowed_roles`. Otherwise the event is stopped; commands additionally
///   get an ephemeral denial answer (see `answer_denial`).
/// - A policy that exists but lists nobody falls back to Discord guild
///   administrators (master-admin fallback): an empty policy must not lock
///   out the admins who would reconfigure it (see `is_allowed`).
/// - An unconfigured guild is open by default (bootstrap: otherwise auth
///   would deny the very commands that configure it).
/// - A malformed config document fails closed - corruption never widens
///   access.
///
/// The allow-list also gates `/auth` itself: whoever manages the policy
/// must stay listed (or hold an allowed role).
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
        // the command behind Manage Server (`default_member_permissions`);
        // the policy gate below remains the kernel-side check.
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "auth".to_owned(),
                description: "Manage who can use the bot in this guild".to_owned(),
                arguments: vec![
                    ArgDescriptor {
                        name: "action".to_owned(),
                        description: "What to do".to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: Some(vec![
                            "allow".to_owned(),
                            "deny".to_owned(),
                            "show".to_owned(),
                        ]),
                    },
                    ArgDescriptor {
                        name: "user".to_owned(),
                        description: "User to allow or deny".to_owned(),
                        required: false,
                        kind: ArgKind::User,
                        choices: None,
                    },
                    ArgDescriptor {
                        name: "role".to_owned(),
                        description: "Role to allow or deny".to_owned(),
                        required: false,
                        kind: ArgKind::Role,
                        choices: None,
                    },
                ],
                required_permission: Some(Permission { name: "manage_guild".to_owned() }),
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

        let raw = match storage.get(NAMESPACE, CONFIG_KEY).await {
            Ok(Some(raw)) => raw,
            // Unconfigured = open by default (documented policy).
            Ok(None) => return Next::Continue,
            // Unreadable = fail closed, same as a malformed policy: a storage
            // failure must never widen access, for any guild.
            Err(err) => {
                tracing::error!(namespace = NAMESPACE, %err, "auth config unreadable - failing closed");
                self.answer_denial(services, event, self.policy_unavailable_embed(event)).await;
                return Next::Stop;
            }
        };

        let Ok(config) = serde_json::from_value::<AuthConfig>(raw) else {
            tracing::warn!(namespace = NAMESPACE, "auth config is malformed - failing closed");
            self.answer_denial(services, event, self.policy_unavailable_embed(event)).await;
            return Next::Stop;
        };

        if self.is_allowed(&config, event) {
            return Next::Continue;
        }

        tracing::info!(
            user = %event.origin.user_id,
            guild_id = ?event.origin.guild_id,
            "event denied by auth"
        );
        self.answer_denial(services, event, self.denial_embed(&config, event)).await;

        // Deliberate rejection: `Stop`, so this plugin's `post` still runs
        // (audit hook) while downstream plugins never see the event.
        Next::Stop
    }
}

impl AuthPlugin {
    /// Access rule: listed by user or role; a policy that exists but lists
    /// nobody additionally admits Discord guild administrators - the empty
    /// policy must not lock the admins who would reconfigure it out.
    fn is_allowed(&self, config: &AuthConfig, event: &RequestContext) -> bool {
        if self.user_listed(config, event) || self.role_listed(config, event) {
            return true;
        }
        config.allowed_users.is_empty()
            && config.allowed_roles.is_empty()
            && Self::author_is_guild_admin(event)
    }

    fn author_is_guild_admin(event: &RequestContext) -> bool {
        let author_permissions = match &event.payload {
            EventPayload::Message(message) => message.author_permissions,
            EventPayload::Command(command) => command.author_permissions,
            _ => return false,
        };
        (author_permissions & GUILD_ADMINISTRATOR_BIT) != 0
    }

    /// Delivers a denial to transactional events only (slash commands owe
    /// the invoker an answer; make it ephemeral so it never spams the
    /// channel or exposes the policy). Plain messages are not interactions -
    /// ephemeral is impossible there and a public reply would be a spam
    /// vector - so they stay silent.
    async fn answer_denial(&self, services: &KernelServices, event: &RequestContext, embed: Embed) {
        if event.origin.reply_token.is_none() {
            return;
        }
        let denial = OutboundMessage::embed(embed).ephemeral();
        if let Err(err) = services.chat_output.send(denial).await {
            tracing::warn!(%err, "failed to deliver auth denial");
        }
    }

    /// Denial when the policy itself is unreadable: no reasons to quote,
    /// point at the broken configuration instead.
    fn policy_unavailable_embed(&self, event: &RequestContext) -> Embed {
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

    fn user_listed(&self, config: &AuthConfig, event: &RequestContext) -> bool {
        config.allowed_users.iter().any(|id| *id == event.origin.user_id.get().to_string())
    }

    fn role_listed(&self, config: &AuthConfig, event: &RequestContext) -> bool {
        let author_roles = match &event.payload {
            EventPayload::Message(message) => &message.author_roles,
            EventPayload::Command(command) => &command.author_roles,
            _ => return false,
        };
        author_roles.iter().any(|role| config.allowed_roles.contains(role))
    }

    /// Why the event was denied, in terms of the policy groups: which
    /// permission group rejected the user, or that a role is missing.
    fn denial_embed(&self, config: &AuthConfig, event: &RequestContext) -> Embed {
        let mut reasons: Vec<String> = Vec::new();
        if !config.allowed_users.is_empty() && !self.user_listed(config, event) {
            reasons.push("Not authorized for the `users` permissions group.".to_owned());
        }
        if !config.allowed_roles.is_empty() && !self.role_listed(config, event) {
            reasons.push(
                "Missing role: none of your roles are in the `roles` permissions group.".to_owned(),
            );
        }
        if reasons.is_empty() {
            // Empty policy: non-admins were just denied; say who is still
            // allowed.
            reasons.push(
                "While the access policy is empty, only Discord guild administrators are allowed."
                    .to_owned(),
            );
        }

        let what = match &event.payload {
            EventPayload::Command(command) => {
                format!("Command `/{}` is not allowed here.", command.name)
            }
            _ => "This action is not allowed here.".to_owned(),
        };

        Embed {
            title: "⛔ Not authorized".to_owned(),
            description: format!("{what}\n{}", reasons.join("\n")),
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
        spi_ports::{GUILD_SETTINGS, StoragePort},
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
    use serde_json::json;
    use std::sync::Arc;

    /// The gate under test; init/command tests live in `command.rs` with
    /// their own registry, so a fresh empty registry is enough here.
    fn test_plugin() -> AuthPlugin {
        AuthPlugin::new(Arc::new(InMemoryCommandRegistry::new()))
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

    fn command_event(user_id: u64, roles: &[&str]) -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: origin(user_id),
            payload: EventPayload::Command(CommandPayload {
                name: "ping".to_owned(),
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
        let mut event = command_event(user_id, &[]);
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

    fn configured_storage(allowed_users: &[&str], allowed_roles: &[&str]) -> Arc<InMemoryStorage> {
        let storage = InMemoryStorage::new();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            json!({
                "allowed_users": allowed_users,
                "allowed_roles": allowed_roles,
            }),
        );
        Arc::new(storage)
    }

    #[tokio::test]
    async fn unconfigured_guild_is_open() {
        let storage = InMemoryStorage::new();
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn allowed_user_passes() {
        let storage = configured_storage(&["3"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn unlisted_user_is_stopped_without_output() {
        let storage = configured_storage(&["999"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// A denied slash command (transactional origin) answers the invoker
    /// with an ephemeral embed naming the rejected permission group.
    #[tokio::test]
    async fn denied_command_answers_ephemerally_with_user_group_reason() {
        let storage = configured_storage(&["999"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event(3, &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        let denial = sent.first().expect("denial expected");
        assert!(denial.ephemeral, "denial must be visible to the invoker only");
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("⛔ Not authorized"));
        assert!(text.contains("Command `/ping` is not allowed here."));
        assert!(text.contains("Not authorized for the `users` permissions group."));
        assert!(!text.contains("Missing role"));
    }

    #[tokio::test]
    async fn denied_command_with_roles_config_reports_missing_role() {
        let storage = configured_storage(&[], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event(3, &["7"]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(
            text.contains("Missing role: none of your roles are in the `roles` permissions group.")
        );
        assert!(!text.contains("Not authorized for the"));
    }

    #[tokio::test]
    async fn denied_command_lists_every_rejected_group() {
        let storage = configured_storage(&["999"], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event(3, &["7"]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("Not authorized for the `users` permissions group."));
        assert!(text.contains("Missing role"));
    }

    /// An empty policy denies non-admins, and the denial explains the
    /// administrator fallback.
    #[tokio::test]
    async fn denied_command_with_empty_policy_denies_non_admin() {
        let storage = configured_storage(&[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        // author_permissions: 0 = unknown, in particular not an administrator.
        let mut event = command_event(3, &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("While the access policy is empty"));
        assert!(text.contains("only Discord guild administrators are allowed"));
    }

    /// Master-admin fallback: a policy that exists but lists nobody keeps
    /// Discord guild administrators allowed (bit 0x8, opaque pass-through).
    #[tokio::test]
    async fn empty_policy_allows_guild_admin() {
        let storage = configured_storage(&[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event_with_permissions(3, 0x8);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    /// The inverse master-admin boundary: an administrator is NOT a master
    /// admin of a configured (non-empty) policy that does not list them.
    /// Only an EMPTY policy admits admins - never a filled one.
    #[tokio::test]
    async fn guild_admin_is_denied_by_a_non_empty_policy_that_omits_them() {
        let storage = configured_storage(&["1"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event_with_permissions(3, 0x8);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("Not authorized for the `users` permissions group."));
        assert!(
            !text.contains("While the access policy is empty"),
            "the policy is not empty - the admin fallback must not apply"
        );
    }

    /// The admin fallback reads the administrator bit off BOTH payload kinds:
    /// a plain message from a guild admin passes an empty policy too.
    #[tokio::test]
    async fn empty_policy_admin_fallback_applies_to_plain_messages() {
        let storage = configured_storage(&[], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = message_event(3, &[]);
        if let EventPayload::Message(payload) = &mut event.payload {
            payload.author_permissions = 0x8;
        }

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn allowed_role_passes_for_unlisted_user() {
        let storage = configured_storage(&[], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = message_event(3, &["42"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn role_mismatch_denies() {
        let storage = configured_storage(&[], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = message_event(3, &["7"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn malformed_config_fails_closed() {
        let storage = InMemoryStorage::new();
        storage.seed(Platform::Discord, GuildId(1), NAMESPACE, CONFIG_KEY, json!("not an object"));
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// A broken policy cannot quote reasons, but a slash command still owes
    /// the invoker an ephemeral answer pointing at the configuration.
    #[tokio::test]
    async fn malformed_config_denies_commands_with_policy_unavailable_embed() {
        let storage = InMemoryStorage::new();
        storage.seed(Platform::Discord, GuildId(1), NAMESPACE, CONFIG_KEY, json!("not an object"));
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event(3, &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        let denial = sent.first().expect("denial expected");
        assert!(denial.ephemeral, "denial must be visible to the invoker only");
        let text = output.messages().into_iter().next().expect("denial expected");
        assert!(text.contains("⛔ Not authorized"));
        assert!(text.contains("Command `/ping` could not be authorized."));
        assert!(text.contains("unreadable (malformed)"));
        assert!(!text.contains("permissions group"), "no policy, no reasons");
    }

    #[tokio::test]
    async fn non_message_events_pass_through() {
        let storage = configured_storage(&["999"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = join_event(3);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn commands_are_gated_like_messages() {
        let storage = configured_storage(&["999"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn command_with_allowed_role_passes() {
        let storage = configured_storage(&[], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = test_plugin();
        let mut event = command_event(3, &["42"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn direct_messages_pass_through() {
        let _storage = configured_storage(&["999"], &[]);
        let output = RecordingChatOutput::new();
        let chat_output: Arc<dyn crate::kernel::spi_ports::ChatOutputPort> =
            Arc::clone(&output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>;
        let services = KernelServices {
            chat_output,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: None,
        };
        let plugin = test_plugin();
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
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
        let plugin = test_plugin();
        let mut event = command_event(3, &[]);
        event.origin.reply_token = Some("token".to_owned());

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        let messages = output.messages();
        assert_eq!(messages.len(), 1, "denial embed expected");
        assert!(messages.first().is_some_and(|m| m.contains("unreadable")));
        assert!(output.sent().first().is_some_and(|m| m.ephemeral));
    }
}
