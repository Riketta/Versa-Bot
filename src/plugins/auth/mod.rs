use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::kernel::{
    models::{EventKind, EventPayload, RequestContext},
    plugin_ports::{MiddlewarePluginPort, Next, PluginPort},
    services::KernelServices,
};

/// Guild storage namespace owned by this plugin (the plugin's slug).
const NAMESPACE: &str = "auth";
/// Guild storage key holding the serialized [`AuthConfig`].
const CONFIG_KEY: &str = "config";

/// Per-guild authorization policy, stored as a JSON document in the
/// plugin's guild storage namespace. The `!auth` management commands
/// (CommandRegistry step) write this document.
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
///   `allowed_roles`. Otherwise the event is stopped silently.
/// - An unconfigured guild is open by default (bootstrap: otherwise auth
///   would deny the very commands that configure it).
/// - A malformed config document fails closed - corruption never widens
///   access.
pub struct AuthPlugin;

impl Default for AuthPlugin {
    fn default() -> Self {
        Self
    }
}

impl PluginPort for AuthPlugin {
    fn name(&self) -> &'static str {
        "auth"
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

        let Some(raw) = storage.get(NAMESPACE, CONFIG_KEY).await.ok().flatten() else {
            return Next::Continue;
        };

        let Ok(config) = serde_json::from_value::<AuthConfig>(raw) else {
            tracing::warn!(namespace = NAMESPACE, "auth config is malformed - failing closed");
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
        // Deliberate rejection: `Stop`, so this plugin's `post` still runs
        // (audit hook) while downstream plugins never see the event.
        Next::Stop
    }
}

impl AuthPlugin {
    fn is_allowed(&self, config: &AuthConfig, event: &RequestContext) -> bool {
        if config.allowed_users.iter().any(|id| *id == event.origin.user_id.get().to_string()) {
            return true;
        }

        let author_roles = match &event.payload {
            EventPayload::Message(message) => &message.author_roles,
            EventPayload::Command(command) => &command.author_roles,
            _ => return false,
        };
        author_roles.iter().any(|role| config.allowed_roles.contains(role))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            }),
        }
    }

    fn message_event(user_id: u64, roles: &[&str]) -> RequestContext {
        RequestContext {
            kind: EventKind::MessageReceived,
            origin: origin(user_id),
            payload: EventPayload::Message(MessagePayload {
                content: "!ping".to_owned(),
                author_roles: roles.iter().map(|role| (*role).to_owned()).collect(),
            }),
        }
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
        let plugin = AuthPlugin;
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn allowed_user_passes() {
        let storage = configured_storage(&["3"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn unlisted_user_is_stopped_without_output() {
        let storage = configured_storage(&["999"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn allowed_role_passes_for_unlisted_user() {
        let storage = configured_storage(&[], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
        let mut event = message_event(3, &["42"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn role_mismatch_denies() {
        let storage = configured_storage(&[], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
        let mut event = message_event(3, &["7"]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn malformed_config_fails_closed() {
        let storage = InMemoryStorage::new();
        storage.seed(Platform::Discord, GuildId(1), NAMESPACE, CONFIG_KEY, json!("not an object"));
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
        let mut event = message_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn non_message_events_pass_through() {
        let storage = configured_storage(&["999"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
        let mut event = join_event(3);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn commands_are_gated_like_messages() {
        let storage = configured_storage(&["999"], &[]);
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
        let mut event = command_event(3, &[]);

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Stop));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn command_with_allowed_role_passes() {
        let storage = configured_storage(&[], &["42"]);
        let (services, output) = test_services(&storage);
        let plugin = AuthPlugin;
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
        let plugin = AuthPlugin;
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
}
