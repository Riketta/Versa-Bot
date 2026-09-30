use std::sync::Arc;

use crate::kernel::{
    models::PluginError,
    plugin_ports::{
        ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort, Permission, PluginPort,
    },
};

use super::commands::{
    AssignLlmHandler, AssignServiceChannelHandler, ClearServiceChannelHandler, UnassignLlmHandler,
};

/// LLM chat plugin. This is the lifecycle/identity half (`PluginPort`): it
/// owns the per-channel configuration and service-channel commands so guild
/// admins can wire channels up. The conversation engine - capture rules,
/// completion, compaction, random replies - joins the middleware pipeline in
/// the following steps and rides the same plugin object.
pub struct LlmPlugin {
    registry: Arc<dyn CommandRegistryPort>,
}

impl LlmPlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>) -> Self {
        Self { registry }
    }

    fn descriptor(
        &self,
        name: &str,
        description: &str,
        arguments: Vec<ArgDescriptor>,
    ) -> CommandDescriptor {
        CommandDescriptor {
            plugin_id: self.name().to_owned(),
            name: name.to_owned(),
            description: description.to_owned(),
            arguments,
            // Platform-interpreted: the Discord adapter publishes this as
            // `default_member_permissions` (Manage Server).
            required_permission: Some(Permission { name: "manage_guild".to_owned() }),
            guild_only: true,
        }
    }
}

impl PluginPort for LlmPlugin {
    fn name(&self) -> &'static str {
        "llm"
    }

    fn init(&self) -> Result<(), PluginError> {
        self.registry.register(
            self.descriptor(
                "llm_assign",
                "Assign the chat bot to this channel",
                vec![ArgDescriptor {
                    name: "model".to_owned(),
                    description: "Provider/model ref, e.g. `local/gemma`".to_owned(),
                    required: true,
                    kind: ArgKind::String,
                    choices: None,
                }],
            ),
            Arc::new(AssignLlmHandler),
        );
        self.registry.register(
            self.descriptor("llm_unassign", "Remove the chat bot from this channel", Vec::new()),
            Arc::new(UnassignLlmHandler),
        );
        self.registry.register(
            self.descriptor(
                "llm_admin",
                "Report LLM errors and service notices in this channel",
                Vec::new(),
            ),
            Arc::new(AssignServiceChannelHandler),
        );
        self.registry.register(
            self.descriptor("llm_admin_clear", "Stop reporting LLM service notices", Vec::new()),
            Arc::new(ClearServiceChannelHandler),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryCommandRegistry;
    use crate::kernel::{
        models::{
            ChannelId as ChannelIdModel, CommandPayload, EventKind, EventPayload, GuildId,
            MessageId, Origin, Platform, RequestContext, UserId,
        },
        plugin_ports::{CommandArgs, CommandHandler},
        services::KernelServices,
        spi_ports::{ChatOutputPort, GUILD_SETTINGS, StoragePort},
    };
    use crate::plugins::llm::model::{NAMESPACE, SERVICE_CHANNEL_KEY, channel_config_key};
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};

    fn command_event(guild: Option<u64>) -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: Origin {
                platform: Platform::Discord,
                guild_id: guild.map(GuildId),
                channel_id: ChannelIdModel(if guild.is_some() { 2 } else { 5 }),
                user_id: UserId(3),
                message_id: Some(MessageId(4)),
                reply_token: None,
            },
            payload: EventPayload::Command(CommandPayload {
                name: "llm".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        }
    }

    struct Fixture {
        registry: Arc<InMemoryCommandRegistry>,
        storage: Arc<InMemoryStorage>,
        services: KernelServices,
        output: Arc<RecordingChatOutput>,
    }

    fn fixture() -> (LlmPlugin, Fixture) {
        let registry = Arc::new(InMemoryCommandRegistry::new());
        let storage = Arc::new(InMemoryStorage::new());
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        };
        let plugin = LlmPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>);
        (plugin, Fixture { registry, storage, services, output })
    }

    fn dm_services(output: &Arc<RecordingChatOutput>) -> KernelServices {
        KernelServices {
            chat_output: Arc::clone(output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(output)).boxed(),
            guild_storage: None,
        }
    }

    #[test]
    fn init_registers_llm_admin_commands() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        let mut names: Vec<String> =
            fixture.registry.descriptors().into_iter().map(|d| d.name).collect();
        names.sort();
        assert_eq!(names, ["llm_admin", "llm_admin_clear", "llm_assign", "llm_unassign"]);
        for descriptor in fixture.registry.descriptors() {
            assert_eq!(descriptor.plugin_id, "llm");
            assert!(descriptor.guild_only, "every llm command is guild-only");
            assert_eq!(
                descriptor.required_permission.as_ref().map(|p| p.name.as_str()),
                Some("manage_guild")
            );
        }
    }

    #[tokio::test]
    async fn assign_stores_channel_config_and_replies() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        AssignLlmHandler
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![("model".to_owned(), "local/gemma".to_owned())]),
                &fixture.services,
            )
            .await
            .expect("assign expected to succeed");

        let raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_config_key(2))
            .await
            .expect("read expected to succeed");
        assert_eq!(
            raw.expect("config expected").get("model").and_then(|v| v.as_str()),
            Some("local/gemma")
        );
        assert!(fixture.output.messages().iter().any(|m| m.contains("assigned")));
    }

    #[tokio::test]
    async fn assign_replaces_previous_model() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        let args = |model: &str| CommandArgs(vec![("model".to_owned(), model.to_owned())]);

        AssignLlmHandler
            .invoke(&command_event(Some(1)), &args("local/gemma"), &fixture.services)
            .await
            .expect("first assign expected to succeed");
        AssignLlmHandler
            .invoke(&command_event(Some(1)), &args("zai/glm-5.3-flash"), &fixture.services)
            .await
            .expect("second assign expected to succeed");

        let storage = fixture.services.guild_storage.as_ref().expect("guild storage expected");
        assert_eq!(storage.list_keys(NAMESPACE).await.expect("keys readable"), ["channel:2"]);
        let raw = storage
            .get(NAMESPACE, &channel_config_key(2))
            .await
            .expect("read expected to succeed")
            .expect("config expected");
        assert_eq!(raw.get("model").and_then(|v| v.as_str()), Some("zai/glm-5.3-flash"));
    }

    #[tokio::test]
    async fn assign_without_model_replies_usage_and_writes_nothing() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        AssignLlmHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("assign expected to succeed");

        assert!(fixture.output.messages().iter().any(|m| m.contains("Usage")));
        let storage = fixture.services.guild_storage.as_ref().expect("guild storage expected");
        assert_eq!(
            storage.list_keys(NAMESPACE).await.expect("keys readable"),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn unassign_clears_config_and_is_idempotent() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        let storage = Arc::clone(&fixture.storage);

        AssignLlmHandler
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![("model".to_owned(), "local/gemma".to_owned())]),
                &fixture.services,
            )
            .await
            .expect("assign expected to succeed");
        UnassignLlmHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("unassign expected to succeed");
        assert_eq!(
            storage
                .guild_scoped(Platform::Discord, GuildId(1))
                .list_keys(NAMESPACE)
                .await
                .expect("keys readable"),
            Vec::<String>::new()
        );
        UnassignLlmHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("second unassign expected to succeed (idempotent)");
    }

    #[tokio::test]
    async fn admin_handler_stores_service_channel_and_clear_removes_it() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        AssignServiceChannelHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("service assign expected to succeed");
        assert_eq!(
            fixture
                .services
                .guild_storage
                .as_ref()
                .expect("guild storage expected")
                .get(NAMESPACE, SERVICE_CHANNEL_KEY)
                .await
                .expect("read expected to succeed"),
            Some(serde_json::json!("2"))
        );

        ClearServiceChannelHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("service clear expected to succeed");
        assert_eq!(
            fixture
                .services
                .guild_storage
                .as_ref()
                .expect("guild storage expected")
                .get(NAMESPACE, SERVICE_CHANNEL_KEY)
                .await
                .expect("read expected to succeed"),
            None
        );
    }

    #[tokio::test]
    async fn commands_in_direct_messages_reply_guild_only() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        let services = dm_services(&fixture.output);

        AssignLlmHandler
            .invoke(
                &command_event(None),
                &CommandArgs(vec![("model".to_owned(), "local/gemma".to_owned())]),
                &services,
            )
            .await
            .expect("dm assign expected to be handled");
        AssignServiceChannelHandler
            .invoke(&command_event(None), &CommandArgs::default(), &services)
            .await
            .expect("dm admin assign expected to be handled");

        let replies = fixture.output.messages();
        assert_eq!(replies.len(), 2);
        assert!(replies.iter().all(|m| m.contains("only works inside a server")));
    }

    #[test]
    fn llm_namespace_is_not_the_reserved_guild_namespace() {
        assert_ne!(NAMESPACE, GUILD_SETTINGS);
    }
}
