use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;

use crate::kernel::{
    models::{EventKind, EventPayload, GuildId, Origin, PluginError, RequestContext},
    plugin_ports::{
        ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort, MiddlewarePluginPort, Next,
        Permission, PluginPort,
    },
    services::KernelServices,
};

use super::chat_engine::ChatEngine;
use super::commands::{
    AssignLlmHandler, AssignServiceChannelHandler, ClearServiceChannelHandler, CutoffLlmHandler,
    PromptLlmHandler, SET_KEYS, SetLlmHandler, StatusLlmHandler, UnassignLlmHandler,
};
use super::model::{ChannelConfig, NAMESPACE, channel_config_key};

/// Identifies one channel's processing lock: platform, guild, channel.
type ChannelKey = (String, u64, u64);

/// LLM chat plugin: lifecycle + admin commands (`PluginPort`) and the
/// conversation intake (`MiddlewarePluginPort`).
///
/// The `pre` hook never runs the engine inline - LLM calls are slow and the
/// pipeline must not wait on them. Assigned-channel messages spawn a task
/// that runs the engine under the channel's lock (tokio's mutex is fair, so
/// execution follows pipeline order, and records keep conversation order).
/// The hook itself only matches the event and reads the channel config.
pub struct LlmPlugin {
    registry: Arc<dyn CommandRegistryPort>,
    engine: Arc<ChatEngine>,
    channel_locks: Arc<Mutex<HashMap<ChannelKey, Arc<AsyncMutex<()>>>>>,
}

impl LlmPlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>, engine: Arc<ChatEngine>) -> Self {
        Self { registry, engine, channel_locks: Arc::new(Mutex::new(HashMap::new())) }
    }

    fn channel_lock(&self, origin: &Origin) -> Arc<AsyncMutex<()>> {
        let key: ChannelKey = (
            origin.platform.as_str().to_owned(),
            origin.guild_id.map_or(0, GuildId::get),
            origin.channel_id.get(),
        );
        Arc::clone(self.channel_locks.lock().entry(key).or_default())
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
        self.registry.register(
            self.descriptor(
                "llm_cutoff",
                "Reset this channel's conversation context (history is kept)",
                Vec::new(),
            ),
            Arc::new(CutoffLlmHandler),
        );
        self.registry.register(
            self.descriptor(
                "llm_status",
                "Show this channel's chat configuration and context state",
                Vec::new(),
            ),
            Arc::new(StatusLlmHandler),
        );
        self.registry.register(
            self.descriptor(
                "llm_set",
                "Tune this channel's chat bot",
                vec![
                    ArgDescriptor {
                        name: "key".to_owned(),
                        description: "Setting to change".to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: Some(SET_KEYS.iter().map(|key| (*key).to_owned()).collect()),
                    },
                    ArgDescriptor {
                        name: "value".to_owned(),
                        description: "New value (`clear` resets)".to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: None,
                    },
                ],
            ),
            Arc::new(SetLlmHandler),
        );
        self.registry.register(
            self.descriptor(
                "llm_prompt",
                "Set this channel's system prompt",
                vec![ArgDescriptor {
                    name: "prompt".to_owned(),
                    description: "Prompt text, or `clear`".to_owned(),
                    required: true,
                    kind: ArgKind::String,
                    choices: None,
                }],
            ),
            Arc::new(PromptLlmHandler),
        );
        Ok(())
    }
}

#[async_trait]
impl MiddlewarePluginPort for LlmPlugin {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        let (origin, payload) = match (&event.kind, &event.payload) {
            (EventKind::MessageReceived, EventPayload::Message(payload)) => {
                (event.origin.clone(), payload.clone())
            }
            _ => return Next::Continue,
        };

        // LLM chat is guild-only by design: per-channel config cannot exist
        // outside a guild.
        if origin.guild_id.is_none() {
            return Next::Continue;
        }
        let Some(storage) = &services.guild_storage else {
            return Next::Continue;
        };

        let raw = match storage.get(NAMESPACE, &channel_config_key(origin.channel_id.get())).await {
            Ok(Some(raw)) => raw,
            Ok(None) => return Next::Continue, // channel not assigned
            Err(err) => {
                tracing::warn!(namespace = NAMESPACE, %err, "llm channel config unreadable");
                return Next::Continue;
            }
        };
        let Ok(config) = serde_json::from_value::<ChannelConfig>(raw) else {
            tracing::warn!(namespace = NAMESPACE, "llm channel config is malformed - skipping");
            return Next::Continue;
        };

        // Off the pipeline task; capture/trigger decisions happen inside,
        // under the channel lock, on fresh records.
        let lock = self.channel_lock(&origin);
        let engine = Arc::clone(&self.engine);
        let services = services.clone();
        tokio::spawn(async move {
            let _guard = lock.lock().await;
            engine.handle_message(&origin, &payload, &config, &services).await;
        });

        Next::Continue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryCommandRegistry;
    use crate::kernel::models::MessagePayload;
    use crate::kernel::{
        models::{
            ChannelId as ChannelIdModel, CommandPayload, EventKind, EventPayload, GuildId,
            MessageId, Origin, Platform, RequestContext, UserId,
        },
        plugin_ports::{CommandArgs, CommandHandler},
        services::KernelServices,
        spi_ports::{ChatOutputPort, GUILD_SETTINGS, StoragePort},
    };
    use crate::plugins::llm::model::{
        ChannelConfig, ConversationState, NAMESPACE, SERVICE_CHANNEL_KEY, channel_config_key,
        channel_state_key, records_namespace,
    };
    use crate::plugins::llm::{
        ChatEngine, CompletionRequest, CompletionResponse, ConversationRecord, LlmCompletionPort,
        LlmError, LlmSettings, RandRandom, RandomPort, RecordRole,
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};

    struct StubCompletion;

    #[async_trait]
    impl LlmCompletionPort for StubCompletion {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            Ok(CompletionResponse { content: "stub reply".to_owned(), usage: None })
        }
    }

    fn message_event(channel_id: u64, mentions_bot: bool) -> RequestContext {
        RequestContext {
            kind: EventKind::MessageReceived,
            origin: Origin {
                platform: Platform::Discord,
                guild_id: Some(GuildId(1)),
                channel_id: ChannelIdModel(channel_id),
                user_id: UserId(3),
                message_id: Some(MessageId(4)),
                reply_token: None,
            },
            payload: EventPayload::Message(MessagePayload {
                content: "hello".to_owned(),
                author_name: Some("alice".to_owned()),
                author_roles: Vec::new(),
                author_permissions: 0,
                reply_to: None,
                mentions_bot,
            }),
        }
    }

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
        let engine = Arc::new(ChatEngine::new(
            Arc::new(LlmSettings::default()),
            Arc::new(StubCompletion) as Arc<dyn LlmCompletionPort>,
            Arc::new(RandRandom) as Arc<dyn RandomPort>,
        ));
        let plugin = LlmPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>, engine);
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
        assert_eq!(
            names,
            [
                "llm_admin",
                "llm_admin_clear",
                "llm_assign",
                "llm_cutoff",
                "llm_prompt",
                "llm_set",
                "llm_status",
                "llm_unassign"
            ]
        );
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

    fn seed_config_in(storage: &InMemoryStorage) {
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_config_key(2),
            serde_json::to_value(ChannelConfig::assigned("local/gemma".to_owned()))
                .expect("config expected to serialize"),
        );
    }

    #[tokio::test]
    async fn pre_spawns_engine_for_assigned_channels() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        let mut event = message_event(2, true);
        let next = plugin.pre(&mut event, &fixture.services).await;
        assert!(matches!(next, Next::Continue));

        // The engine runs on a spawned task - yield until its output lands.
        for _ in 0..1000 {
            if !fixture.output.messages().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(fixture.output.messages().iter().any(|m| m.contains("stub reply")));
    }

    #[tokio::test]
    async fn pre_ignores_unassigned_channels() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        let mut event = message_event(3, true); // no config doc for channel 3
        let next = plugin.pre(&mut event, &fixture.services).await;
        assert!(matches!(next, Next::Continue));

        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert!(fixture.output.messages().is_empty());
    }

    #[tokio::test]
    async fn pre_ignores_non_message_events() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        let mut event = command_event(Some(1));
        let next = plugin.pre(&mut event, &fixture.services).await;
        assert!(matches!(next, Next::Continue));

        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert!(fixture.output.messages().is_empty());
    }

    #[tokio::test]
    async fn cutoff_clears_summary_and_advances_past_all_records() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        fixture
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .append(
                &records_namespace(2),
                serde_json::to_value(ConversationRecord {
                    message_id: Some(10),
                    role: RecordRole::User,
                    author: Some("alice".to_owned()),
                    content: "old".to_owned(),
                    reply_to: None,
                    captured_at: 0,
                })
                .expect("record expected to serialize"),
            )
            .await
            .expect("append expected to succeed");
        fixture.storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_state_key(2),
            serde_json::json!({"summary": "old gist", "cutoff_seq": 0}),
        );

        CutoffLlmHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("cutoff expected to succeed");

        let state_raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable")
            .expect("state expected");
        let state: ConversationState =
            serde_json::from_value(state_raw).expect("state expected to deserialize");
        assert_eq!(state.summary, None);
        assert_eq!(state.cutoff_seq, 1);
        assert!(state.cutoff_at.is_some());
        assert!(fixture.output.messages().iter().any(|m| m.contains("Context cleared")));
    }

    #[tokio::test]
    async fn status_reports_model_context_and_summary() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);
        fixture.storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_state_key(2),
            serde_json::json!({"summary": "the gist", "cutoff_seq": 0, "cutoff_at": 1_717_000_000}),
        );

        StatusLlmHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("status expected to succeed");

        let messages = fixture.output.messages();
        assert!(messages.iter().any(|m| m.contains("LLM status")));
        assert!(messages.iter().any(|m| m.contains("local/gemma")));
        assert!(messages.iter().any(|m| m.contains("the gist")));
        // The record log is empty - the context-start line degrades honestly.
        assert!(messages.iter().any(|m| m.contains("no messages after the cutoff")));
    }

    #[tokio::test]
    async fn status_on_unassigned_channel_replies_so() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        StatusLlmHandler
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("status expected to succeed");

        assert!(fixture.output.messages().iter().any(|m| m.contains("not assigned")));
    }

    #[tokio::test]
    async fn set_updates_stored_channel_config() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        SetLlmHandler
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("key".to_owned(), "temperature".to_owned()),
                    ("value".to_owned(), "0.7".to_owned()),
                ]),
                &fixture.services,
            )
            .await
            .expect("set expected to succeed");

        let raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_config_key(2))
            .await
            .expect("config readable")
            .expect("config expected");
        let config: ChannelConfig =
            serde_json::from_value(raw).expect("config expected to deserialize");
        assert_eq!(config.params.temperature, Some(0.7));
        assert!(fixture.output.messages().iter().any(|m| m.contains("`temperature` set")));
    }

    #[tokio::test]
    async fn set_with_malformed_value_replies_usage_and_saves_nothing() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        SetLlmHandler
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("key".to_owned(), "depth".to_owned()),
                    ("value".to_owned(), "abc".to_owned()),
                ]),
                &fixture.services,
            )
            .await
            .expect("set expected to be handled");

        assert!(fixture.output.messages().iter().any(|m| m.contains("expects a whole number")));
        let raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_config_key(2))
            .await
            .expect("config readable")
            .expect("config expected");
        // The malformed set must not have touched the stored depth.
        let config: ChannelConfig =
            serde_json::from_value(raw).expect("config expected to deserialize");
        assert_eq!(config.history_depth, 100);
    }

    #[tokio::test]
    async fn set_on_unassigned_channel_replies_so() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        SetLlmHandler
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("key".to_owned(), "depth".to_owned()),
                    ("value".to_owned(), "10".to_owned()),
                ]),
                &fixture.services,
            )
            .await
            .expect("set expected to be handled");

        assert!(fixture.output.messages().iter().any(|m| m.contains("not assigned")));
    }

    #[tokio::test]
    async fn prompt_sets_and_clears_system_prompt() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        PromptLlmHandler
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![("prompt".to_owned(), "You are a pirate.".to_owned())]),
                &fixture.services,
            )
            .await
            .expect("prompt expected to succeed");
        let raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_config_key(2))
            .await
            .expect("config readable")
            .expect("config expected");
        let config: ChannelConfig =
            serde_json::from_value(raw).expect("config expected to deserialize");
        assert_eq!(config.system_prompt.as_deref(), Some("You are a pirate."));

        PromptLlmHandler
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![("prompt".to_owned(), "clear".to_owned())]),
                &fixture.services,
            )
            .await
            .expect("clear expected to succeed");
        let raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_config_key(2))
            .await
            .expect("config readable")
            .expect("config expected");
        let config: ChannelConfig =
            serde_json::from_value(raw).expect("config expected to deserialize");
        assert_eq!(config.system_prompt, None);
    }
}
