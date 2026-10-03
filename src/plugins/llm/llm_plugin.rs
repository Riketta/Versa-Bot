use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;

use crate::kernel::{
    models::{EventKind, EventPayload, GuildId, Origin, PluginError, RequestContext},
    plugin_ports::{
        AccessTier, ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort,
        MiddlewarePluginPort, Next, PluginPort,
    },
    services::KernelServices,
};

use super::chat_engine::ChatEngine;
use super::commands::{
    AssignLlmHandler, AssignServiceChannelHandler, ClearServiceChannelHandler, CutoffLlmHandler,
    ModelsLlmHandler, PromptFileLlmHandler, PromptLlmHandler, SET_KEYS, SetLlmHandler,
    StatusLlmHandler, UnassignLlmHandler, model_choices,
};
use super::model::{ChannelConfig, NAMESPACE, channel_config_key};

/// Whole-request timeout for prompt-file downloads from the Discord CDN -
/// deliberately shorter than provider timeouts: the fetch is interactive
/// (the invoking admin waits for the answer).
const PROMPT_FETCH_TIMEOUT_SECS: u64 = 30;

/// Identifies one channel's processing lock: platform, guild, channel.
type ChannelKey = (String, u64, u64);

/// Per-channel processing locks, shared by the engine intake and the
/// state-mutating admin commands: everything that reads or writes one
/// channel's records, state, or config serializes on the same key - an
/// in-flight engine run can no longer undo a `/llm_cutoff` commit or race
/// a `/llm_set` read-modify-write.
#[derive(Default)]
pub(super) struct ChannelLocks {
    locks: Mutex<HashMap<ChannelKey, Arc<AsyncMutex<()>>>>,
}

impl ChannelLocks {
    #[must_use]
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(super) fn lock_for(&self, origin: &Origin) -> Arc<AsyncMutex<()>> {
        let key: ChannelKey = (
            origin.platform.as_str().to_owned(),
            origin.guild_id.map_or(0, GuildId::get),
            origin.channel_id.get(),
        );
        Arc::clone(self.locks.lock().entry(key).or_default())
    }
}

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
    channel_locks: Arc<ChannelLocks>,
    /// Keyless client for prompt-file downloads - Discord CDN only (host
    /// pinned in the handler), never a provider endpoint.
    prompt_fetch: reqwest::Client,
}

impl LlmPlugin {
    /// Builds the plugin with its prompt-file download client.
    ///
    /// # Panics
    /// Only if reqwest cannot build a client from purely static settings
    /// (TLS backend unavailable) - a process-level defect, not config.
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>, engine: Arc<ChatEngine>) -> Self {
        let prompt_fetch = reqwest::Client::builder()
            .timeout(Duration::from_secs(PROMPT_FETCH_TIMEOUT_SECS))
            .build()
            .expect("static prompt-fetch client config expected to build");
        Self { registry, engine, channel_locks: ChannelLocks::new(), prompt_fetch }
    }

    fn channel_lock(&self, origin: &Origin) -> Arc<AsyncMutex<()>> {
        self.channel_locks.lock_for(origin)
    }

    fn descriptor(
        &self,
        name: &str,
        description: &str,
        arguments: Vec<ArgDescriptor>,
        tier: AccessTier,
    ) -> CommandDescriptor {
        CommandDescriptor {
            plugin_id: self.name().to_owned(),
            name: name.to_owned(),
            description: description.to_owned(),
            arguments,
            // No platform gate: moderators may lack Discord's Manage Server
            // permission. The auth plugin enforces the tier kernel-side and
            // answers with an ephemeral denial.
            required_permission: None,
            required_tier: Some(tier),
            guild_only: true,
        }
    }

    /// The two system-prompt commands: inline text (Discord option limit)
    /// and attachment upload (long prompts, formatting-preserving).
    fn register_prompt_commands(&self) {
        self.registry.register(
            self.descriptor(
                "llm_prompt",
                "Set the chat bot's persona for this channel (clear = plugin default)",
                vec![ArgDescriptor {
                    name: "prompt".to_owned(),
                    description: "Prompt text; `clear` restores the default (long prompts: \
                         /llm_prompt_file)"
                        .to_owned(),
                    required: true,
                    kind: ArgKind::String,
                    choices: None,
                }],
                AccessTier::Moderator,
            ),
            Arc::new(PromptLlmHandler::new(Arc::clone(&self.channel_locks))),
        );
        self.registry.register(
            self.descriptor(
                "llm_prompt_file",
                "Set this channel's persona from an uploaded text file (formatting is kept)",
                vec![ArgDescriptor {
                    name: "file".to_owned(),
                    description: "Text (.txt/.md) file holding the prompt; size-capped by the \
                         operator"
                        .to_owned(),
                    required: true,
                    kind: ArgKind::Attachment,
                    choices: None,
                }],
                AccessTier::Moderator,
            ),
            Arc::new(PromptFileLlmHandler::new(
                Arc::clone(&self.channel_locks),
                self.prompt_fetch.clone(),
                self.engine.settings().max_prompt_file_bytes,
            )),
        );
    }

    /// Channel assignment and inspection: turning the bot on/off per
    /// channel, the model catalog, and the tune/reset commands.
    fn register_model_commands(&self) {
        // Discord renders the declared refs as a native dropdown (capped at
        // 25); runtime validation enforces the full registry either way.
        let model_refs = model_choices(self.engine.settings());
        self.registry.register(
            self.descriptor(
                "llm_assign",
                "Turn the LLM chat bot on in this channel (starts with an empty context)",
                vec![ArgDescriptor {
                    name: "model".to_owned(),
                    description: "Model to answer with; the dropdown lists the operator-declared \
                         models"
                        .to_owned(),
                    required: true,
                    kind: ArgKind::String,
                    choices: (!model_refs.is_empty()).then_some(model_refs),
                }],
                AccessTier::Moderator,
            ),
            Arc::new(AssignLlmHandler::new(Arc::clone(&self.engine))),
        );
        self.registry.register(
            self.descriptor(
                "llm_models",
                "List the models the bot operator has made available for channels",
                Vec::new(),
                AccessTier::User,
            ),
            Arc::new(ModelsLlmHandler::new(Arc::clone(&self.engine))),
        );
        self.registry.register(
            self.descriptor(
                "llm_unassign",
                "Turn the LLM chat bot off in this channel (stored history is kept)",
                Vec::new(),
                AccessTier::Moderator,
            ),
            Arc::new(UnassignLlmHandler),
        );
    }
}

impl PluginPort for LlmPlugin {
    fn name(&self) -> &'static str {
        "llm"
    }

    fn init(&self) -> Result<(), PluginError> {
        self.register_model_commands();
        self.registry.register(
            self.descriptor(
                "llm_admin",
                "Send LLM errors and service notices to this channel (one per guild)",
                Vec::new(),
                AccessTier::Moderator,
            ),
            Arc::new(AssignServiceChannelHandler),
        );
        self.registry.register(
            self.descriptor(
                "llm_admin_clear",
                "Stop sending LLM service notices for this guild",
                Vec::new(),
                AccessTier::Moderator,
            ),
            Arc::new(ClearServiceChannelHandler),
        );
        self.registry.register(
            self.descriptor(
                "llm_cutoff",
                "Reset this channel's conversation context (stored history is kept)",
                Vec::new(),
                AccessTier::Moderator,
            ),
            Arc::new(CutoffLlmHandler::new(Arc::clone(&self.channel_locks))),
        );
        self.registry.register(
            self.descriptor(
                "llm_status",
                "Show this channel's chat bot settings, context state and usage stats",
                Vec::new(),
                AccessTier::User,
            ),
            Arc::new(StatusLlmHandler::new(Arc::clone(&self.engine))),
        );
        self.registry.register(
            self.descriptor(
                "llm_set",
                "Change a channel chat setting (model, images, reasoning, sampling, chime-ins)",
                vec![
                    ArgDescriptor {
                        name: "key".to_owned(),
                        description: "Setting to change - see the dropdown (reasoning_effort=off \
                             disables thinking)"
                            .to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: Some(SET_KEYS.iter().map(|key| (*key).to_owned()).collect()),
                    },
                    ArgDescriptor {
                        name: "value".to_owned(),
                        description: "New value; `clear` = default (for reasoning_effort, `off` \
                             disables thinking)"
                            .to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: None,
                    },
                ],
                AccessTier::Moderator,
            ),
            Arc::new(SetLlmHandler::new(Arc::clone(&self.channel_locks), Arc::clone(&self.engine))),
        );
        self.register_prompt_commands();
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
    use crate::plugins::llm::providers::ModelSettings;
    use crate::plugins::llm::{
        ChatEngine, CompletionRequest, CompletionResponse, ConversationRecord, ImageDescriber,
        ImageJob, ImageSource, LlmCompletionPort, LlmError, LlmSettings, RandRandom, RandomPort,
        RecordRole, ResponseTiming,
    };
    use crate::test_support::{
        FailingStorage, InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory,
    };

    struct StubCompletion;

    /// Capture-path stub: no test here sends image payloads, so describe is
    /// never called - it exists to satisfy the engine's constructor.
    #[derive(Default)]
    struct FakeDescriber;

    #[async_trait]
    impl ImageDescriber for FakeDescriber {
        async fn describe(&self, _job: &ImageJob, images: Vec<ImageSource>) -> Vec<Option<String>> {
            images.iter().map(|_| None).collect()
        }
    }

    #[async_trait]
    impl LlmCompletionPort for StubCompletion {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            Ok(CompletionResponse {
                content: "stub reply".to_owned(),
                usage: None,
                timing: ResponseTiming::measured(0),
            })
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
                attachments: Vec::new(),
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
        engine: Arc<ChatEngine>,
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
        let mut settings = LlmSettings::default();
        for reference in ["local/gemma", "zai/glm-5.3-flash"] {
            settings.models.insert(reference.to_owned(), ModelSettings::default());
        }
        let engine = Arc::new(ChatEngine::new(
            Arc::new(settings),
            Arc::new(StubCompletion) as Arc<dyn LlmCompletionPort>,
            Arc::new(RandRandom) as Arc<dyn RandomPort>,
            Arc::new(FakeDescriber) as Arc<dyn ImageDescriber>,
        ));
        let plugin = LlmPlugin::new(
            Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&engine),
        );
        (plugin, Fixture { registry, storage, services, output, engine })
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

        let descriptors = fixture.registry.descriptors();
        crate::test_support::assert_descriptions_fit_discord(&descriptors);
        let mut names: Vec<String> = descriptors.into_iter().map(|d| d.name).collect();
        names.sort();
        assert_eq!(
            names,
            [
                "llm_admin",
                "llm_admin_clear",
                "llm_assign",
                "llm_cutoff",
                "llm_models",
                "llm_prompt",
                "llm_prompt_file",
                "llm_set",
                "llm_status",
                "llm_unassign"
            ]
        );
        for descriptor in fixture.registry.descriptors() {
            assert_eq!(descriptor.plugin_id, "llm");
            assert!(descriptor.guild_only, "every llm command is guild-only");
            assert!(descriptor.required_permission.is_none(), "tier-gated, not platform-gated");
            let expected = if matches!(descriptor.name.as_str(), "llm_status" | "llm_models") {
                AccessTier::User
            } else {
                AccessTier::Moderator
            };
            assert_eq!(descriptor.required_tier, Some(expected), "command {}", descriptor.name);
        }
    }

    #[tokio::test]
    async fn assign_stores_channel_config_and_replies() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        AssignLlmHandler::new(Arc::clone(&fixture.engine))
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

        AssignLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(&command_event(Some(1)), &args("local/gemma"), &fixture.services)
            .await
            .expect("first assign expected to succeed");
        AssignLlmHandler::new(Arc::clone(&fixture.engine))
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

        AssignLlmHandler::new(Arc::clone(&fixture.engine))
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

    /// The registry boundary holds at the handler level: an undeclared model
    /// is rejected with a correction and nothing is stored.
    #[tokio::test]
    async fn assign_undeclared_model_is_rejected_and_writes_nothing() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        AssignLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![("model".to_owned(), "nope/model".to_owned())]),
                &fixture.services,
            )
            .await
            .expect("assign expected to be handled");

        assert!(fixture.output.messages().iter().any(|m| m.contains("Unknown model `nope/model`")));
        let storage = fixture.services.guild_storage.as_ref().expect("guild storage expected");
        assert_eq!(
            storage.list_keys(NAMESPACE).await.expect("keys readable"),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn set_model_undeclared_is_rejected_and_saves_nothing() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        SetLlmHandler::new(ChannelLocks::new(), Arc::clone(&fixture.engine))
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("key".to_owned(), "model".to_owned()),
                    ("value".to_owned(), "nope/model".to_owned()),
                ]),
                &fixture.services,
            )
            .await
            .expect("set expected to be handled");

        assert!(fixture.output.messages().iter().any(|m| m.contains("Unknown model")));
        let raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_config_key(2))
            .await
            .expect("config readable")
            .expect("config expected");
        // The rejected set must not have touched the stored model.
        assert_eq!(raw.get("model").and_then(|v| v.as_str()), Some("local/gemma"));
    }

    /// The model argument ships the declared refs as Discord choices - the
    /// discovery half of the registry boundary.
    #[test]
    fn assign_argument_carries_declared_models_as_choices() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        let assign = fixture
            .registry
            .descriptors()
            .into_iter()
            .find(|descriptor| descriptor.name == "llm_assign")
            .expect("llm_assign expected registered");
        let model_argument = assign.arguments.first().expect("model argument expected");
        assert_eq!(
            model_argument.choices.as_deref(),
            Some(["local/gemma".to_owned(), "zai/glm-5.3-flash".to_owned()].as_ref())
        );
    }

    #[tokio::test]
    async fn models_command_lists_declared_refs() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        ModelsLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("models expected to succeed");

        let messages = fixture.output.messages();
        assert!(messages.iter().any(|m| m.contains("Declared models:")));
        assert!(messages.iter().any(|m| m.contains("`local/gemma`")));
    }

    #[tokio::test]
    async fn unassign_clears_config_and_is_idempotent() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        let storage = Arc::clone(&fixture.storage);

        AssignLlmHandler::new(Arc::clone(&fixture.engine))
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

        AssignLlmHandler::new(Arc::clone(&fixture.engine))
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

    /// An unreadable config document is a degraded guild, not a crashed one:
    /// the hook continues (no engine run) instead of failing the pipeline.
    #[tokio::test]
    async fn pre_continues_when_config_storage_fails() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        let output = Arc::clone(&fixture.output);
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: Some(FailingStorage.guild_scoped(Platform::Discord, GuildId(1))),
        };

        let mut event = message_event(2, true);
        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert!(fixture.output.messages().is_empty());
    }

    /// A malformed config document skips the channel (warn + Continue):
    /// answering from defaults would fabricate a conversation.
    #[tokio::test]
    async fn pre_skips_channel_with_malformed_config() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        fixture.storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_config_key(2),
            serde_json::json!("not a config"),
        );

        let mut event = message_event(2, true);
        assert!(matches!(plugin.pre(&mut event, &fixture.services).await, Next::Continue));

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
                    images: Vec::new(),
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

        CutoffLlmHandler::new(ChannelLocks::new())
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

    /// `/llm_cutoff` serializes against the engine's per-channel lock: while
    /// an engine run holds the lock, the cutoff waits instead of committing a
    /// state an in-flight compaction could later overwrite.
    #[tokio::test]
    async fn cutoff_waits_for_the_channel_lock() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);
        fixture.storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            &channel_state_key(2),
            serde_json::json!({"summary": "old gist", "cutoff_seq": 0}),
        );
        let record = serde_json::to_value(ConversationRecord {
            message_id: Some(1),
            role: RecordRole::User,
            author: Some("alice".to_owned()),
            content: "old".to_owned(),
            reply_to: None,
            captured_at: 0,
            images: Vec::new(),
        })
        .expect("record expected to serialize");
        fixture
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .append(&records_namespace(2), record)
            .await
            .expect("append expected to succeed");

        // Hold the engine-side lock for the channel.
        let lock = plugin.channel_lock(&command_event(Some(1)).origin);
        let guard = lock.lock().await;

        let services = fixture.services.clone();
        let handler = CutoffLlmHandler::new(Arc::clone(&plugin.channel_locks));
        let task = tokio::spawn(async move {
            handler
                .invoke(&command_event(Some(1)), &CommandArgs::default(), &services)
                .await
                .expect("cutoff expected to succeed");
        });

        // The cutoff must still be waiting: the old state is untouched.
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        let state_raw = fixture
            .services
            .guild_storage
            .as_ref()
            .expect("guild storage expected")
            .get(NAMESPACE, &channel_state_key(2))
            .await
            .expect("state readable")
            .expect("state expected");
        assert_eq!(
            state_raw,
            serde_json::json!({"summary": "old gist", "cutoff_seq": 0}),
            "the cutoff must wait for the channel lock, not race the engine"
        );

        drop(guard);
        task.await.expect("cutoff task expected to finish");

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

        StatusLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("status expected to succeed");

        let messages = fixture.output.messages();
        assert!(messages.iter().any(|m| m.contains("LLM status")));
        assert!(messages.iter().any(|m| m.contains("local/gemma")));
        assert!(messages.iter().any(|m| m.contains("the gist")));
        // The record log is empty - the context-start line degrades honestly.
        assert!(messages.iter().any(|m| m.contains("no messages after the cutoff")));
        // No channel override: the report names the effective plugin default.
        assert!(messages.iter().any(|m| m.contains("Prompt: plugin default")));
        assert!(messages.iter().any(|m| m.contains("You are a helpful chat assistant.")));
    }

    /// An override is reported as such - and its head preview + fingerprint
    /// let an admin verify the active version without printing the whole
    /// prompt.
    #[tokio::test]
    async fn status_shows_the_channel_prompt_override() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        PromptLlmHandler::new(ChannelLocks::new())
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![("prompt".to_owned(), "You are a pirate.".to_owned())]),
                &fixture.services,
            )
            .await
            .expect("prompt expected to succeed");

        StatusLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("status expected to succeed");

        let messages = fixture.output.messages();
        assert!(messages.iter().any(|m| m.contains("Prompt: channel override")));
        assert!(messages.iter().any(|m| m.contains("You are a pirate.")));
        // 17 chars, fingerprint present in the same line.
        assert!(messages.iter().any(|m| m.contains("17 chars, #")));
    }

    #[tokio::test]
    async fn status_on_unassigned_channel_replies_so() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        StatusLlmHandler::new(Arc::clone(&fixture.engine))
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

        SetLlmHandler::new(ChannelLocks::new(), Arc::clone(&fixture.engine))
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

        SetLlmHandler::new(ChannelLocks::new(), Arc::clone(&fixture.engine))
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

        SetLlmHandler::new(ChannelLocks::new(), Arc::clone(&fixture.engine))
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

        PromptLlmHandler::new(ChannelLocks::new())
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

        PromptLlmHandler::new(ChannelLocks::new())
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
