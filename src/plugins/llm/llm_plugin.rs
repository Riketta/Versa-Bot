use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::FutureExt;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::common::panic_message;
use crate::kernel::{
    models::{
        EventKind, EventPayload, GuildId, MessagePayload, Origin, PluginError, RequestContext,
    },
    plugin_ports::{
        AccessTier, ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort,
        MiddlewarePluginPort, Next, PluginPort,
    },
    services::KernelServices,
};

use super::chat_engine::ChatEngine;
use super::commands::{
    AssignLlmHandler, AssignServiceChannelHandler, ClearServiceChannelHandler, CutoffLlmHandler,
    DumpLlmHandler, GetLlmHandler, ModelsLlmHandler, SET_KEYS, SetLlmHandler, SetPromptLlmHandler,
    StatusLlmHandler, UnassignLlmHandler, model_choices,
};
use super::conversation::{ConversationRecord, RecordRole};
use super::model::{
    ChannelConfig, ConversationState, NAMESPACE, channel_config_key, channel_state_key,
    records_namespace,
};

/// Whole-request timeout for prompt-file downloads from the Discord CDN -
/// deliberately shorter than provider timeouts: the fetch is interactive
/// (the invoking admin waits for the answer).
const PROMPT_FETCH_TIMEOUT_SECS: u64 = 30;

/// Cap on accepted-but-unfinished engine runs per channel - the flood
/// valve. Generous by design: regular multi-guild traffic never approaches
/// it; a channel that floods past the cap sheds excess messages (warn +
/// skip, not captured) instead of accumulating tasks and memory without
/// bound.
const MAX_PENDING_RUNS_PER_CHANNEL: usize = 64;

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

/// Per-channel admission permits, keyed like [`ChannelLocks`]: at most
/// [`MAX_PENDING_RUNS_PER_CHANNEL`] accepted-but-unfinished engine runs per
/// channel. Handed out non-blockingly (`try_acquire` - the pipeline never
/// waits) and held until the spawned run finishes, so the shed decision
/// happens at intake while the slot frees itself on completion.
#[derive(Default)]
struct ChannelPermits {
    permits: Mutex<HashMap<ChannelKey, Arc<Semaphore>>>,
}

impl ChannelPermits {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn permit_for(&self, origin: &Origin) -> Arc<Semaphore> {
        let key: ChannelKey = (
            origin.platform.as_str().to_owned(),
            origin.guild_id.map_or(0, GuildId::get),
            origin.channel_id.get(),
        );
        Arc::clone(
            self.permits
                .lock()
                .entry(key)
                .or_insert_with(|| Arc::new(Semaphore::new(MAX_PENDING_RUNS_PER_CHANNEL))),
        )
    }
}

/// LLM chat plugin: lifecycle + admin commands (`PluginPort`) and the
/// conversation intake (`MiddlewarePluginPort`).
///
/// The `pre` hook never runs the engine inline - LLM calls are slow and the
/// pipeline must not wait on them. Assigned-channel messages spawn a task
/// that runs the engine under the channel's lock (tokio's mutex is fair, so
/// runs serialize in lock-arrival order). The gateway dispatches each event
/// on its own task and the hook awaits a storage read before the spawn, so
/// strict gateway-order processing is a best effort, not a guarantee.
/// The hook itself only matches the event and reads the channel config.
pub struct LlmPlugin {
    registry: Arc<dyn CommandRegistryPort>,
    engine: Arc<ChatEngine>,
    channel_locks: Arc<ChannelLocks>,
    /// Admission permits bounding outstanding engine runs per channel (the
    /// flood valve - see [`MAX_PENDING_RUNS_PER_CHANNEL`]).
    channel_permits: Arc<ChannelPermits>,
    /// Cancelled in `stop`: afterwards no new engine runs are admitted (the
    /// kernel stops plugins during shutdown). In-flight runs finish
    /// naturally - they are bounded by the permits.
    shutdown: CancellationToken,
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
            // Same trust boundary as vision downloads: the CDN host is
            // prefix-checked per request - redirects must not carry the
            // fetch off the pinned host, so none are followed.
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(PROMPT_FETCH_TIMEOUT_SECS))
            .build()
            .expect("static prompt-fetch client config expected to build");
        Self {
            registry,
            engine,
            channel_locks: ChannelLocks::new(),
            channel_permits: ChannelPermits::new(),
            shutdown: CancellationToken::new(),
            prompt_fetch,
        }
    }

    /// Whether `reply_to` names one of the bot's recorded turns in this
    /// channel - the panic-path trigger check, after the engine's in-flight
    /// window died with the unwound frame. Reads the same bounded
    /// post-cutoff window the engine would have used. A storage error keeps
    /// the reply trigger silent (logged): history integrity is the only
    /// source of truth, and guessing would fire fallback notices into
    /// conversations between users.
    async fn reply_targets_bot(
        storage: &Arc<dyn crate::kernel::spi_ports::GuildStorage>,
        channel_id: u64,
        reply_to: u64,
        depth: u32,
        keep_tail: u32,
    ) -> bool {
        let window = usize::try_from(depth)
            .unwrap_or(usize::MAX)
            .saturating_add(usize::try_from(keep_tail).unwrap_or(usize::MAX))
            .max(1);
        let limit = u32::try_from(window).unwrap_or(u32::MAX);
        let cutoff = storage
            .get(NAMESPACE, &channel_state_key(channel_id))
            .await
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_value::<ConversationState>(raw).ok())
            .map_or(0, |state| state.cutoff_seq);
        match storage.list_last(&records_namespace(channel_id), limit).await {
            Ok(stored) => stored
                .into_iter()
                .filter(|record| record.seq > cutoff)
                .filter_map(|record| {
                    serde_json::from_value::<ConversationRecord>(record.payload).ok()
                })
                .any(|record| {
                    record.role == RecordRole::Assistant && record.message_id == Some(reply_to)
                }),
            Err(err) => {
                tracing::warn!(channel = channel_id, %err, "reply-trigger check failed after engine panic");
                false
            }
        }
    }

    /// Whether the engine would answer this message, decided WITHOUT the
    /// channel lock - the mirror the plugin needs for paths outside the
    /// engine frame: typing starts before the lock (the queue wait must
    /// read as "composing") and panic recovery re-checks after the frame
    /// unwound. A mention is decidable from the payload; a reply trigger
    /// matches Assistant records through `reply_targets_bot` - the same
    /// bounded post-cutoff window `should_trigger` reads under the lock.
    async fn payload_triggers(
        services: &KernelServices,
        payload: &MessagePayload,
        channel_id: u64,
        depth: u32,
        keep_tail: u32,
    ) -> bool {
        if payload.mentions_bot {
            return true;
        }
        let Some(storage) = &services.guild_storage else {
            return false;
        };
        match payload.reply_to {
            Some(reply_to) => {
                Self::reply_targets_bot(storage, channel_id, reply_to.get(), depth, keep_tail).await
            }
            None => false,
        }
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

    /// The one prompt command: every channel prompt (system persona,
    /// compaction instruction, image instruction) is set inline, from an
    /// uploaded file, or read back here - `/llm_set` stays a pure key/value
    /// tuner and keeps its dropdown far under Discord's choice cap.
    fn register_prompt_command(&self) {
        self.registry.register(
            self.descriptor(
                "llm_set_prompt",
                "Set or show a channel prompt: system (persona), compaction or image",
                vec![
                    ArgDescriptor {
                        name: "kind".to_owned(),
                        description: "Prompt to change - system = bot persona, compaction = \
                             history summary, image = descriptions"
                            .to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: Some(
                            ["system", "compaction", "image"]
                                .iter()
                                .map(|kind| (*kind).to_owned())
                                .collect(),
                        ),
                    },
                    ArgDescriptor {
                        name: "prompt".to_owned(),
                        description: "Prompt text; `clear` restores the plugin default; omit \
                             with `file` to view"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::String,
                        choices: None,
                    },
                    ArgDescriptor {
                        name: "file".to_owned(),
                        description: "Text (.txt/.md) file with the prompt for long texts; \
                             overrides `prompt`"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::Attachment,
                        choices: None,
                    },
                ],
                AccessTier::Moderator,
            ),
            Arc::new(SetPromptLlmHandler::new(
                Arc::clone(&self.channel_locks),
                Arc::clone(&self.engine),
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
                "Change a channel chat setting (model, images, reasoning, sampling, reactions)",
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
        self.registry.register(
            self.descriptor(
                "llm_get",
                "Show the current value of a channel chat setting (all keys when omitted)",
                vec![ArgDescriptor {
                    name: "key".to_owned(),
                    description: "Setting to read - see the dropdown; omit to list every setting"
                        .to_owned(),
                    required: false,
                    kind: ArgKind::String,
                    choices: Some(SET_KEYS.iter().map(|key| (*key).to_owned()).collect()),
                }],
                AccessTier::Moderator,
            ),
            Arc::new(GetLlmHandler::new(Arc::clone(&self.engine))),
        );
        self.registry.register(
            self.descriptor(
                "llm_dump",
                "Dump every channel chat setting at once as one copy-pasteable block",
                Vec::new(),
                AccessTier::Moderator,
            ),
            Arc::new(DumpLlmHandler::new(Arc::clone(&self.engine))),
        );
        self.register_prompt_command();
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        // Shutdown gate: the kernel stops plugins while the gateway is
        // already tearing down - admit no new engine runs from here on.
        // Runs already admitted finish naturally (bounded by the permits).
        self.shutdown.cancel();
        Ok(())
    }
}

#[async_trait]
impl MiddlewarePluginPort for LlmPlugin {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        // Cheap rejects first - nothing before the spawn needs owned data,
        // so DMs and channels that turn out unassigned never pay for a
        // payload clone.
        if !matches!(
            (&event.kind, &event.payload),
            (EventKind::MessageReceived, EventPayload::Message(_))
        ) {
            return Next::Continue;
        }
        let EventPayload::Message(payload) = &event.payload else {
            return Next::Continue; // unreachable: the match above paired them
        };
        let origin = &event.origin;

        // LLM chat is guild-only by design: per-channel config cannot exist
        // outside a guild.
        if origin.guild_id.is_none() {
            return Next::Continue;
        }
        let Some(storage) = &services.guild_storage else {
            return Next::Continue;
        };

        // Shutdown gate first: a stopped plugin admits nothing.
        if self.shutdown.is_cancelled() {
            return Next::Continue;
        }
        // Admission control before any I/O: at most
        // MAX_PENDING_RUNS_PER_CHANNEL accepted-but-unfinished runs per
        // channel. `try_acquire` never blocks the pipeline - a channel
        // flooding past the cap sheds its excess messages (not captured,
        // warned) instead of accumulating unbounded work.
        let Ok(permit) = self.channel_permits.permit_for(origin).try_acquire_owned() else {
            tracing::warn!(
                channel = origin.channel_id.get(),
                "engine run backlog full - shedding message"
            );
            return Next::Continue;
        };

        // Read OUTSIDE the channel lock by design: intake must not queue
        // behind an in-flight run just to read config (the admin commands
        // hold that lock). A `/llm_set` or `/llm_cutoff` landing between
        // this read and the lock below therefore applies one message late -
        // the in-flight run executes on the stale snapshot. Self-limited to
        // that one message; accepted.
        let raw = match storage.get(NAMESPACE, &channel_config_key(origin.channel_id.get())).await {
            Ok(Some(raw)) => raw,
            Ok(None) => return Next::Continue, // channel not assigned
            Err(err) => {
                tracing::warn!(namespace = NAMESPACE, %err, "llm channel config unreadable");
                return Next::Continue;
            }
        };
        let Ok(config) = ChannelConfig::from_stored(raw) else {
            tracing::warn!(namespace = NAMESPACE, "llm channel config is malformed - skipping");
            return Next::Continue;
        };

        // Clone for the spawned run only now that the channel is known
        // assigned - the copies are actually used.
        let origin = event.origin.clone();
        let payload = payload.clone();

        // Off the pipeline task; capture/trigger decisions happen inside,
        // under the channel lock, on fresh records. The permit rides along
        // and frees when the run finishes.
        let lock = self.channel_lock(&origin);
        let engine = Arc::clone(&self.engine);
        let services = services.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if shutdown.is_cancelled() {
                return;
            }
            // Typing must cover the queue, not just the generation: a
            // triggered run first waits here for an in-flight answer to
            // release the channel lock, and users should see "composing"
            // from the moment the run was accepted. Chime-ins have not
            // rolled yet, so unaddressed messages stay quiet; a rare
            // disagreement with the engine's post-lock decision only means
            // a brief typing flicker - the guard drops with the task.
            let typing = if Self::payload_triggers(
                &services,
                &payload,
                origin.channel_id.get(),
                config.history_depth,
                engine.settings().compaction_keep_tail,
            )
            .await
            {
                Some(services.chat_output_factory.start_typing(&origin))
            } else {
                None
            };
            let _guard = lock.lock().await;
            // Last-resort panic isolation: the pipeline catches panics in
            // every hook, but the engine runs here, off-pipeline - an
            // uncaught panic would silently drop a guaranteed answer. The
            // channel lock and the admission permit drop with the unwound
            // task, so the channel stays usable.
            let run = std::panic::AssertUnwindSafe(
                engine.handle_message(&origin, &payload, &config, &services, typing),
            )
            .catch_unwind()
            .await;
            if let Err(panic) = run {
                tracing::error!(
                    channel = origin.channel_id.get(),
                    panic = panic_message(&panic),
                    "LLM engine panicked - run aborted"
                );
                // The guaranteed-answer contract survives the panic: a
                // triggered message still receives the generic fallback
                // (the engine's state is intact - only the call frame
                // unwound). The rare panic after content was already
                // revealed may place the notice after a partial answer -
                // acceptable noise for an internal bug.
                let triggered = Self::payload_triggers(
                    &services,
                    &payload,
                    origin.channel_id.get(),
                    config.history_depth,
                    engine.settings().compaction_keep_tail,
                )
                .await;
                if triggered {
                    engine.send_fallback(&origin, &services).await;
                }
            }
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

    /// Completion whose call panics - the fixture for the engine panic guard.
    struct PanickingCompletion;

    #[async_trait]
    impl LlmCompletionPort for PanickingCompletion {
        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            panic!("completion exploded");
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
                guild_name: None,
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
                "llm_dump",
                "llm_get",
                "llm_models",
                "llm_set",
                "llm_set_prompt",
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
    async fn get_reads_back_the_channel_config() {
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

        let handler = GetLlmHandler::new(Arc::clone(&fixture.engine));
        let invoke = |key: Option<&str>| {
            let args = key.map(|key| vec![("key".to_owned(), key.to_owned())]).unwrap_or_default();
            (&handler, CommandArgs(args))
        };

        // Single key: the stored value.
        let (handler_ref, args) = invoke(Some("model"));
        handler_ref
            .invoke(&command_event(Some(1)), &args, &fixture.services)
            .await
            .expect("get expected to succeed");
        let reply = fixture.output.messages().last().expect("reply expected").clone();
        assert!(reply.contains("`model`: local/gemma"), "unexpected: {reply}");

        // Unknown key: usage, not a value.
        let (handler_ref, args) = invoke(Some("nonsense"));
        handler_ref
            .invoke(&command_event(Some(1)), &args, &fixture.services)
            .await
            .expect("get expected to succeed");
        let reply = fixture.output.messages().last().expect("reply expected").clone();
        assert!(reply.contains("Unknown key"), "unexpected: {reply}");

        // No key: one line per setting, defaults as effective values.
        let (handler_ref, args) = invoke(None);
        handler_ref
            .invoke(&command_event(Some(1)), &args, &fixture.services)
            .await
            .expect("get expected to succeed");
        let reply = fixture.output.messages().last().expect("reply expected").clone();
        assert!(reply.contains("Channel settings:"), "unexpected: {reply}");
        assert!(reply.contains("`react`: off"), "unexpected: {reply}");
        assert!(reply.contains("`model`: local/gemma"), "unexpected: {reply}");
        assert!(reply.contains("plugin default"), "unexpected: {reply}");
    }

    /// `/llm_dump`: every key at once, `key = value` inside a four-backtick
    /// fence, effective values; keys that differ from a fresh assignment
    /// carry a `*` marker, defaults do not. Prompts never enter the dump -
    /// not even as a preview: only set-or-not and size, the text is one
    /// argument-free `/llm_set_prompt kind` away. Long non-prompt values
    /// (templates) degrade to head previews so the whole dump stays one
    /// Discord message.
    #[tokio::test]
    async fn dump_lists_every_setting_in_a_fence() {
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
        let long_prompt = "ário ".repeat(400); // 2400 chars, multibyte-safe
        prompt_handler(&fixture)
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("kind".to_owned(), "compaction".to_owned()),
                    ("prompt".to_owned(), long_prompt.clone()),
                ]),
                &fixture.services,
            )
            .await
            .expect("set expected to succeed");

        DumpLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("dump expected to succeed");

        let reply = fixture.output.messages().last().expect("reply expected").clone();
        assert!(reply.contains("````"), "four-backtick fence expected: {reply}");
        assert!(reply.contains("`*` differs from the default"), "marker hint expected: {reply}");
        for key in SET_KEYS {
            assert!(reply.contains(&format!("{key} = ")), "key {key} missing: {reply}");
        }
        // Customized keys carry the marker ...
        assert!(reply.contains("* temperature = 0.7"), "unexpected: {reply}");
        assert!(reply.contains("* compaction_prompt = <set,"), "unexpected: {reply}");
        // ... the assignment baseline and untouched defaults do not.
        assert!(reply.contains("\nmodel = local/gemma"), "unexpected: {reply}");
        assert!(reply.contains("\nreact = off"), "unexpected: {reply}");
        assert!(reply.contains("\nstreaming = off"), "unexpected: {reply}");
        assert!(reply.contains("image_prompt = <plugin default>"), "unexpected: {reply}");
        assert!(!reply.contains("* depth ="), "untouched default must not be marked");
        // Prompts: shape only - set-or-not and size, never the text.
        let expected = format!("compaction_prompt = <set, {} chars>", long_prompt.chars().count());
        assert!(reply.contains(&expected), "unexpected: {reply}");
        assert!(!reply.contains("ário"), "prompt content leaked: {reply}");
        // One Discord message.
        assert!(reply.chars().count() <= 2000, "dump exceeds Discord's cap");
    }

    #[tokio::test]
    async fn dump_without_assignment_replies_not_assigned() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        DumpLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("dump expected to succeed");

        assert!(fixture.output.messages().last().expect("reply expected").contains("not assigned"));
    }

    #[tokio::test]
    async fn get_without_assignment_replies_not_assigned() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");

        GetLlmHandler::new(Arc::clone(&fixture.engine))
            .invoke(&command_event(Some(1)), &CommandArgs::default(), &fixture.services)
            .await
            .expect("get expected to succeed");

        assert!(fixture.output.messages().last().expect("reply expected").contains("not assigned"));
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

    /// A panic anywhere in an engine run must not silently drop a
    /// guaranteed answer: the spawned task isolates the panic and a
    /// triggered message still receives the generic fallback notice.
    #[tokio::test]
    async fn engine_panic_still_delivers_the_triggered_fallback() {
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
            Arc::new(PanickingCompletion) as Arc<dyn LlmCompletionPort>,
            Arc::new(RandRandom) as Arc<dyn RandomPort>,
            Arc::new(FakeDescriber) as Arc<dyn ImageDescriber>,
        ));
        let plugin = LlmPlugin::new(
            Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&engine),
        );
        plugin.init().expect("init expected to succeed");
        seed_config_in(&storage);

        let mut event = message_event(2, true);
        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        // The engine runs on a spawned task - yield until the fallback lands.
        for _ in 0..1000 {
            if !output.messages().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let messages = output.messages();
        assert_eq!(messages.len(), 1, "exactly the fallback was sent: {messages:?}");
        assert!(
            messages.first().is_some_and(|m| m.contains("couldn't process")),
            "unexpected messages: {messages:?}"
        );
    }

    /// The guaranteed-answer contract covers reply triggers too: a reply
    /// into the bot's conversation gets the panic fallback even though the
    /// payload alone cannot prove it - the record log re-check names the
    /// bot's recorded turn.
    #[tokio::test]
    async fn engine_panic_covers_reply_triggers_via_the_record_log() {
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
            Arc::new(PanickingCompletion) as Arc<dyn LlmCompletionPort>,
            Arc::new(RandRandom) as Arc<dyn RandomPort>,
            Arc::new(FakeDescriber) as Arc<dyn ImageDescriber>,
        ));
        let plugin = LlmPlugin::new(
            Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&engine),
        );
        plugin.init().expect("init expected to succeed");
        seed_config_in(&storage);

        // The bot's own earlier turn, recorded at send time.
        let bot_turn = ConversationRecord {
            message_id: Some(999),
            role: RecordRole::Assistant,
            author: None,
            sender_id: None,
            guild_name: None,
            content: "earlier bot answer".to_owned(),
            reply_to: None,
            captured_at: 1,
            images: Vec::new(),
        };
        storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .append(
                &records_namespace(2),
                serde_json::to_value(&bot_turn).expect("record serializes"),
            )
            .await
            .expect("append expected to succeed");

        let mut event = message_event(2, false);
        if let EventPayload::Message(payload) = &mut event.payload {
            payload.reply_to = Some(MessageId(999));
        }
        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        for _ in 0..1000 {
            if !output.messages().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let messages = output.messages();
        assert_eq!(messages.len(), 1, "the reply trigger must get the fallback: {messages:?}");
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

    /// The flood valve: a channel at its cap sheds further messages (no
    /// capture, no engine run); other channels keep their own budget.
    #[test]
    fn channel_permits_bound_outstanding_runs_per_channel() {
        let permits = ChannelPermits::new();
        let origin = Origin {
            platform: Platform::Discord,
            guild_id: Some(GuildId(1)),
            channel_id: ChannelIdModel(2),
            user_id: UserId(3),
            message_id: None,
            reply_token: None,
        };
        let gate = permits.permit_for(&origin);
        // Hold the permits - a dropped permit releases its slot instantly.
        let held: Vec<_> =
            (0..MAX_PENDING_RUNS_PER_CHANNEL).filter_map(|_| gate.try_acquire().ok()).collect();
        assert_eq!(held.len(), MAX_PENDING_RUNS_PER_CHANNEL);
        assert!(gate.try_acquire().is_err(), "cap expected");

        let other = Origin { channel_id: ChannelIdModel(9), ..origin };
        assert!(permits.permit_for(&other).try_acquire().is_ok());
    }

    /// A full backlog sheds the message at intake: not captured, no engine
    /// run, pipeline unaffected.
    #[tokio::test]
    async fn full_backlog_sheds_the_message() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);
        let origin = Origin {
            platform: Platform::Discord,
            guild_id: Some(GuildId(1)),
            channel_id: ChannelIdModel(2),
            user_id: UserId(3),
            message_id: Some(MessageId(4)),
            reply_token: None,
        };
        let gate = plugin.channel_permits.permit_for(&origin);
        // Hold the permits so the backlog is genuinely full when `pre` runs.
        let held: Vec<_> =
            (0..MAX_PENDING_RUNS_PER_CHANNEL).filter_map(|_| gate.try_acquire().ok()).collect();
        assert_eq!(held.len(), MAX_PENDING_RUNS_PER_CHANNEL);

        let mut event = message_event(2, true);
        let next = plugin.pre(&mut event, &fixture.services).await;

        assert!(matches!(next, Next::Continue));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(stored_records(&fixture).await.is_empty(), "shed messages must not be captured");
    }

    /// `stop` cancels admission: a stopped plugin spawns no engine runs -
    /// the shutdown gate.
    #[tokio::test]
    async fn stopped_plugin_admits_no_engine_runs() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);
        plugin.stop().expect("stop expected to succeed");

        let mut event = message_event(2, true);
        let next = plugin.pre(&mut event, &fixture.services).await;

        assert!(matches!(next, Next::Continue));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            stored_records(&fixture).await.is_empty(),
            "a stopped plugin must not run the engine"
        );
    }

    async fn stored_records(fixture: &Fixture) -> Vec<ConversationRecord> {
        fixture
            .storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .list_after(&records_namespace(2), 0, 100)
            .await
            .expect("records readable")
            .into_iter()
            .filter_map(|stored| serde_json::from_value(stored.payload).ok())
            .collect()
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
                    sender_id: None,
                    guild_name: None,
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
            sender_id: None,
            guild_name: None,
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

    fn prompt_handler(fixture: &Fixture) -> SetPromptLlmHandler {
        SetPromptLlmHandler::new(
            ChannelLocks::new(),
            Arc::clone(&fixture.engine),
            reqwest::Client::new(),
            10_000,
        )
    }

    /// An override is reported as such - and its head preview + fingerprint
    /// let an admin verify the active version without printing the whole
    /// prompt.
    #[tokio::test]
    async fn status_shows_the_channel_prompt_override() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        prompt_handler(&fixture)
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("kind".to_owned(), "system".to_owned()),
                    ("prompt".to_owned(), "You are a pirate.".to_owned()),
                ]),
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
    async fn prompt_sets_clears_and_reads_back_system_prompt() {
        let (plugin, fixture) = fixture();
        plugin.init().expect("init expected to succeed");
        seed_config_in(&fixture.storage);

        prompt_handler(&fixture)
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("kind".to_owned(), "system".to_owned()),
                    ("prompt".to_owned(), "You are a pirate.".to_owned()),
                ]),
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

        // No text arguments: the command reads the current value back.
        prompt_handler(&fixture)
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![("kind".to_owned(), "system".to_owned())]),
                &fixture.services,
            )
            .await
            .expect("read-back expected to succeed");
        assert!(
            fixture
                .output
                .messages()
                .iter()
                .any(|m| m.contains("`system_prompt`: `You are a pirate.`")),
            "read-back expected, got: {:?}",
            fixture.output.messages()
        );

        prompt_handler(&fixture)
            .invoke(
                &command_event(Some(1)),
                &CommandArgs(vec![
                    ("kind".to_owned(), "system".to_owned()),
                    ("prompt".to_owned(), "clear".to_owned()),
                ]),
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
