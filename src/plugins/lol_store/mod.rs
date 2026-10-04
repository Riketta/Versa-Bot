//! LoL store tracker: announces store events (sales, new skins, Mythic Shop
//! rotations, Your Shop start) from the locally running League client to a
//! per-guild assigned channel. Account data is deliberately out of scope -
//! the logged-in client is only the API key.
//!
//! A `PluginPort`-only plugin (no middleware hook): its driver is a
//! scheduler job, not chat events. Own hexagon inside: the engine depends on
//! the [`lcu::LcuPort`] boundary, [`lcu::LcuClient`] is the one adapter.
//!
//! Disabled by default at both levels: without a configured `[lol]` section
//! the engine never schedules, and per guild nothing announces until
//! `/lol_store_enable` + `/lol_store_assign`. The poll cadence and feature
//! flags are startup-only ([`EngineSettings`]).

mod commands;
mod diff;
mod engine;
mod events;
mod format;
pub mod lcu;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::kernel::{
    models::PluginError,
    plugin_ports::{
        AccessTier, ArgDescriptor, ArgKind, CommandDescriptor, CommandRegistryPort, EventBusPort,
        JobHandle, PluginPort, SchedulerPort,
    },
};

pub use commands::{
    AssignHandler, ClientStatusHandler, DisableHandler, DumpHandler, EnableHandler, RoleHandler,
    UnassignHandler,
};
pub use diff::LastSeen;
pub use engine::{
    AnnounceFlags, CONFIG_KEY, EngineSettings, GuildConfig, NAMESPACE, StatusSnapshot, StoreEngine,
};
pub use lcu::{LcuClient, LcuPort};

/// The plugin facade: command registration (`init`), poll scheduling
/// (`start`), cancellation (`stop`).
pub struct LolStorePlugin<B: EventBusPort> {
    registry: Arc<dyn CommandRegistryPort>,
    scheduler: Arc<dyn SchedulerPort>,
    engine: Arc<StoreEngine<B>>,
    job: Mutex<Option<JobHandle>>,
    stopped: AtomicBool,
}

impl<B: EventBusPort> LolStorePlugin<B> {
    #[must_use]
    pub fn new(
        registry: Arc<dyn CommandRegistryPort>,
        scheduler: Arc<dyn SchedulerPort>,
        engine: Arc<StoreEngine<B>>,
    ) -> Self {
        Self { registry, scheduler, engine, job: Mutex::new(None), stopped: AtomicBool::new(false) }
    }
}

impl<B: EventBusPort> PluginPort for LolStorePlugin<B> {
    fn name(&self) -> &'static str {
        "lol_store"
    }

    fn init(&self) -> Result<(), PluginError> {
        let registry = &self.registry;
        let register =
            |descriptor: CommandDescriptor,
             handler: Arc<dyn crate::kernel::plugin_ports::CommandHandler>| {
                registry.register(descriptor, handler);
            };

        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_client_status".to_owned(),
                description: "Show League client link state and store watcher status".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(ClientStatusHandler { engine: Arc::clone(&self.engine) }),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_enable".to_owned(),
                description: "Enable LoL store event tracking for this guild".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Admin),
                guild_only: true,
            },
            Arc::new(EnableHandler),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_disable".to_owned(),
                description: "Disable LoL store event tracking for this guild".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Admin),
                guild_only: true,
            },
            Arc::new(DisableHandler),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_assign".to_owned(),
                description: "Post LoL store events in this channel (run it there)".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(AssignHandler),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_unassign".to_owned(),
                description: "Stop posting LoL store events in this guild".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(UnassignHandler),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_role".to_owned(),
                description: "Tag this role on store announcements (omit to clear)".to_owned(),
                arguments: vec![ArgDescriptor {
                    name: "role".to_owned(),
                    description: "Role to tag on announcements; omit to clear".to_owned(),
                    required: false,
                    kind: ArgKind::Role,
                    choices: None,
                }],
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(RoleHandler),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_dump".to_owned(),
                description: "Post the latest LoL store update summary in this channel".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(DumpHandler { engine: Arc::clone(&self.engine) }),
        );
        Ok(())
    }

    fn start(&self) -> Result<(), PluginError> {
        self.stopped.store(false, Ordering::Release);
        let settings = self.engine.settings();
        if settings.poll.is_zero() {
            tracing::info!("lol store plugin has no poll interval - watcher disabled");
            return Ok(());
        }
        let job = self.scheduler.schedule(
            "lol_store_poll",
            settings.poll,
            Arc::new(engine::PollJob { engine: Arc::clone(&self.engine) }),
        );
        *self.job.lock() = Some(job);
        tracing::info!(poll_secs = settings.poll.as_secs(), "lol store watcher scheduled");
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        self.stopped.store(true, Ordering::Release);
        if let Some(job) = self.job.lock().take() {
            job.cancel();
        }
        Ok(())
    }
}

impl AnnounceFlags {
    /// Every tracker announcing (the config default).
    #[must_use]
    pub fn all_on() -> Self {
        Self { sales: true, new_skins: true, mythic_rotation: true, yourshop: true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{
        CommandPayload, EventKind, EventPayload, GuildId, Origin, Platform, UserId,
    };
    use crate::kernel::plugin_ports::{CommandArgs, CommandHandler};
    use crate::kernel::services::KernelServices;
    use crate::kernel::spi_ports::{ChatOutputPort, StoragePort};
    use crate::test_support::{
        InMemoryStorage, NoopScheduler, RecordingChatOutput, RecordingChatOutputFactory,
        assert_descriptions_fit_discord,
    };
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::Duration;

    /// Minimal registry double capturing descriptors and handlers.
    #[derive(Default)]
    struct CapturingRegistry {
        descriptors: parking_lot::Mutex<Vec<CommandDescriptor>>,
    }

    impl CommandRegistryPort for CapturingRegistry {
        fn register(
            &self,
            descriptor: CommandDescriptor,
            _handler: Arc<dyn crate::kernel::plugin_ports::CommandHandler>,
        ) {
            self.descriptors.lock().push(descriptor);
        }

        fn lookup(
            &self,
            _name: &str,
        ) -> Option<Arc<dyn crate::kernel::plugin_ports::CommandHandler>> {
            None
        }

        fn descriptor(&self, _name: &str) -> Option<CommandDescriptor> {
            None
        }

        fn descriptors(&self) -> Vec<CommandDescriptor> {
            self.descriptors.lock().clone()
        }
    }

    #[test]
    fn init_registers_the_six_commands_within_discord_limits() {
        let registry = Arc::new(CapturingRegistry::default());
        let plugin = LolStorePlugin::new(
            Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
            Arc::new(NoopScheduler) as Arc<dyn SchedulerPort>,
            disabled_engine(),
        );
        plugin.init().expect("init expected");
        let descriptors = registry.descriptors();
        let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "lol_client_status",
                "lol_store_enable",
                "lol_store_disable",
                "lol_store_assign",
                "lol_store_unassign",
                "lol_store_role",
                "lol_store_dump",
            ]
        );
        assert_descriptions_fit_discord(&descriptors);
        for descriptor in &descriptors {
            assert!(descriptor.guild_only, "{} must be guild-only", descriptor.name);
            assert!(descriptor.required_tier.is_some());
        }
    }

    fn disabled_engine() -> Arc<StoreEngine<RecordingBus>> {
        Arc::new(StoreEngine::new(
            Arc::new(OfflineLcu),
            Arc::new(InMemoryStorage::new()) as Arc<dyn crate::kernel::spi_ports::StoragePort>,
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecordingBus::default(),
            EngineSettings { poll: Duration::ZERO, flags: AnnounceFlags::all_on() },
        ))
    }

    /// Bus double accepting publications without subscribers.
    #[derive(Default)]
    struct RecordingBus;

    impl EventBusPort for RecordingBus {
        fn publish(&self, _event: Arc<dyn crate::kernel::models::Event>) {}

        fn subscribe<E: crate::kernel::models::Event + 'static>(
            &self,
            _handler: Arc<dyn crate::kernel::plugin_ports::EventHandler<E>>,
        ) -> crate::kernel::plugin_ports::EventBusSubscription {
            crate::kernel::plugin_ports::EventBusSubscription::new(Arc::new(|| {}))
        }
    }

    /// LCU double that is always offline (engine commands never fetch).
    struct OfflineLcu;

    #[async_trait]
    impl lcu::LcuPort for OfflineLcu {
        async fn catalog(&self) -> Result<Vec<lcu::CatalogItem>, lcu::LcuError> {
            Err(lcu::LcuError::Offline("test".to_owned()))
        }

        async fn sales(&self) -> Result<Vec<lcu::Sale>, lcu::LcuError> {
            Err(lcu::LcuError::Offline("test".to_owned()))
        }

        async fn rotations(&self) -> Result<Vec<lcu::RotationStore>, lcu::LcuError> {
            Err(lcu::LcuError::Offline("test".to_owned()))
        }

        async fn yourshop_status(&self) -> Result<lcu::YourShopStatus, lcu::LcuError> {
            Err(lcu::LcuError::Offline("test".to_owned()))
        }

        async fn champion_names(&self) -> Result<Vec<lcu::ChampionEntry>, lcu::LcuError> {
            Err(lcu::LcuError::Offline("test".to_owned()))
        }
    }

    struct CommandFixture {
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        services: KernelServices,
        event: crate::kernel::models::RequestContext,
    }

    async fn command_fixture(guilded: bool) -> CommandFixture {
        let storage = Arc::new(InMemoryStorage::new());
        let output = RecordingChatOutput::new();
        let origin = Origin {
            platform: Platform::Discord,
            guild_id: guilded.then(|| GuildId(42)),
            channel_id: crate::kernel::models::ChannelId(555),
            user_id: UserId(1),
            message_id: None,
            reply_token: Some("token".to_owned()),
        };
        let services = KernelServices {
            chat_output: output.clone() as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: guilded.then(|| storage.guild_scoped(Platform::Discord, GuildId(42))),
        };
        let event = crate::kernel::models::RequestContext {
            origin,
            kind: EventKind::CommandInvoked,
            payload: EventPayload::Command(CommandPayload {
                name: "x".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        };
        CommandFixture { storage, output, services, event }
    }

    async fn stored_config(fixture: &CommandFixture) -> Option<GuildConfig> {
        fixture
            .storage
            .guild_scoped(Platform::Discord, GuildId(42))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("config read expected")
            .map(|raw| serde_json::from_value(raw).expect("config shape expected"))
    }

    #[tokio::test]
    async fn enable_and_disable_flip_the_guild_flag() {
        let f = command_fixture(true).await;
        EnableHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        assert_eq!(stored_config(&f).await.map(|config| config.enabled), Some(true));
        DisableHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        assert_eq!(stored_config(&f).await.map(|config| config.enabled), Some(false));
        assert_eq!(f.output.messages().len(), 2, "both commands replied");
    }

    #[tokio::test]
    async fn assign_and_unassign_manage_only_the_channel() {
        let f = command_fixture(true).await;
        AssignHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        let config = stored_config(&f).await.expect("config expected");
        assert_eq!(config.channel_id.as_deref(), Some("555"));
        assert!(!config.enabled, "assigning must not enable");

        UnassignHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        let config = stored_config(&f).await.expect("config expected");
        assert_eq!(config.channel_id, None);
    }

    #[tokio::test]
    async fn role_command_sets_and_clears_the_mention_role() {
        let f = command_fixture(true).await;
        // With the role: stored, and orthogonal to enable/channel.
        let args = CommandArgs(vec![("role".to_owned(), "999".to_owned())]);
        RoleHandler.invoke(&f.event, &args, &f.services).await.expect("invoke");
        let config = stored_config(&f).await.expect("config expected");
        assert_eq!(config.role_id.as_deref(), Some("999"));
        assert!(!config.enabled);
        assert_eq!(config.channel_id, None);

        // Without the role: cleared, everything else untouched.
        f.storage
            .guild_scoped(Platform::Discord, GuildId(42))
            .set(
                NAMESPACE,
                CONFIG_KEY,
                serde_json::json!({ "enabled": true, "channel_id": "555", "role_id": "999" }),
            )
            .await
            .expect("config write expected");
        RoleHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        let config = stored_config(&f).await.expect("config expected");
        assert_eq!(config.role_id, None);
        assert!(config.enabled, "clearing the role must not touch the switch");
        assert_eq!(config.channel_id.as_deref(), Some("555"));
    }

    #[tokio::test]
    async fn role_command_rejects_non_role_values() {
        let f = command_fixture(true).await;
        let args = CommandArgs(vec![("role".to_owned(), "not-a-role".to_owned())]);
        RoleHandler.invoke(&f.event, &args, &f.services).await.expect("invoke");
        let messages = f.output.messages();
        let first = messages.first().expect("reply expected");
        assert!(first.contains("must be a role"));
        assert!(stored_config(&f).await.is_none(), "nothing stored on rejection");
    }

    #[tokio::test]
    async fn config_commands_outside_a_guild_reply_guild_only() {
        let f = command_fixture(false).await;
        EnableHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        let messages = f.output.messages();
        let first = messages.first().expect("reply expected");
        assert!(first.contains("only works inside a server"));
    }

    #[tokio::test]
    async fn dump_without_announcements_replies_the_placeholder() {
        let f = command_fixture(true).await;
        let engine = disabled_engine();
        DumpHandler { engine }
            .invoke(&f.event, &Default::default(), &f.services)
            .await
            .expect("invoke");
        assert!(
            f.output
                .messages()
                .first()
                .expect("reply expected")
                .contains("No store update has been announced yet.")
        );
    }

    #[tokio::test]
    async fn client_status_renders_the_engine_snapshot() {
        let f = command_fixture(true).await;
        let engine = disabled_engine();
        ClientStatusHandler { engine }
            .invoke(&f.event, &Default::default(), &f.services)
            .await
            .expect("invoke");
        let messages = f.output.messages();
        let reply = messages.first().expect("reply expected");
        assert!(reply.contains("League client: offline"));
        assert!(reply.contains("Last poll: never"));
        assert!(reply.contains("Announce: sales=on"));
    }
}
