//! LoL store tracker: announces store events (sales, new skins, Mythic Shop
//! rotations, Your Shop start) from the locally running League client to a
//! per-guild assigned channel. Account data is deliberately out of scope -
//! the logged-in client is only the API key.
//!
//! A `PluginPort`-only plugin (no middleware hook): its driver is a
//! scheduler job, not chat events. Own hexagon inside: the engine depends on
//! the [`lcu::LcuPort`] boundary, [`lcu::LcuClient`] is the one adapter.
//!
//! Disabled by default at both levels: without a configured `[lol_store]` section
//! the engine never schedules, and per guild nothing announces until
//! `/lol_store_enable` + `/lol_store_assign`. The poll cadence and feature
//! flags are startup-only ([`EngineSettings`]).

mod commands;
mod diff;
mod engine;
mod events;
mod format;
pub mod lcu;
pub mod watch;

use std::sync::Arc;

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
    UnassignHandler, UnwatchHandler, WatchHandler, WatchlistHandler,
};
pub use diff::LastSeen;
pub use engine::{AnnounceFlags, CONFIG_KEY, EngineSettings, GuildConfig, NAMESPACE, StoreEngine};
pub use lcu::{LcuClient, LcuPort};
pub use watch::{DEFAULT_GUILD_CAP, DEFAULT_USER_CAP, WATCH_KEY};

/// The plugin facade: command registration (`init`), poll scheduling
/// (`start`), cancellation (`stop`).
pub struct LolStorePlugin<B: EventBusPort> {
    registry: Arc<dyn CommandRegistryPort>,
    scheduler: Arc<dyn SchedulerPort>,
    engine: Arc<StoreEngine<B>>,
    job: Mutex<Option<JobHandle>>,
}

impl<B: EventBusPort> LolStorePlugin<B> {
    #[must_use]
    pub fn new(
        registry: Arc<dyn CommandRegistryPort>,
        scheduler: Arc<dyn SchedulerPort>,
        engine: Arc<StoreEngine<B>>,
    ) -> Self {
        Self { registry, scheduler, engine, job: Mutex::new(None) }
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
                description:
                    "Post the latest store update, or the current store state, in this channel"
                        .to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(DumpHandler { engine: Arc::clone(&self.engine) }),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_watch".to_owned(),
                description: "Watch a skin or champion for sales, Mythic Shop rotations, \
                     new releases"
                    .to_owned(),
                arguments: vec![
                    ArgDescriptor {
                        name: "target".to_owned(),
                        description: "Watch one skin or a champion's whole skin line".to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: Some(vec!["skin".to_owned(), "champion".to_owned()]),
                    },
                    ArgDescriptor {
                        name: "name".to_owned(),
                        description: "Skin name (e.g. Blood Moon Evelynn) or champion name"
                            .to_owned(),
                        required: true,
                        kind: ArgKind::String,
                        choices: None,
                    },
                    ArgDescriptor {
                        name: "kinds".to_owned(),
                        description: "What fires: sale, mythic, release, or all (default; all = \
                                     sale + mythic + release)"
                            .to_owned(),
                        required: false,
                        kind: ArgKind::String,
                        choices: Some(vec![
                            "sale".to_owned(),
                            "mythic".to_owned(),
                            "release".to_owned(),
                            "all".to_owned(),
                        ]),
                    },
                ],
                required_permission: None,
                required_tier: Some(AccessTier::User),
                guild_only: true,
            },
            Arc::new(WatchHandler { engine: Arc::clone(&self.engine) }),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_unwatch".to_owned(),
                description: "Remove a store watch by its list id, or every watch with `all`"
                    .to_owned(),
                arguments: vec![ArgDescriptor {
                    name: "what".to_owned(),
                    description: "Watch id from `/lol_store_watchlist`, or `all`".to_owned(),
                    required: true,
                    kind: ArgKind::String,
                    choices: None,
                }],
                required_permission: None,
                required_tier: Some(AccessTier::User),
                guild_only: true,
            },
            Arc::new(UnwatchHandler),
        );
        register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_store_watchlist".to_owned(),
                description: "List your store watches (private to you)".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::User),
                guild_only: true,
            },
            Arc::new(WatchlistHandler),
        );
        Ok(())
    }

    fn start(&self) -> Result<(), PluginError> {
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
        if let Some(job) = self.job.lock().take() {
            job.cancel();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{CommandPayload, EventKind, EventPayload, GuildId, Origin, UserId};
    use crate::kernel::plugin_ports::{CommandArgs, CommandHandler, Job};
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
    fn init_registers_the_ten_commands_within_discord_limits() {
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
                "lol_store_watch",
                "lol_store_unwatch",
                "lol_store_watchlist",
            ]
        );
        assert_descriptions_fit_discord(&descriptors);
        for descriptor in &descriptors {
            assert!(descriptor.guild_only, "{} must be guild-only", descriptor.name);
            assert!(descriptor.required_tier.is_some());
        }
        // Exact tiers: a regression demoting enable to User (or watches to
        // Moderator) must fail here, not in production.
        let tier_of =
            |name: &str| descriptors.iter().find(|d| d.name == name).and_then(|d| d.required_tier);
        assert_eq!(tier_of("lol_store_enable"), Some(AccessTier::Admin));
        assert_eq!(tier_of("lol_store_disable"), Some(AccessTier::Admin));
        for moderator in [
            "lol_client_status",
            "lol_store_assign",
            "lol_store_unassign",
            "lol_store_role",
            "lol_store_dump",
        ] {
            assert_eq!(tier_of(moderator), Some(AccessTier::Moderator), "{moderator}");
        }
        for user in ["lol_store_watch", "lol_store_unwatch", "lol_store_watchlist"] {
            assert_eq!(tier_of(user), Some(AccessTier::User), "{user}");
        }
    }

    fn disabled_engine() -> Arc<StoreEngine<RecordingBus>> {
        Arc::new(StoreEngine::new(
            Arc::new(OfflineLcu),
            Arc::new(InMemoryStorage::new()) as Arc<dyn crate::kernel::spi_ports::StoragePort>,
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecordingBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::ZERO,
                flags: AnnounceFlags::all_on(),
                watch_user_cap: DEFAULT_USER_CAP,
                watch_guild_cap: DEFAULT_GUILD_CAP,
            },
        ))
    }

    /// Scheduler double recording what the plugin asked for; handles are
    /// dead (like `NoopScheduler`), so cancellation mechanics stay the
    /// adapter's tested concern - this pins the plugin's scheduling call.
    #[derive(Default)]
    struct RecordingScheduler {
        jobs: std::sync::Mutex<Vec<(String, Duration)>>,
    }

    impl SchedulerPort for RecordingScheduler {
        fn schedule(&self, name: &str, interval: Duration, _job: Arc<dyn Job>) -> JobHandle {
            self.jobs.lock().expect("jobs lock").push((name.to_owned(), interval));
            JobHandle::new(Arc::new(|| {}))
        }
    }

    #[test]
    fn start_schedules_the_poll_and_stop_is_idempotent() {
        let scheduler = Arc::new(RecordingScheduler::default());
        let engine = Arc::new(StoreEngine::new(
            Arc::new(OfflineLcu),
            Arc::new(InMemoryStorage::new()) as Arc<dyn crate::kernel::spi_ports::StoragePort>,
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecordingBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(90),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: DEFAULT_USER_CAP,
                watch_guild_cap: DEFAULT_GUILD_CAP,
            },
        ));
        let plugin = LolStorePlugin::new(
            Arc::new(CapturingRegistry::default()) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
            engine,
        );

        plugin.init().expect("init expected");
        plugin.start().expect("start expected");
        assert_eq!(
            scheduler.jobs.lock().expect("jobs lock").as_slice(),
            vec![("lol_store_poll".to_owned(), Duration::from_secs(90))],
            "the poll job is scheduled under the plugin's name at the configured cadence"
        );

        plugin.stop().expect("stop expected");
        plugin.stop().expect("a second stop must be a no-op, not a panic");
        assert_eq!(scheduler.jobs.lock().expect("jobs lock").len(), 1, "stop never re-schedules");
    }

    /// A zero poll interval keeps the watcher off: `start()` must not
    /// schedule anything (the scheduler's zero-interval path is never
    /// reached).
    #[test]
    fn start_with_zero_poll_schedules_nothing() {
        let scheduler = Arc::new(RecordingScheduler::default());
        let plugin = LolStorePlugin::new(
            Arc::new(CapturingRegistry::default()) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
            disabled_engine(), // poll: Duration::ZERO
        );
        plugin.init().expect("init expected");
        plugin.start().expect("start expected");
        assert!(scheduler.jobs.lock().expect("jobs lock").is_empty());
        plugin.stop().expect("stop expected");
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

    /// LCU double that is always online with two Ahri skins in the catalog
    /// (champion 103) - enough for exact-match and ambiguous candidate
    /// flows in the watch commands. The sales list is mutable so tests can
    /// seed "currently on sale" snapshots for status-line assertions.
    struct OnlineLcu {
        sales: parking_lot::Mutex<Vec<lcu::Sale>>,
    }

    impl OnlineLcu {
        fn new() -> Self {
            Self { sales: parking_lot::Mutex::new(Vec::new()) }
        }

        fn skin_sale(item_id: u64) -> lcu::Sale {
            lcu::Sale {
                id: 1,
                item: lcu::ItemRef {
                    inventory_type: Some("CHAMPION_SKIN".to_owned()),
                    item_id: Some(item_id),
                },
                sale: lcu::SaleInfo {
                    start_date: None,
                    end_date: None,
                    prices: vec![lcu::Price { cost: Some(607), currency: Some("RP".to_owned()) }],
                },
            }
        }
    }

    #[async_trait]
    impl lcu::LcuPort for OnlineLcu {
        async fn catalog(&self) -> Result<Vec<lcu::CatalogItem>, lcu::LcuError> {
            let skin = |item_id: u64, name: &str| lcu::CatalogItem {
                item_id,
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                prices: vec![lcu::Price { cost: Some(975), currency: Some("RP".to_owned()) }],
                localizations: std::collections::BTreeMap::from([(
                    "en_US".to_owned(),
                    lcu::LocalizedText { name: Some(name.to_owned()) },
                )]),
                item_requirements: vec![lcu::ItemRef {
                    inventory_type: Some("CHAMPION".to_owned()),
                    item_id: Some(103),
                }],
            };
            Ok(vec![skin(1031, "Foxfire Ahri"), skin(1032, "Dynasty Ahri")])
        }

        async fn sales(&self) -> Result<Vec<lcu::Sale>, lcu::LcuError> {
            Ok(self.sales.lock().clone())
        }

        async fn rotations(&self) -> Result<Vec<lcu::RotationStore>, lcu::LcuError> {
            Ok(Vec::new())
        }

        async fn yourshop_status(&self) -> Result<lcu::YourShopStatus, lcu::LcuError> {
            Ok(lcu::YourShopStatus::default())
        }

        async fn champion_names(&self) -> Result<Vec<lcu::ChampionEntry>, lcu::LcuError> {
            Ok(vec![lcu::ChampionEntry { id: 103, name: Some("Ahri".to_owned()) }])
        }
    }

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
        command_fixture_with(guilded, Arc::new(InMemoryStorage::new())).await
    }

    async fn command_fixture_with(guilded: bool, storage: Arc<InMemoryStorage>) -> CommandFixture {
        let output = RecordingChatOutput::new();
        let origin = Origin {
            guild_id: guilded.then(|| GuildId(42)),
            channel_id: crate::kernel::models::ChannelId(555),
            user_id: UserId(1),
            message_id: None,
            reply_token: Some("token".to_owned()),
        };
        let services = KernelServices {
            chat_output: output.clone() as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: guilded.then(|| storage.guild_scoped("test", GuildId(42))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
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

    async fn stored_watch_doc(fixture: &CommandFixture) -> Option<serde_json::Value> {
        fixture
            .storage
            .guild_scoped("test", GuildId(42))
            .get(NAMESPACE, WATCH_KEY)
            .await
            .expect("watch doc read expected")
    }

    async fn seed_watch_doc(fixture: &CommandFixture, doc: serde_json::Value) {
        fixture
            .storage
            .guild_scoped("test", GuildId(42))
            .set(NAMESPACE, WATCH_KEY, doc)
            .await
            .expect("watch doc write expected");
    }

    /// Watch-command fixture: the fixture's guild storage is shared with
    /// the engine, and one baseline tick fills the snapshot so name search
    /// works. The catalog holds two Ahri skins - exact matches hit one,
    /// the loose prefix hits both.
    async fn watch_fixture(
        user_cap: u32,
        guild_cap: u32,
    ) -> (CommandFixture, Arc<StoreEngine<RecordingBus>>) {
        let storage = Arc::new(InMemoryStorage::new());
        let f = command_fixture_with(true, Arc::clone(&storage)).await;
        let engine = Arc::new(StoreEngine::new(
            Arc::new(OnlineLcu::new()),
            Arc::clone(&storage) as Arc<dyn crate::kernel::spi_ports::StoragePort>,
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecordingBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: user_cap,
                watch_guild_cap: guild_cap,
            },
        ));
        engine.tick().await; // silent baseline: fills the raw snapshot
        (f, engine)
    }

    async fn stored_config(fixture: &CommandFixture) -> Option<GuildConfig> {
        fixture
            .storage
            .guild_scoped("test", GuildId(42))
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
            .guild_scoped("test", GuildId(42))
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

    fn watch_args(target: &str, name: &str, kinds: Option<&str>) -> CommandArgs {
        let mut args =
            vec![("target".to_owned(), target.to_owned()), ("name".to_owned(), name.to_owned())];
        if let Some(kinds) = kinds {
            args.push(("kinds".to_owned(), kinds.to_owned()));
        }
        CommandArgs(args)
    }

    #[tokio::test]
    async fn watch_without_store_data_explains_and_stores_nothing() {
        let f = command_fixture(true).await;
        let engine = disabled_engine(); // offline: no snapshot ever
        WatchHandler { engine }
            .invoke(&f.event, &watch_args("skin", "Foxfire Ahri", None), &f.services)
            .await
            .expect("invoke");
        let first = f.output.messages().first().expect("reply expected").clone();
        assert!(first.contains("No store data yet"));
        assert!(stored_watch_doc(&f).await.is_none(), "nothing stored on failure");
    }

    #[tokio::test]
    async fn watch_resolves_a_unique_name_and_stores_the_subscription() {
        let (f, engine) = watch_fixture(20, 300).await;
        WatchHandler { engine }
            .invoke(&f.event, &watch_args("skin", "Foxfire Ahri", Some("sale")), &f.services)
            .await
            .expect("invoke");
        let first = f.output.messages().first().expect("reply expected").clone();
        assert!(first.contains("Watch #1 added"), "reply: {first}");
        assert!(first.contains("Ahri - Foxfire Ahri (sale)"));

        let doc = stored_watch_doc(&f).await.expect("watch doc expected");
        let subs = doc.get("subs").and_then(|subs| subs.as_array()).expect("subs array");
        assert_eq!(subs.len(), 1);
        let target = subs.first().expect("sub").get("target").expect("target");
        assert_eq!(target.get("type").and_then(|t| t.as_str()), Some("skin"));
        assert_eq!(target.get("item_id").and_then(|id| id.as_u64()), Some(1031));
        assert_eq!(
            subs.first().expect("sub").get("user_id").and_then(|user| user.as_str()),
            Some("1"),
            "the invoking user owns the watch"
        );
    }

    #[tokio::test]
    async fn watch_resolves_a_champion_and_reports_current_activity() {
        let (f, engine) = watch_fixture(20, 300).await;
        WatchHandler { engine }
            .invoke(&f.event, &watch_args("champion", "aHRi", None), &f.services)
            .await
            .expect("invoke");
        let first = f.output.messages().first().expect("reply expected").clone();
        assert!(first.contains("Watch #1 added: Ahri (all categories)"), "reply: {first}");
    }

    #[tokio::test]
    async fn ambiguous_names_reply_with_candidates_and_store_nothing() {
        let (f, engine) = watch_fixture(20, 300).await;
        WatchHandler { engine }
            .invoke(&f.event, &watch_args("skin", "ahri", None), &f.services)
            .await
            .expect("invoke");
        let first = f.output.messages().first().expect("reply expected").clone();
        assert!(first.contains("Several matches"), "reply: {first}");
        assert!(first.contains("`Ahri - Foxfire Ahri`"), "reply: {first}");
        assert!(first.contains("`Ahri - Dynasty Ahri`"), "reply: {first}");
        assert!(stored_watch_doc(&f).await.is_none());
    }

    #[tokio::test]
    async fn watch_enforces_the_user_and_guild_caps() {
        let (f, engine) = watch_fixture(1, 300).await;
        seed_watch_doc(
            &f,
            serde_json::json!({
                "version": 1, "next_id": 2,
                "subs": [{ "id": 1, "user_id": "1",
                    "target": { "type": "skin", "item_id": 1031,
                        "champion": "Ahri", "skin": "Foxfire Ahri" }, "kinds": "all" }]
            }),
        )
        .await;
        WatchHandler { engine }
            .invoke(&f.event, &watch_args("skin", "Dynasty Ahri", None), &f.services)
            .await
            .expect("invoke");
        let first = f.output.messages().first().expect("reply expected").clone();
        assert!(first.contains("Watch limit reached (1/1 per user)"), "reply: {first}");

        // A different user can still watch, and the guild cap is checked
        // across all users.
        let (f2, engine2) = watch_fixture(20, 1).await;
        WatchHandler { engine: engine2 }
            .invoke(&f.event, &watch_args("champion", "Ahri", None), &f2.services)
            .await
            .expect("invoke");
        let (f3, engine3) = watch_fixture(20, 1).await;
        seed_watch_doc(
            &f3,
            serde_json::json!({
                "version": 1, "next_id": 2,
                "subs": [{ "id": 1, "user_id": "999",
                    "target": { "type": "champion", "champion_id": 103, "champion": "Ahri" },
                    "kinds": "all" }]
            }),
        )
        .await;
        WatchHandler { engine: engine3 }
            .invoke(&f3.event, &watch_args("skin", "Foxfire Ahri", None), &f3.services)
            .await
            .expect("invoke");
        let reply = f3.output.messages().first().expect("reply expected").clone();
        assert!(reply.contains("guild reached its watch limit"), "reply: {reply}");
        // The non-cap path: a different user still subscribes, and the doc
        // gains a second watch (the guild-cap fixture runs user 1 with cap
        // 1, so this insert is the first for its user).
        let reply2 = f2.output.messages().first().expect("reply expected").clone();
        assert!(reply2.contains("Watch #1 added"), "reply: {reply2}");
        let doc2 = stored_watch_doc(&f2).await.expect("doc expected");
        assert_eq!(doc2.get("subs").and_then(|subs| subs.as_array()).map(Vec::len), Some(1));
    }

    #[tokio::test]
    async fn unwatch_and_watchlist_manage_only_own_watches() {
        let f = command_fixture(true).await;
        seed_watch_doc(
            &f,
            serde_json::json!({
                "version": 1, "next_id": 3,
                "subs": [
                    { "id": 1, "user_id": "1",
                      "target": { "type": "skin", "item_id": 1031,
                          "champion": "Ahri", "skin": "Foxfire Ahri" }, "kinds": "sale" },
                    { "id": 2, "user_id": "1",
                      "target": { "type": "champion", "champion_id": 103, "champion": "Ahri" },
                      "kinds": "all" },
                    { "id": 3, "user_id": "2",
                      "target": { "type": "champion", "champion_id": 103, "champion": "Ahri" },
                      "kinds": "all" }
                ]
            }),
        )
        .await;

        // Removing another user's watch by id is a no-op with a hint.
        let args = CommandArgs(vec![("what".to_owned(), "3".to_owned())]);
        UnwatchHandler.invoke(&f.event, &args, &f.services).await.expect("invoke");
        let first = f.output.messages().first().expect("reply expected").clone();
        assert!(first.contains("No watch #3 of yours"), "reply: {first}");

        // Watchlist shows only the invoker's watches.
        WatchlistHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        let list = f.output.messages().last().expect("reply expected").clone();
        assert!(list.contains("#1 Ahri - Foxfire Ahri (sale)"), "list: {list}");
        assert!(list.contains("#2 Ahri (all categories)"));
        assert!(!list.contains("#3"));

        // A removal names what went away.
        let args = CommandArgs(vec![("what".to_owned(), "1".to_owned())]);
        UnwatchHandler.invoke(&f.event, &args, &f.services).await.expect("invoke");
        let reply = f.output.messages().last().expect("reply expected").clone();
        assert!(reply.contains("Watch #1 removed: Ahri - Foxfire Ahri."), "reply: {reply}");

        // `all` clears only the invoker's watches, naming each one.
        let args = CommandArgs(vec![("what".to_owned(), "all".to_owned())]);
        UnwatchHandler.invoke(&f.event, &args, &f.services).await.expect("invoke");
        let reply = f.output.messages().last().expect("reply expected").clone();
        assert!(reply.contains("Removed 1 watch(es):"), "reply: {reply}");
        assert!(reply.contains("- #2 Ahri (all categories)"), "reply: {reply}");
        let doc = stored_watch_doc(&f).await.expect("doc expected");
        let subs = doc.get("subs").and_then(|subs| subs.as_array()).expect("subs array");
        assert_eq!(subs.len(), 1, "only user 2's watch survives");
    }

    /// A corrupted watch document is moved aside (recoverable) before a
    /// mutation overwrites it - the fresh document repairs the commands
    /// without silently destroying whatever the corruption held.
    #[tokio::test]
    async fn unreadable_watch_doc_is_quarantined_before_overwrite() {
        let (f, engine) = watch_fixture(20, 300).await;
        let garbage = serde_json::json!({ "next_id": "not-a-number" });
        seed_watch_doc(&f, garbage.clone()).await;

        WatchHandler { engine }
            .invoke(&f.event, &watch_args("skin", "Foxfire Ahri", None), &f.services)
            .await
            .expect("invoke");

        let doc = stored_watch_doc(&f).await.expect("fresh doc expected");
        assert_eq!(
            doc.get("subs").and_then(|subs| subs.as_array()).map(Vec::len),
            Some(1),
            "the fresh doc carries the new watch: {doc}"
        );
        let recovered = f
            .storage
            .guild_scoped("test", GuildId(42))
            .get(NAMESPACE, crate::plugins::lol_store::commands::WATCH_RECOVERY_KEY)
            .await
            .expect("recovery read expected");
        assert_eq!(recovered.as_ref(), Some(&garbage), "the corruption was preserved");
    }

    #[tokio::test]
    async fn watch_commands_outside_a_guild_reply_guild_only() {
        let f = command_fixture(false).await;
        let engine = disabled_engine();
        WatchHandler { engine }
            .invoke(&f.event, &watch_args("skin", "Foxfire Ahri", None), &f.services)
            .await
            .expect("invoke");
        UnwatchHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        WatchlistHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        let messages = f.output.messages();
        assert_eq!(messages.len(), 3, "every watch command replied");
        assert!(messages.iter().all(|m| m.contains("only works inside a server")));
    }

    /// Every guild-only command self-guards outside a guild - not just the
    /// ones that happened to get their own DM test.
    #[tokio::test]
    async fn all_guild_only_commands_reject_outside_a_guild() {
        let f = command_fixture(false).await;
        DisableHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        UnassignHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        RoleHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        UnwatchHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        WatchlistHandler.invoke(&f.event, &Default::default(), &f.services).await.expect("invoke");
        let engine = disabled_engine();
        ClientStatusHandler { engine: Arc::clone(&engine) }
            .invoke(&f.event, &Default::default(), &f.services)
            .await
            .expect("invoke");
        DumpHandler { engine }
            .invoke(&f.event, &Default::default(), &f.services)
            .await
            .expect("invoke");
        let messages = f.output.messages();
        assert_eq!(messages.len(), 7, "every command replied: {messages:?}");
        assert!(messages.iter().all(|m| m.contains("only works inside a server")));
    }

    /// Invalid arguments are rejected before the store snapshot is touched,
    /// and nothing is stored.
    #[tokio::test]
    async fn watch_rejects_invalid_arguments_before_searching() {
        let (f, engine) = watch_fixture(20, 300).await;
        for (args, _expected) in [
            (watch_args("tree", "Ahri", None), "target must be"),
            (watch_args("skin", "Ahri", Some("banana")), "kinds must be one of"),
            (watch_args("skin", "   ", None), "Give a name to watch"),
        ] {
            WatchHandler { engine: Arc::clone(&engine) }
                .invoke(&f.event, &args, &f.services)
                .await
                .expect("invoke");
        }
        let messages = f.output.messages();
        assert_eq!(messages.len(), 3);
        assert!(messages.iter().any(|m| m.contains("target must be")));
        assert!(messages.iter().any(|m| m.contains("kinds must be one of")));
        assert!(messages.iter().any(|m| m.contains("Give a name to watch")));
        assert!(stored_watch_doc(&f).await.is_none(), "nothing stored on rejection");
    }

    /// The subscribe confirmation reports current activity from the last
    /// snapshot ("currently on sale"), so a watcher knows the immediate
    /// situation - the watched skin's active sale shows up even though it
    /// will never fire an edge (it is already on sale).
    #[tokio::test]
    async fn watch_confirmation_reports_current_activity() {
        let storage = Arc::new(InMemoryStorage::new());
        let f = command_fixture_with(true, Arc::clone(&storage)).await;
        let lcu = OnlineLcu::new();
        *lcu.sales.lock() = vec![OnlineLcu::skin_sale(1031)];
        let engine = Arc::new(StoreEngine::new(
            Arc::new(lcu),
            Arc::clone(&storage) as Arc<dyn crate::kernel::spi_ports::StoragePort>,
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecordingBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: DEFAULT_USER_CAP,
                watch_guild_cap: DEFAULT_GUILD_CAP,
            },
        ));
        engine.tick().await; // baseline captures the active sale

        WatchHandler { engine }
            .invoke(&f.event, &watch_args("skin", "Foxfire Ahri", None), &f.services)
            .await
            .expect("invoke");
        let reply = f.output.messages().first().expect("reply expected").clone();
        assert!(reply.contains("Watch #1 added"), "reply: {reply}");
        assert!(reply.contains("- currently on sale"), "reply: {reply}");
    }

    #[tokio::test]
    async fn dump_without_a_snapshot_replies_the_placeholder() {
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
                .contains("no store snapshot is available")
        );
    }

    /// A fresh launch has announced nothing yet - the dump falls back to
    /// the current store snapshot instead of the placeholder.
    #[tokio::test]
    async fn dump_on_a_fresh_launch_posts_the_current_store() {
        let storage = Arc::new(InMemoryStorage::new());
        let f = command_fixture_with(true, Arc::clone(&storage)).await;
        let lcu = OnlineLcu::new();
        *lcu.sales.lock() = vec![OnlineLcu::skin_sale(1031)];
        let engine = Arc::new(StoreEngine::new(
            Arc::new(lcu),
            Arc::clone(&storage) as Arc<dyn crate::kernel::spi_ports::StoragePort>,
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecordingBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: DEFAULT_USER_CAP,
                watch_guild_cap: DEFAULT_GUILD_CAP,
            },
        ));
        engine.tick().await; // silent baseline

        DumpHandler { engine }
            .invoke(&f.event, &Default::default(), &f.services)
            .await
            .expect("invoke");
        let reply = f.output.messages().first().expect("reply expected").clone();
        assert!(reply.contains("LoL Store - current state"), "reply: {reply}");
        assert!(reply.contains("New sales"), "reply: {reply}");
    }

    /// Once an update has been announced, the dump replays that
    /// announcement - the current-store fallback must not shadow it.
    #[tokio::test]
    async fn dump_prefers_the_last_announcement_over_the_current_store() {
        let storage = Arc::new(InMemoryStorage::new());
        let f = command_fixture_with(true, Arc::clone(&storage)).await;
        let lcu = Arc::new(OnlineLcu::new());
        *lcu.sales.lock() = vec![OnlineLcu::skin_sale(1031)];
        let engine = Arc::new(StoreEngine::new(
            Arc::clone(&lcu) as Arc<dyn crate::plugins::lol_store::lcu::LcuPort>,
            Arc::clone(&storage) as Arc<dyn crate::kernel::spi_ports::StoragePort>,
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecordingBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: DEFAULT_USER_CAP,
                watch_guild_cap: DEFAULT_GUILD_CAP,
            },
        ));
        engine.tick().await; // silent baseline

        // A second, distinct sale (the helper hardcodes id 1) produces a
        // real delta and a stored announcement.
        lcu.sales.lock().push(lcu::Sale {
            id: 2,
            item: lcu::ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(1032),
            },
            sale: lcu::SaleInfo {
                start_date: None,
                end_date: None,
                prices: vec![lcu::Price { cost: Some(607), currency: Some("RP".to_owned()) }],
            },
        });
        engine.tick().await;

        DumpHandler { engine }
            .invoke(&f.event, &Default::default(), &f.services)
            .await
            .expect("invoke");
        let reply = f.output.messages().last().expect("reply expected").clone();
        assert!(reply.contains("LoL Store - latest update"), "reply: {reply}");
        assert!(!reply.contains("LoL Store - current state"), "reply: {reply}");
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
