use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use config::{Config, Environment, File};
use serenity::all::{ClientBuilder, GatewayIntents, Http, HttpBuilder};

use versa_bot::infrastructure::{
    Configuration, PollingConfigWatcher,
    inbound_adapters::{DiscordGatewayAdapter, SerenityChatOutputFactory},
    observability,
    outbound_adapters::{DiscordCommandRegistrar, SerenityPresence, SqlxStorage},
    plugin_adapters::{InMemoryCommandRegistry, InMemoryEventBus, TokioScheduler},
};
use versa_bot::kernel::{
    plugin_ports::{CommandRegistryPort, Job, MiddlewarePluginPort, PluginPort, SchedulerPort},
    services::KernelService,
    spi_ports::{
        ChatOutputFactoryPort, ConfigChangeHandler, ConfigPort, PresencePort, StoragePort,
    },
};
use versa_bot::plugins::audit::AuditLogPlugin;
use versa_bot::plugins::auth::AuthPlugin;
use versa_bot::plugins::command::CommandPlugin;
use versa_bot::plugins::llm::{
    ChatEngine, LlmCompletionPort, LlmPlugin, LlmSettings, OpenAiCompatibleAdapter, RandRandom,
    RandomPort,
};
use versa_bot::plugins::status::{StatusRotatorPlugin, StatusSettings};
use versa_bot::plugins::tracker::UserActivityTrackerPlugin;

#[tokio::main]
async fn main() -> ExitCode {
    let config = load_config().expect("config expected to exist and be valid");

    // Sentry/GlitchTip endpoint is DSN-driven; absent DSN means stdout only.
    // The guard must outlive the whole run - bound at `main`'s top level.
    let _sentry_guard = observability::init(
        config.debug,
        config.sentry.as_ref().map(|s| s.dsn.as_str()),
        config.sentry.as_ref().and_then(|s| s.environment.as_deref()),
    );

    // Application id: required by Discord HTTP calls that are not authorized
    // by the bot token alone (command registration, interaction followups).
    let bootstrap = build_http(&config.discord.token, config.discord.proxy.clone(), None);
    let app_id = bootstrap
        .get_current_application_info()
        .await
        .expect("application info expected to be reachable")
        .id
        .get();

    let storage = Arc::new(
        SqlxStorage::connect(&config.storage.url)
            .await
            .expect("storage expected to connect and migrate"),
    );

    let registry = Arc::new(InMemoryCommandRegistry::new());
    let event_bus = InMemoryEventBus::new();

    // Chain order = registration order: auth gates everything below it.
    let auth = Arc::new(AuthPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>));
    let command =
        Arc::new(CommandPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>));
    // The bus is kernel-owned; each plugin receives its own clone at
    // construction (same instance, per the cardinality rule). The registry is
    // shared the same way: the tracker declares its commands in `init()`.
    let tracker = Arc::new(UserActivityTrackerPlugin::new(
        event_bus.clone(),
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
    ));
    // LLM chat plugin: the conversation engine is live. The provider layer
    // starts empty until the [llm] config section is wired here - channels
    // can already be assigned; completions fail with a clear error until
    // providers are declared. Random replies use the plain RNG adapter;
    // swapping in the deck-style generator is a one-argument change.
    let llm_settings = Arc::new(LlmSettings::default());
    let llm_adapter = OpenAiCompatibleAdapter::from_settings(Arc::clone(&llm_settings))
        .expect("llm provider settings expected to configure cleanly");
    let llm_engine = Arc::new(ChatEngine::new(
        llm_settings,
        Arc::new(llm_adapter) as Arc<dyn LlmCompletionPort>,
        Arc::new(RandRandom) as Arc<dyn RandomPort>,
    ));
    let llm =
        Arc::new(LlmPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>, llm_engine));
    // Bus-only plugin: in `plugins` for lifecycle, never in the middleware
    // chain - it reacts to derived events, not to raw inbound ones.
    let audit = Arc::new(AuditLogPlugin::new(event_bus.clone()));

    // Kernel scheduling service + presence: the status rotator's drives.
    let scheduler = Arc::new(TokioScheduler::new());
    let (presence, gateway_context) = SerenityPresence::new();
    let presence = Arc::new(presence);

    // Status rotator: always registered - `[status]` changes are applied at
    // runtime; an absent/invalid section just means it starts disabled.
    let status_plugin = Arc::new(StatusRotatorPlugin::new(
        Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
        Arc::clone(&presence) as Arc<dyn PresencePort>,
        status_settings(&config),
    ));

    // Config hot reload: a polling watcher re-reads file+env configuration
    // and applies hot-reloadable sections without a restart. The task dies
    // with the process on shutdown - no explicit cancellation needed.
    let watcher = Arc::new(PollingConfigWatcher::new(load_config));
    watcher.seed(config.clone());
    watcher.subscribe(Arc::new(StatusSettingsReloader { plugin: Arc::clone(&status_plugin) }));
    scheduler.schedule(
        "config_watcher",
        Duration::from_secs(5),
        Arc::new(ConfigWatchJob { watcher: Arc::clone(&watcher) }),
    );

    let plugins: Vec<Arc<dyn PluginPort>> = vec![
        Arc::clone(&auth) as Arc<dyn PluginPort>,
        Arc::clone(&command) as Arc<dyn PluginPort>,
        Arc::clone(&tracker) as Arc<dyn PluginPort>,
        Arc::clone(&llm) as Arc<dyn PluginPort>,
        Arc::clone(&audit) as Arc<dyn PluginPort>,
        Arc::clone(&status_plugin) as Arc<dyn PluginPort>,
    ];

    let kernel = Arc::new(
        KernelService::builder()
            .plugins(plugins)
            .middleware(vec![
                Arc::clone(&auth) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&command) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&tracker) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&llm) as Arc<dyn MiddlewarePluginPort>,
            ])
            .event_bus(event_bus)
            .chat_output_factory(Arc::new(SerenityChatOutputFactory::new(build_http(
                &config.discord.token,
                config.discord.proxy.clone(),
                Some(app_id),
            ))) as Arc<dyn ChatOutputFactoryPort>)
            .storage(Arc::clone(&storage) as Arc<dyn StoragePort>)
            .build(),
    );

    kernel.boot().expect("kernel boot failed");

    // Discord specifics: publish the registry as global slash commands.
    let registrar = DiscordCommandRegistrar::new(build_http(
        &config.discord.token,
        config.discord.proxy.clone(),
        Some(app_id),
    ));
    registrar
        .sync(&registry.descriptors())
        .await
        .expect("slash command registration expected to succeed");

    // Minimal intent set, one per justified feature (see README "Gateway
    // intents"). GUILD_MEMBERS and MESSAGE_CONTENT are privileged: enable
    // "Server Members Intent" and "Message Content Intent" in the Developer
    // Portal, or the gateway disconnects on start. MESSAGE_CONTENT is
    // consumed by the LLM plugin, which reads guild message content for
    // conversation history.
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::GUILD_MEMBERS
        | GatewayIntents::MESSAGE_CONTENT;

    let mut client = ClientBuilder::new_with_http(
        build_http(&config.discord.token, config.discord.proxy, Some(app_id)),
        intents,
    )
    .event_handler(DiscordGatewayAdapter::new(Arc::clone(&kernel), gateway_context))
    .await
    .expect("failed to create client");

    // Ctrl-C: stop the gateway so `start` returns, then plugins stop in
    // reverse order below.
    let shard_manager = client.shard_manager.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("shutdown signal received");
            shard_manager.shutdown_all().await;
        }
    });

    let start_result = client.start().await;
    kernel.shutdown();
    gateway_exit_code(start_result)
}

/// Maps the gateway outcome onto the process exit code: non-zero on gateway
/// failure so orchestrator restart policies (Docker restart=on-failure,
/// systemd Restart=on-failure) see the crash; the clean Ctrl-C path exits 0.
fn gateway_exit_code(start_result: serenity::Result<()>) -> ExitCode {
    match start_result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(?err, "gateway client failed");
            ExitCode::FAILURE
        }
    }
}

fn load_config() -> anyhow::Result<Configuration> {
    let config = Config::builder()
        .add_source(File::with_name("versabot").required(false))
        .add_source(Environment::with_prefix("VERSABOT").separator("__"))
        .build()?;
    Ok(config.try_deserialize()?)
}

/// Extracts the status rotator's settings from a configuration snapshot;
/// absent or invalid `[status]` maps to the disabled state.
fn status_settings(config: &Configuration) -> StatusSettings {
    match &config.status {
        Some(status) if status.interval_seconds > 0 && !status.statuses.is_empty() => {
            StatusSettings {
                interval: Duration::from_secs(status.interval_seconds),
                statuses: status.statuses.clone(),
            }
        }
        Some(_) => {
            tracing::warn!(
                "config section [status] ignored: interval_seconds must be > 0 and statuses must not be empty"
            );
            StatusSettings::disabled()
        }
        None => StatusSettings::disabled(),
    }
}

/// Applies `[status]` changes to the rotator; the plugin itself ignores
/// identical settings, so unrelated config edits don't reset the rotation.
struct StatusSettingsReloader {
    plugin: Arc<StatusRotatorPlugin>,
}

impl ConfigChangeHandler<Configuration> for StatusSettingsReloader {
    fn on_change(&self, config: Arc<Configuration>) {
        self.plugin.update(status_settings(&config));
    }
}

/// Scheduler job: one watcher poll per tick.
struct ConfigWatchJob {
    watcher: Arc<PollingConfigWatcher<Configuration>>,
}

#[async_trait]
impl Job for ConfigWatchJob {
    async fn run(&self) {
        self.watcher.poll();
    }
}

fn build_http(token: &str, proxy: Option<String>, application_id: Option<u64>) -> Http {
    let mut http_builder = HttpBuilder::new(token);
    if let Some(application_id) = application_id {
        http_builder =
            http_builder.application_id(serenity::all::ApplicationId::new(application_id));
    }
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(proxy).expect("proxy string expected to be valid");
        let reqwest_client = reqwest::Client::builder()
            .proxy(proxy)
            .build()
            .expect("failed to build reqwest client");
        http_builder = http_builder.client(reqwest_client);
    }
    http_builder.build()
}
