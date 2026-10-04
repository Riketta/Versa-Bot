use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use config::{Config, Environment, File};
use serenity::all::{ClientBuilder, GatewayIntents, Http, HttpBuilder};

use versa_bot::infrastructure::{
    Configuration, LlmConfig, LlmReasoningStyle, LlmSummaryPlacement, LolConfig,
    LolLeaderboardConfig, PollingConfigWatcher,
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
    ChatEngine, DISCORD_MESSAGE_LIMIT, DeckRandom, ImageDescriber, LlmCompletionPort, LlmPlugin,
    LlmSettings, ModelSettings, OpenAiCompatibleAdapter, ProviderSettings, RandomPort,
    ReasoningStyle, SummaryPlacement, VisionService,
};
use versa_bot::plugins::lol_leaderboard::{
    DeepLolSource, EngineSettings as LeaderboardEngineSettings, LeaderboardEngine,
    LeaderboardPlugin, LeaderboardSourcePort, ResolvedView as LeaderboardView,
};
use versa_bot::plugins::lol_store::{
    AnnounceFlags, EngineSettings, LcuClient, LolStorePlugin, StoreEngine,
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
        config.sentry.as_ref().and_then(|s| s.traces_sample_rate),
    );

    // Application id: required by Discord HTTP calls that are not authorized
    // by the bot token alone (command registration, interaction followups).
    // The proxy covers REST only: serenity's gateway WebSocket uses its own
    // connector and bypasses it - on proxy-only networks the bot would hang
    // silently at shard start. Route the container instead (see README).
    if config.discord.proxy.is_some() {
        tracing::warn!(
            "discord.proxy applies to REST calls only - the Discord gateway \
             WebSocket bypasses it; on proxy-only networks route the whole \
             container through the proxy (see README, proxy section)"
        );
    }
    let bootstrap = build_http(&config.discord.token, config.discord.proxy.clone(), None);
    let app_id = bootstrap
        .get_current_application_info()
        .await
        .expect("application info expected to be reachable")
        .id
        .get();

    // One shared REST client for every driven Discord call (factory outputs,
    // command registration): serenity rate limiting is per `Http`, so one
    // instance keeps them in one bucket set. The gateway client must own its
    // own `Http` (serenity API), and the bootstrap above exists only because
    // the application id is not known before it runs.
    let http =
        Arc::new(build_http(&config.discord.token, config.discord.proxy.clone(), Some(app_id)));

    // One shared outbound factory for event-scoped sends AND poll-driven
    // sends (the store watcher has no inbound event to scope from).
    let chat_factory: Arc<dyn ChatOutputFactoryPort> =
        Arc::new(SerenityChatOutputFactory::new(Arc::clone(&http)));

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
    // LLM chat plugin: the conversation engine is live. `[llm]` is
    // startup-only - provider clients and API keys are built once here, so
    // changes require a restart (same class as token/storage). Random
    // replies draw from per-channel decks ("fake random"): hits balance out
    // over each 100-draw cycle instead of statistically clumping; the plain
    // RNG adapter (`RandRandom`) is the drop-in swap.
    let llm_settings = Arc::new(config.llm.as_ref().map(llm_settings_from).unwrap_or_default());
    let llm_adapter = OpenAiCompatibleAdapter::from_settings(Arc::clone(&llm_settings))
        .expect("config [llm] section expected to be valid (api_key_env set, proxies parseable)");
    // One completion port, two consumers: the chat engine and the image
    // recognition service (same adapter, per the cardinality rule).
    let llm_completion = Arc::new(llm_adapter) as Arc<dyn LlmCompletionPort>;
    let llm_describer = Arc::new(VisionService::new(Arc::clone(&llm_completion)));
    let llm_engine = Arc::new(ChatEngine::new(
        llm_settings,
        Arc::clone(&llm_completion),
        Arc::new(DeckRandom::new()) as Arc<dyn RandomPort>,
        llm_describer as Arc<dyn ImageDescriber>,
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

    // LoL store watcher: always registered (its commands explain themselves
    // when the watcher is off). `[lol]` is startup-only - the LCU client and
    // the poll engine are built once here; changes require a restart. An
    // absent section, an empty lockfile path or a zero poll keep the
    // watcher disabled.
    let lol_settings = lol_engine_settings(config.lol.as_ref());
    let lcu_client = LcuClient::new(
        config.lol.as_ref().map(|lol| lol.lockfile_path.clone()).unwrap_or_default(),
        config.lol.as_ref().map(|lol| lol.address.clone()).unwrap_or_default(),
    )
    .expect("LCU http client expected to build");
    let lol_engine = Arc::new(StoreEngine::new(
        Arc::new(lcu_client) as Arc<dyn versa_bot::plugins::lol_store::lcu::LcuPort>,
        Arc::clone(&storage) as Arc<dyn StoragePort>,
        Arc::clone(&chat_factory),
        event_bus.clone(),
        lol_settings,
    ));
    let lol_store = Arc::new(LolStorePlugin::new(
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
        Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
        Arc::clone(&lol_engine),
    ));

    // LoL leaderboard: always registered (its command explains itself when
    // the source is unconfigured). `[lol_leaderboard]` is startup-only -
    // the HTTP client and engine are built once here; changes require a
    // restart. The data is world data: no guild storage, no scheduler - the
    // refresh runs inside the command under the typing indicator.
    let leaderboard_interval = Duration::from_secs(
        config.lol_leaderboard.as_ref().map_or(1, |config| config.request_interval_secs).max(1),
    );
    let leaderboard_proxy = config.lol_leaderboard.as_ref().and_then(|c| c.proxy.clone());
    let leaderboard_source = DeepLolSource::new(leaderboard_proxy.as_deref(), leaderboard_interval)
        .expect("config [lol_leaderboard] section expected to be valid (proxy parseable)");
    let leaderboard_engine_settings =
        leaderboard_settings(config.lol_leaderboard.as_ref(), &leaderboard_source).unwrap_or_else(
            || {
                LeaderboardEngineSettings::new(
                    &leaderboard_source,
                    &[],
                    1000,
                    Duration::from_secs(18 * 60 * 60),
                    Duration::from_secs(1),
                    LeaderboardView::resolve(1000, &[300, 1000], 1000, 5),
                )
            },
        );
    let leaderboard_engine = Arc::new(LeaderboardEngine::new(
        Arc::new(leaderboard_source) as Arc<dyn LeaderboardSourcePort>,
        leaderboard_engine_settings,
    ));
    let lol_leaderboard = Arc::new(LeaderboardPlugin::new(
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
        Arc::clone(&leaderboard_engine),
    ));

    // Config hot reload: a polling watcher re-reads file+env configuration
    // and applies hot-reloadable sections without a restart. The handle is
    // kept and cancelled after the kernel shuts down - a post-shutdown tick
    // must not hand a fresh snapshot to an already-stopped plugin.
    let watcher = Arc::new(PollingConfigWatcher::new(load_config));
    watcher.seed(config.clone());
    watcher.subscribe(Arc::new(StatusSettingsReloader { plugin: Arc::clone(&status_plugin) }));
    let config_watch_job = scheduler.schedule(
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
        Arc::clone(&lol_store) as Arc<dyn PluginPort>,
        Arc::clone(&lol_leaderboard) as Arc<dyn PluginPort>,
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
            .chat_output_factory(Arc::clone(&chat_factory))
            .storage(Arc::clone(&storage) as Arc<dyn StoragePort>)
            .build(),
    );

    kernel.boot().expect("kernel boot failed");

    // Discord specifics: publish the registry as global slash commands.
    let registrar = DiscordCommandRegistrar::new(Arc::clone(&http));
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

    // Ctrl-C (SIGINT) and SIGTERM (docker stop's default signal) both stop
    // the gateway so `start` returns, then plugins stop in reverse order
    // below - without the SIGTERM listener, container teardown would bypass
    // the whole graceful-shutdown lifecycle.
    let shard_manager = client.shard_manager.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shard_manager.shutdown_all().await;
    });

    let start_result = client.start().await;
    kernel.shutdown();
    config_watch_job.cancel();
    gateway_exit_code(start_result)
}

/// Resolves once the process is asked to shut down: Ctrl-C (SIGINT) or
/// SIGTERM - the signal `docker stop` sends by default. On platforms without
/// Unix signals only Ctrl-C can resolve it.
async fn shutdown_signal() {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown signal received (SIGINT)");
        }
        () = sigterm() => {
            tracing::info!("shutdown signal received (SIGTERM)");
        }
    }
}

/// Resolves on SIGTERM. Never resolves on platforms without Unix signals
/// (the select in [`shutdown_signal`] then waits for Ctrl-C only).
#[cfg(unix)]
async fn sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    signal(SignalKind::terminate()).expect("SIGTERM handler expected to install").recv().await;
}

#[cfg(not(unix))]
async fn sigterm() {
    std::future::pending::<()>().await;
}

/// Maps the infra `[llm]` config onto the plugin-facing settings. Field-by-
/// field on purpose: the composition root is the only place allowed to know
/// both sides.
fn llm_settings_from(config: &LlmConfig) -> LlmSettings {
    // Operator knobs clamped to safe ranges, mirroring the command-layer
    // guards. The clamp is announced - a silent correction hides a typo.
    let max_message_length = config.max_message_length.clamp(1, DISCORD_MESSAGE_LIMIT);
    if config.max_message_length == 0 {
        tracing::warn!("[llm] max_message_length is zero - clamped to 1");
    } else if config.max_message_length > DISCORD_MESSAGE_LIMIT {
        tracing::warn!(
            configured = config.max_message_length,
            clamped = DISCORD_MESSAGE_LIMIT,
            "[llm] max_message_length exceeds Discord's message cap - clamped"
        );
    }
    let image_max_side = config.image_max_side.max(1);
    if config.image_max_side == 0 {
        tracing::warn!("[llm] image_max_side is zero - clamped to 1");
    }
    // A zero tail makes compaction unreachable (it would fold everything
    // and keep nothing) - the window degrades to a sliding depth cap.
    let compaction_keep_tail = config.compaction_keep_tail.max(1);
    if config.compaction_keep_tail == 0 {
        tracing::warn!("[llm] compaction_keep_tail is zero - clamped to 1");
    }
    // A sub-second cadence means one Discord edit per SSE delta - a fast
    // path to rate limiting. The floor keeps the live reveal usable.
    const MIN_STREAM_INTERVAL_MS: u64 = 250;
    let stream_interval_ms = config.stream_interval_ms.max(MIN_STREAM_INTERVAL_MS);
    if config.stream_interval_ms < MIN_STREAM_INTERVAL_MS {
        tracing::warn!(
            configured = config.stream_interval_ms,
            clamped = MIN_STREAM_INTERVAL_MS,
            "[llm] stream_interval_ms below the Discord-friendly floor - clamped"
        );
    }

    LlmSettings {
        default_system_prompt: config.default_system_prompt.clone(),
        default_compaction_prompt: config.default_compaction_prompt.clone(),
        compaction_model: config.compaction_model.clone(),
        compaction_keep_tail,
        max_message_length,
        stream_interval_ms,
        max_prompt_file_bytes: config.max_prompt_file_bytes,
        image_model: config.image_model.clone(),
        image_max_side,
        image_jpeg_quality: config.image_jpeg_quality,
        image_max_source_bytes: config.image_max_source_bytes,
        image_prompt: config.image_prompt.clone(),
        max_images_per_message: config.max_images_per_message,
        max_consecutive_newlines: config.max_consecutive_newlines,
        react_max_per_message: config.react_max_per_message,
        log_raw_traffic: config.log_raw_traffic,
        providers: config
            .providers
            .iter()
            .map(|(name, provider)| {
                // A zero timeout would fail (or hang, depending on the HTTP
                // stack's reading of it) every request - clamp to 1s.
                let timeout_secs = provider.timeout_secs.max(1);
                if provider.timeout_secs == 0 {
                    tracing::warn!(provider = %name, "[llm] timeout_secs is zero - clamped to 1");
                }
                (
                    name.clone(),
                    ProviderSettings {
                        api_url: provider.api_url.clone(),
                        api_key_env: provider.api_key_env.clone(),
                        proxy: provider.proxy.clone(),
                        timeout_secs,
                        reasoning_style: match provider.reasoning_style {
                            LlmReasoningStyle::OpenaiEffort => ReasoningStyle::OpenaiEffort,
                            LlmReasoningStyle::GlmThinking => ReasoningStyle::GlmThinking,
                        },
                        extra_body: provider.extra_body.clone(),
                    },
                )
            })
            .collect(),
        models: config
            .models
            .iter()
            .map(|(name, model)| {
                (
                    name.clone(),
                    ModelSettings {
                        reasoning: model.reasoning,
                        context_window: model.context_window,
                        summary_placement: match model.summary_placement {
                            LlmSummaryPlacement::SystemSuffix => SummaryPlacement::SystemSuffix,
                            LlmSummaryPlacement::SystemTurn => SummaryPlacement::SystemTurn,
                            LlmSummaryPlacement::AssistantTurn => SummaryPlacement::AssistantTurn,
                        },
                    },
                )
            })
            .collect(),
    }
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

/// Extracts the store watcher's settings; absent `[lol]`, an empty lockfile
/// path or a zero poll all map to the disabled state (zero poll = the
/// scheduler contract's dead handle).
fn lol_engine_settings(lol: Option<&LolConfig>) -> EngineSettings {
    let disabled = EngineSettings { poll: Duration::ZERO, flags: AnnounceFlags::all_on() };
    let Some(lol) = lol else { return disabled };
    if lol.lockfile_path.is_empty() {
        tracing::info!("config section [lol] has no lockfile_path - store watcher disabled");
        return disabled;
    }
    if lol.poll_secs == 0 {
        tracing::warn!("config section [lol] ignored: poll_secs must be > 0");
        return disabled;
    }
    EngineSettings {
        poll: Duration::from_secs(lol.poll_secs),
        flags: AnnounceFlags {
            sales: lol.announce_sales,
            new_skins: lol.announce_new_skins,
            mythic_rotation: lol.announce_mythic_rotation,
            yourshop: lol.announce_yourshop,
        },
    }
}

/// Maps `[lol_leaderboard]` onto the engine settings. A zero interval,
/// depth or TTL would misbehave rather than degrade, so - like `[lol]`'s
/// zero poll - it disables the plugin with a warning instead of a silent
/// rescue. `None` maps to nothing usable: the command stays in
/// "not configured" mode.
fn leaderboard_settings(
    config: Option<&LolLeaderboardConfig>,
    source: &dyn LeaderboardSourcePort,
) -> Option<LeaderboardEngineSettings> {
    let Some(config) = config else { return None };
    if config.request_interval_secs == 0 {
        tracing::warn!(
            "config section [lol_leaderboard] ignored: request_interval_secs must be > 0"
        );
        return None;
    }
    if config.parse_depth == 0 {
        tracing::warn!("config section [lol_leaderboard] ignored: parse_depth must be > 0");
        return None;
    }
    if config.cache_ttl_secs == 0 {
        tracing::warn!("config section [lol_leaderboard] ignored: cache_ttl_secs must be > 0");
        return None;
    }
    let settings = LeaderboardEngineSettings::new(
        source,
        &config.regions,
        config.parse_depth,
        Duration::from_secs(config.cache_ttl_secs),
        Duration::from_secs(config.request_interval_secs),
        LeaderboardView::resolve(
            config.parse_depth,
            &config.display_buckets,
            config.champ_pool_depth,
            config.champs_per_role,
        ),
    );
    if settings.regions.is_empty() {
        tracing::info!(
            "config section [lol_leaderboard] has no regions served by the source - command in not-configured mode"
        );
    }
    Some(settings)
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
