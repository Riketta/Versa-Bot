use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use config::{Config, Environment, File};
use parking_lot::Mutex;
use serenity::all::{ClientBuilder, GatewayIntents, Http, HttpBuilder};

use versa_bot::infrastructure::{
    Configuration, LlmConfig, LlmReasoningStyle, LlmSummaryPlacement, LolLeaderboardConfig,
    LolStoreConfig, PollingConfigWatcher,
    inbound_adapters::{DiscordGatewayAdapter, DiscordPlatform},
    observability,
    outbound_adapters::{
        DiscordCommandRegistrar, SerenityChatOutputFactory, SerenityNickname, SerenityPresence,
        SqlxStorage,
    },
    plugin_adapters::{InMemoryCommandRegistry, InMemoryEventBus, TokioScheduler},
};
use versa_bot::kernel::{
    plugin_ports::{CommandRegistryPort, Job, MiddlewarePluginPort, PluginPort, SchedulerPort},
    services::KernelService,
    spi_ports::{
        ChatOutputFactoryPort, ConfigChangeHandler, ConfigPort, NicknamePort, PlatformInfoPort,
        PluginStoragePort, PresencePort, StoragePort,
    },
};
use versa_bot::plugins::audit::AuditLogPlugin;
use versa_bot::plugins::auth::{AuthPlugin, OwnerList};
use versa_bot::plugins::command::CommandPlugin;
use versa_bot::plugins::llm::{
    ChatEngine, DeckRandom, ImageDescriber, LlmCompletionPort, LlmPlugin, LlmSettings,
    ModelSettings, OpenAiCompatibleAdapter, ProviderSettings, RandomPort, ReasoningStyle,
    SummaryPlacement, VisionService,
};
use versa_bot::plugins::lol_leaderboard::{
    DeepLolSource, EngineSettings as LeaderboardEngineSettings, LeaderboardEngine,
    LeaderboardPlugin, LeaderboardSourcePort, ResolvedView as LeaderboardView,
};
use versa_bot::plugins::lol_store::{
    AnnounceFlags, DEFAULT_GUILD_CAP, DEFAULT_HISTORY_DAYS, DEFAULT_USER_CAP, EngineSettings,
    LcuClient, LolStorePlugin, StoreEngine,
};
use versa_bot::plugins::nickname::NicknamePlugin;
use versa_bot::plugins::status::{StatusRotatorPlugin, StatusSettings};
use versa_bot::plugins::tracker::UserActivityTrackerPlugin;

#[tokio::main]
async fn main() -> ExitCode {
    let config = load_config().expect("config expected to exist and be valid");

    // Sentry/GlitchTip endpoint is DSN-driven; absent DSN means stdout only.
    // Optional `[logging]` adds a file layer. Both need guards that outlive
    // the whole run - bound at `main`'s top level.
    let _guards = observability::init(
        config.debug,
        config.logging.as_ref(),
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
    // Bot identity for LLM prompt templates (`{{bot_name}}`/`{{bot_id}}`):
    // resolved once here, config overrides take precedence per part.
    let bot_user = bootstrap.get_current_user().await.expect("bot user expected to be resolvable");
    // The deployment's platform identity - adapter-owned values (one
    // constant in the serenity adapter). The kernel binds storage under the
    // slug; plugins render the display name (prompt templates).
    let platform_info: Arc<dyn PlatformInfoPort> = Arc::new(DiscordPlatform);

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
    // Bot owners: deployment-global identities from the root `owners` config
    // (platform user IDs). They sit above every guild-side rank, are
    // hot-reloadable, and cannot be changed through the bot.
    let owners = OwnerList::from_ids(&config.owners);
    tracing::info!(count = owners.len(), "bot owner list configured");
    let auth =
        Arc::new(AuthPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>, owners));
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
    let llm_settings = Arc::new(
        config
            .llm
            .as_ref()
            .map(|config| {
                // Bot identity for prompt templates: config overrides win,
                // discovery from the platform is the fallback per part.
                llm_settings_from(
                    config,
                    Some(config.bot_name.clone().unwrap_or_else(|| bot_user.name.clone())),
                    Some(config.bot_id.clone().unwrap_or_else(|| bot_user.id.get().to_string())),
                    platform_info.message_limit(),
                )
            })
            .unwrap_or_default(),
    );
    versa_bot::plugins::llm::warn_unknown_prompt_tokens(&llm_settings);
    let llm_adapter = OpenAiCompatibleAdapter::from_settings(Arc::clone(&llm_settings))
        .expect("config [llm] section expected to be valid (api_key_env set, proxies parseable)");
    // One completion port, two consumers: the chat engine and the image
    // recognition service (same adapter, per the cardinality rule).
    let llm_completion = Arc::new(llm_adapter) as Arc<dyn LlmCompletionPort>;
    let llm_describer = Arc::new(VisionService::new(Arc::clone(&llm_completion)));
    // Plugin-global storage for the usage totals: the same backend, bound
    // to the deployment slug once (the kernel's context shares the same
    // instance for event-scoped access).
    let llm_usage_storage = Arc::clone(&storage) as Arc<dyn PluginStoragePort>;
    let llm_engine = Arc::new(ChatEngine::new(
        llm_settings,
        Arc::clone(&llm_completion),
        Arc::new(DeckRandom::new()) as Arc<dyn RandomPort>,
        llm_describer as Arc<dyn ImageDescriber>,
        Arc::clone(&platform_info),
        llm_usage_storage.plugin_scoped(platform_info.slug()),
    ));

    // Kernel scheduling service + presence: the status rotator's and the
    // LLM usage flush's drives.
    let scheduler = Arc::new(TokioScheduler::new());
    let llm = Arc::new(LlmPlugin::new(
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
        llm_engine,
        Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
    ));
    // Bus-only plugin: in `plugins` for lifecycle, never in the middleware
    // chain - it reacts to derived events, not to raw inbound ones.
    let audit = Arc::new(AuditLogPlugin::new(event_bus.clone()));

    // Presence rides the gateway: queued until the connection is ready
    // (the nickname adapter below reuses the same context handle).
    let (presence, gateway_context) = SerenityPresence::new();
    let presence = Arc::new(presence);

    // Guild-local bot name: same gateway context handle as presence, but a
    // REST call - it fails (no queueing) when the gateway is not up yet,
    // because the invoking member is waiting for the command's answer.
    let nickname = Arc::new(SerenityNickname::new(Arc::clone(&gateway_context)));
    let nickname_plugin = Arc::new(NicknamePlugin::new(
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
        Arc::clone(&nickname) as Arc<dyn NicknamePort>,
    ));

    // Status rotator: always registered - `[status]` changes are applied at
    // runtime; an absent/invalid section just means it starts disabled.
    let status_plugin = Arc::new(StatusRotatorPlugin::new(
        Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
        Arc::clone(&presence) as Arc<dyn PresencePort>,
        status_settings(&config),
    ));

    // LoL store watcher: always registered (its commands explain themselves
    // when the watcher is off). The LCU client and the poll engine are
    // built once here; enabling or disabling the watcher stays startup-only
    // (absent section, empty lockfile path, zero poll), while the announce
    // flags, watch caps, and history retention hot-reload. The store state
    // and history are deployment-wide: they persist in plugin-global
    // storage, not guild storage.
    let lol_settings = lol_store_engine_settings(config.lol_store.as_ref());
    let lcu_client = LcuClient::new(
        config.lol_store.as_ref().map(|store| store.lockfile_path.clone()).unwrap_or_default(),
        config.lol_store.as_ref().map(|store| store.address.clone()).unwrap_or_default(),
    )
    .expect("LCU http client expected to build");
    let lol_engine = Arc::new(StoreEngine::new(
        Arc::new(lcu_client) as Arc<dyn versa_bot::plugins::lol_store::lcu::LcuPort>,
        Arc::clone(&storage) as Arc<dyn StoragePort>,
        storage.plugin_scoped(platform_info.slug()),
        Arc::clone(&chat_factory),
        event_bus.clone(),
        Arc::clone(&platform_info),
        lol_settings,
    ));
    let lol_store = Arc::new(LolStorePlugin::new(
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
        Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
        Arc::clone(&lol_engine),
    ));

    // LoL leaderboard: always registered (its command explains itself when
    // the source is unconfigured). The HTTP client and engine are built
    // once here - the proxy and the pacing interval are startup-only - but
    // the data window (regions, parse depth, cache TTL, display view)
    // hot-reloads. The data is world data: no guild storage, no scheduler -
    // the refresh runs inside the command under the typing indicator.
    // 3 = the documented `request_interval_secs` default for an absent
    // section (the engine then serves no regions, so nothing calls the
    // source anyway).
    let leaderboard_interval = Duration::from_secs(
        config.lol_leaderboard.as_ref().map_or(3, |config| config.request_interval_secs).max(1),
    );
    let leaderboard_proxy = config.lol_leaderboard.as_ref().and_then(|c| c.proxy.clone());
    let leaderboard_source: Arc<dyn LeaderboardSourcePort> = Arc::new(
        DeepLolSource::new(leaderboard_proxy.as_deref(), leaderboard_interval)
            .expect("config [lol_leaderboard] section expected to be valid (proxy parseable)"),
    );
    let leaderboard_engine = Arc::new(LeaderboardEngine::with_storage(
        Arc::clone(&leaderboard_source),
        leaderboard_engine_settings(config.lol_leaderboard.as_ref(), leaderboard_source.as_ref()),
        storage.plugin_scoped(platform_info.slug()),
    ));
    let lol_leaderboard = Arc::new(LeaderboardPlugin::new(
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
        Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
        Arc::clone(&leaderboard_engine),
    ));

    // Config hot reload: a polling watcher re-reads file+env configuration
    // and applies hot-reloadable sections without a restart. The handle is
    // kept and cancelled after the kernel shuts down - a post-shutdown tick
    // must not hand a fresh snapshot to an already-stopped plugin.
    let watcher = Arc::new(PollingConfigWatcher::new(load_config));
    watcher.seed(config.clone());
    watcher.subscribe(Arc::new(ConfigSectionDiffLogger {
        last: Mutex::new(Some(Arc::new(config.clone()))),
    }));
    watcher.subscribe(Arc::new(StatusSettingsReloader { plugin: Arc::clone(&status_plugin) }));
    watcher.subscribe(Arc::new(AuthOwnersReloader { plugin: Arc::clone(&auth) }));
    watcher.subscribe(Arc::new(LolStoreSettingsReloader { engine: Arc::clone(&lol_engine) }));
    watcher.subscribe(Arc::new(LolLeaderboardSettingsReloader {
        engine: Arc::clone(&leaderboard_engine),
        source: Arc::clone(&leaderboard_source),
        plugin: Arc::clone(&lol_leaderboard),
    }));
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
        Arc::clone(&nickname_plugin) as Arc<dyn PluginPort>,
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
            .plugin_storage(Arc::clone(&storage) as Arc<dyn PluginStoragePort>)
            .platform_info(Arc::clone(&platform_info))
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
    // The usage flush cannot run inside the plugin's sync `stop` (it would
    // race process exit as a detached task) - the composition root awaits
    // it here, bounded: a graceful restart must not hang on observability.
    match tokio::time::timeout(Duration::from_secs(3), llm.flush_usage_totals()).await {
        Ok(()) => tracing::debug!("final usage flush completed"),
        Err(_) => {
            tracing::warn!("final usage flush timed out after 3s - pending usage totals lost");
        }
    }
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
fn llm_settings_from(
    config: &LlmConfig,
    bot_name: Option<String>,
    bot_id: Option<String>,
    message_limit: Option<usize>,
) -> LlmSettings {
    // Operator knobs clamped to safe ranges, mirroring the command-layer
    // guards. The clamp is announced - a silent correction hides a typo.
    // The reply-chunk floor is operator policy, announced when corrected;
    // the platform cap stays the hard invariant and wins over floor and
    // default alike (`cap.max(1)` keeps a zero-cap platform away from a
    // boot panic, min > max).
    let min_split_length = match message_limit {
        Some(cap) => config.min_split_length.max(1).min(cap.max(1)),
        None => config.min_split_length.max(1),
    };
    if config.min_split_length == 0 {
        tracing::warn!("[llm] min_split_length is zero - clamped to 1");
    } else if let Some(cap) = message_limit
        && config.min_split_length > cap
    {
        tracing::warn!(
            configured = config.min_split_length,
            clamped = min_split_length,
            "[llm] min_split_length exceeds the platform's message cap - clamped"
        );
    }
    let max_message_length = match message_limit {
        Some(cap) => config.max_message_length.max(min_split_length).min(cap.max(1)),
        None => config.max_message_length.max(min_split_length),
    };
    if config.max_message_length < min_split_length {
        tracing::warn!(
            configured = config.max_message_length,
            clamped = max_message_length,
            "[llm] max_message_length below the reply-chunk floor - raised"
        );
    } else if let Some(cap) = message_limit
        && config.max_message_length > cap
    {
        tracing::warn!(
            configured = config.max_message_length,
            clamped = max_message_length,
            "[llm] max_message_length exceeds the platform's message cap - clamped"
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
        bot_name,
        bot_id,
        time_offset_minutes: {
            // A shifted clock beyond ±24h is a config typo, not a timezone.
            const MAX_OFFSET_MINUTES: i16 = 1439;
            let clamped = config.time_offset_minutes.clamp(-MAX_OFFSET_MINUTES, MAX_OFFSET_MINUTES);
            if clamped != config.time_offset_minutes {
                tracing::warn!(
                    configured = config.time_offset_minutes,
                    clamped,
                    "[llm] time_offset_minutes outside -1439..=1439 - clamped"
                );
            }
            clamped
        },
        compaction_model: config.compaction_model.clone(),
        compaction_keep_tail,
        max_message_length,
        min_split_length,
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
                // Backoff caps at 8s, but every attempt still burns up to
                // `timeout_secs` inside the engine's per-channel lock - a
                // typo'd retry count must not quietly scale that into
                // minutes.
                const MAX_RETRIES: u32 = 8;
                let max_retries = provider.max_retries.min(MAX_RETRIES);
                if provider.max_retries > MAX_RETRIES {
                    tracing::warn!(
                        provider = %name,
                        configured = provider.max_retries,
                        clamped = MAX_RETRIES,
                        "[llm] max_retries above the cap - clamped"
                    );
                }
                (
                    name.clone(),
                    ProviderSettings {
                        api_url: provider.api_url.clone(),
                        api_key_env: provider.api_key_env.clone(),
                        proxy: provider.proxy.clone(),
                        timeout_secs,
                        max_retries,
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
        .add_source(
            // `ignore_empty`: an empty variable (`VERSABOT__DISCORD__TOKEN=`)
            // means absent - a blank line in an env file or a placeholder
            // export must not override a valid file value with "".
            Environment::with_prefix("VERSABOT").separator("__").ignore_empty(true),
        )
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

/// Extracts the store watcher's settings; absent `[lol_store]`, an empty
/// lockfile path or a zero poll all map to the disabled state (zero poll =
/// the scheduler contract's dead handle).
fn lol_store_engine_settings(lol_store: Option<&LolStoreConfig>) -> EngineSettings {
    let disabled = EngineSettings {
        poll: Duration::ZERO,
        flags: AnnounceFlags::all_on(),
        watch_user_cap: DEFAULT_USER_CAP,
        watch_guild_cap: DEFAULT_GUILD_CAP,
        history_days: DEFAULT_HISTORY_DAYS,
    };
    let Some(lol_store) = lol_store else { return disabled };
    if lol_store.lockfile_path.is_empty() {
        tracing::info!("config section [lol_store] has no lockfile_path - store watcher disabled");
        return disabled;
    }
    if lol_store.poll_secs == 0 {
        tracing::warn!("config section [lol_store] ignored: poll_secs must be > 0");
        return disabled;
    }
    EngineSettings {
        poll: Duration::from_secs(lol_store.poll_secs),
        flags: AnnounceFlags {
            sales: lol_store.announce_sales,
            new_skins: lol_store.announce_new_skins,
            mythic_rotation: lol_store.announce_mythic_rotation,
            yourshop: lol_store.announce_yourshop,
        },
        watch_user_cap: lol_store.watch_user_cap,
        watch_guild_cap: lol_store.watch_guild_cap,
        history_days: lol_store.history_days,
    }
}

/// Maps `[lol_leaderboard]` onto the engine settings. A zero interval,
/// depth or TTL would misbehave rather than degrade, so - like
/// `[lol_store]`'s zero poll - it disables the plugin with a warning
/// instead of a silent rescue. `None` maps to nothing usable: the command
/// stays in "not configured" mode.
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
    // The source adapter caps its own page walk, but an absurd depth would
    // still bloat memory and the rendered dump before any cap helps.
    const MAX_PARSE_DEPTH: u32 = 10_000;
    let parse_depth = if config.parse_depth > MAX_PARSE_DEPTH {
        tracing::warn!(
            requested = config.parse_depth,
            cap = MAX_PARSE_DEPTH,
            "config [lol_leaderboard] parse_depth capped"
        );
        MAX_PARSE_DEPTH
    } else {
        config.parse_depth
    };
    if config.cache_ttl_secs == 0 {
        tracing::warn!("config section [lol_leaderboard] ignored: cache_ttl_secs must be > 0");
        return None;
    }
    let settings = LeaderboardEngineSettings::new(
        source,
        &config.regions,
        parse_depth,
        Duration::from_secs(config.cache_ttl_secs),
        Duration::from_secs(config.request_interval_secs),
        config.background_refresh,
        Duration::from_secs(config.inline_refresh_budget_secs),
        LeaderboardView::resolve(
            parse_depth,
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

/// Maps `[lol_leaderboard]` onto the engine settings, degrading an absent
/// or invalid section to the not-configured mode the boot path uses -
/// derived from the config's own defaults so the two can never drift.
/// Shared by boot and the hot-reload handler.
fn leaderboard_engine_settings(
    config: Option<&LolLeaderboardConfig>,
    source: &dyn LeaderboardSourcePort,
) -> LeaderboardEngineSettings {
    leaderboard_settings(config, source).unwrap_or_else(|| {
        leaderboard_settings(Some(&LolLeaderboardConfig::default()), source)
            .expect("the default leaderboard config is enabled")
    })
}

/// Names the configuration sections that changed, block by block. The
/// watcher is generic - it can only see "the snapshot differs"; section
/// knowledge lives here in the composition root. Hot sections are applied
/// by the reloaders subscribed after this one (their own lines carry the
/// values); startup-only sections are named explicitly as kept, so an
/// edit there cannot read as applied.
struct ConfigSectionDiffLogger {
    last: Mutex<Option<Arc<Configuration>>>,
}

impl ConfigChangeHandler<Configuration> for ConfigSectionDiffLogger {
    fn on_change(&self, config: Arc<Configuration>) {
        let previous = self.last.lock().replace(Arc::clone(&config));
        let Some(previous) = previous.as_deref() else { return };
        let (hot, startup_only) = section_changes(previous, &config);
        if hot.is_empty() && startup_only.is_empty() {
            return;
        }
        tracing::info!(?hot, ?startup_only, "configuration sections changed");
    }
}

/// Block-level diff of two configuration snapshots: the changed sections,
/// split into hot-applied (a subscribed reloader picks them up) and
/// startup-only (kept until restart). Exhaustive over the `Configuration`
/// fields - a new section must be added here or its changes go unnamed.
fn section_changes(
    previous: &Configuration,
    current: &Configuration,
) -> (Vec<&'static str>, Vec<&'static str>) {
    let mut hot = Vec::new();
    let mut startup_only = Vec::new();
    if previous.status != current.status {
        hot.push("status");
    }
    if previous.owners != current.owners {
        hot.push("owners");
    }
    if previous.lol_store != current.lol_store {
        hot.push("lol_store");
    }
    if previous.lol_leaderboard != current.lol_leaderboard {
        hot.push("lol_leaderboard");
    }
    if previous.debug != current.debug {
        startup_only.push("debug");
    }
    if previous.discord != current.discord {
        startup_only.push("discord");
    }
    if previous.storage != current.storage {
        startup_only.push("storage");
    }
    if previous.sentry != current.sentry {
        startup_only.push("sentry");
    }
    if previous.logging != current.logging {
        startup_only.push("logging");
    }
    if previous.llm != current.llm {
        startup_only.push("llm");
    }
    (hot, startup_only)
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

/// Hot-reloads the deployment-global bot-owner list. Owner identity is
/// config data, not connection state - unlike token/providers it can apply
/// without a restart.
struct AuthOwnersReloader {
    plugin: Arc<AuthPlugin>,
}

impl ConfigChangeHandler<Configuration> for AuthOwnersReloader {
    fn on_change(&self, config: Arc<Configuration>) {
        self.plugin.update_owners(&config.owners);
    }
}

/// Applies `[lol_store]` value changes (announce flags, watch caps, history
/// retention) to the poll engine. The poll cadence, lockfile path and LCU
/// address stay
/// boot-frozen - the poll job is scheduled once and the client is built
/// once - so enabling or disabling the watcher remains a restart-level
/// change: a config that maps to disabled leaves the current values
/// untouched.
struct LolStoreSettingsReloader {
    engine: Arc<StoreEngine<InMemoryEventBus>>,
}

impl ConfigChangeHandler<Configuration> for LolStoreSettingsReloader {
    fn on_change(&self, config: Arc<Configuration>) {
        let settings = lol_store_engine_settings(config.lol_store.as_ref());
        if settings.poll.is_zero() {
            tracing::info!("config lol_store absent or disabled - watcher values kept");
            return;
        }
        // The poll cadence is boot-frozen (the job is scheduled once):
        // naming a changed-but-kept value keeps the section-level "hot"
        // diff line from reading as applied.
        let current = self.engine.settings();
        if current.poll != settings.poll {
            tracing::info!(
                "config lol_store poll_secs changed - kept until restart (the poll job is \n                 scheduled once)"
            );
        }
        self.engine.update_settings(settings);
    }
}

/// Applies `[lol_leaderboard]` value changes (regions, parse depth, cache
/// TTL, display view). The proxy and the pacing interval stay boot-frozen
/// with the source adapter; an absent or invalid section degrades to the
/// same not-configured mode as at boot - the engine is always live, only
/// its data window swaps. A TTL change reschedules the background job so
/// its tick cadence never forks from the configured freshness window.
struct LolLeaderboardSettingsReloader {
    engine: Arc<LeaderboardEngine>,
    source: Arc<dyn LeaderboardSourcePort>,
    plugin: Arc<LeaderboardPlugin>,
}

impl ConfigChangeHandler<Configuration> for LolLeaderboardSettingsReloader {
    fn on_change(&self, config: Arc<Configuration>) {
        let old = self.engine.settings();
        let old_ttl = old.cache_ttl;
        let settings =
            leaderboard_engine_settings(config.lol_leaderboard.as_ref(), self.source.as_ref());
        let new_ttl = settings.cache_ttl;
        // Pacing, refresh mode and the inline budget are boot-frozen with
        // the source adapter and the job; naming a changed-but-kept value
        // keeps the section-level "hot" diff line honest.
        if old.request_interval != settings.request_interval {
            tracing::info!(
                "config lol_leaderboard request_interval_secs changed - kept until restart"
            );
        }
        if old.background_refresh != settings.background_refresh {
            tracing::info!(
                "config lol_leaderboard background_refresh changed - kept until restart (boot \n                 decision)"
            );
        }
        if old.inline_budget != settings.inline_budget {
            tracing::info!(
                "config lol_leaderboard inline_refresh_budget_secs changed - kept until restart"
            );
        }
        self.engine.update_settings(settings);
        if old_ttl != new_ttl {
            self.plugin.reschedule(new_ttl);
        }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;

    use super::*;
    use versa_bot::infrastructure::{LlmProviderConfig, LolLeaderboardConfig, LolStoreConfig};
    use versa_bot::plugins::lol_leaderboard::{RegionLeaderboard, SourceError};

    /// The boot-time clamp matrix of `llm_settings_from`: the platform cap,
    /// the absence of one, and the zero-value rescues all announce
    /// themselves - here only the resulting values are pinned.
    #[test]
    fn max_message_length_clamps_to_the_platform_cap() {
        let mut config = LlmConfig::default();
        config.max_message_length = 5000;

        let settings = llm_settings_from(&config, None, None, Some(2000));
        assert_eq!(settings.max_message_length, 2000);

        // No platform cap: the configured value passes through.
        let settings = llm_settings_from(&config, None, None, None);
        assert_eq!(settings.max_message_length, 5000);

        // Below the reply-chunk floor: raised to it, cap or no cap.
        config.max_message_length = 10;
        let settings = llm_settings_from(&config, None, None, Some(2000));
        assert_eq!(settings.max_message_length, 100);
        let settings = llm_settings_from(&config, None, None, None);
        assert_eq!(settings.max_message_length, 100);

        // A cap below the floor degrades both to the cap: the hard invariant
        // wins.
        let settings = llm_settings_from(&config, None, None, Some(50));
        assert_eq!(settings.max_message_length, 50);
        assert_eq!(settings.min_split_length, 50);
    }

    /// The floor itself is operator policy: a raised floor lifts the plugin
    /// default with it; a floor above the cap degrades to the cap.
    #[test]
    fn min_split_length_is_operator_policy() {
        let mut config = LlmConfig::default();
        config.min_split_length = 500;
        config.max_message_length = 10;
        let settings = llm_settings_from(&config, None, None, Some(2000));
        assert_eq!(settings.min_split_length, 500);
        assert_eq!(settings.max_message_length, 500);

        config.min_split_length = 2500;
        config.max_message_length = 2500;
        let settings = llm_settings_from(&config, None, None, Some(2000));
        assert_eq!(settings.min_split_length, 2000);
        assert_eq!(settings.max_message_length, 2000);
    }

    #[test]
    fn llm_zero_values_clamp_to_safe_floors() {
        let mut config = LlmConfig::default();
        config.max_message_length = 0;
        config.min_split_length = 0;
        config.stream_interval_ms = 10;
        config.compaction_keep_tail = 0;
        config.image_max_side = 0;

        let settings = llm_settings_from(&config, None, None, Some(2000));
        assert_eq!(settings.max_message_length, 1);
        assert_eq!(settings.min_split_length, 1);
        assert_eq!(settings.stream_interval_ms, 250);
        assert_eq!(settings.compaction_keep_tail, 1);
        assert_eq!(settings.image_max_side, 1);
    }

    /// The plugin-facing defaults and the config-file defaults are two
    /// `Default` impls that must agree: production values come from the
    /// config side only, so a one-sided edit would make the plugin's own
    /// tests exercise numbers the composition root never produces.
    #[test]
    fn llm_defaults_survive_the_config_mapping() {
        let settings = llm_settings_from(&LlmConfig::default(), None, None, None);
        let expected = LlmSettings::default();
        assert_eq!(settings.default_system_prompt, expected.default_system_prompt);
        assert_eq!(settings.default_compaction_prompt, expected.default_compaction_prompt);
        assert_eq!(settings.time_offset_minutes, expected.time_offset_minutes);
        assert_eq!(settings.compaction_keep_tail, expected.compaction_keep_tail);
        assert_eq!(settings.max_message_length, expected.max_message_length);
        assert_eq!(settings.min_split_length, expected.min_split_length);
        assert_eq!(settings.stream_interval_ms, expected.stream_interval_ms);
        assert_eq!(settings.max_prompt_file_bytes, expected.max_prompt_file_bytes);
        assert_eq!(settings.image_max_side, expected.image_max_side);
        assert_eq!(settings.image_jpeg_quality, expected.image_jpeg_quality);
        assert_eq!(settings.image_max_source_bytes, expected.image_max_source_bytes);
        assert_eq!(settings.max_images_per_message, expected.max_images_per_message);
        assert_eq!(settings.max_consecutive_newlines, expected.max_consecutive_newlines);
        assert_eq!(settings.react_max_per_message, expected.react_max_per_message);
        assert_eq!(settings.log_raw_traffic, expected.log_raw_traffic);
        assert_eq!(settings.image_prompt, expected.image_prompt);
        assert!(settings.providers.is_empty() && expected.providers.is_empty());
    }

    #[test]
    fn time_offset_clamps_to_a_full_day_either_way() {
        let mut config = LlmConfig::default();
        config.time_offset_minutes = 5000;
        let settings = llm_settings_from(&config, None, None, None);
        assert_eq!(settings.time_offset_minutes, 1439);

        config.time_offset_minutes = -5000;
        let settings = llm_settings_from(&config, None, None, None);
        assert_eq!(settings.time_offset_minutes, -1439);
    }

    /// A typo'd retry count must not quietly scale the per-channel lock
    /// hold into minutes: values above the cap clamp (announced at boot -
    /// here only the resulting values are pinned). The cap itself, sane
    /// values, and the disable value pass through unchanged.
    #[test]
    fn max_retries_clamps_to_the_cap() {
        let mut config = LlmConfig::default();
        config.providers.insert(
            "zai".to_owned(),
            LlmProviderConfig {
                api_url: "https://example.invalid/v4".to_owned(),
                max_retries: 100,
                ..LlmProviderConfig::default()
            },
        );
        let settings = llm_settings_from(&config, None, None, None);
        assert_eq!(settings.providers.get("zai").expect("zai provider").max_retries, 8);

        let provider = config.providers.get_mut("zai").expect("zai provider");
        provider.max_retries = 8;
        let settings = llm_settings_from(&config, None, None, None);
        assert_eq!(settings.providers.get("zai").expect("zai provider").max_retries, 8);

        let provider = config.providers.get_mut("zai").expect("zai provider");
        provider.max_retries = 0;
        let settings = llm_settings_from(&config, None, None, None);
        assert_eq!(settings.providers.get("zai").expect("zai provider").max_retries, 0);
    }

    /// The store watcher's disable matrix: absent section, empty lockfile
    /// path, and zero poll all map to the disabled state (dead handle).
    #[test]
    fn store_watcher_disabled_states() {
        assert_eq!(lol_store_engine_settings(None).poll, Duration::ZERO);

        let mut config = LolStoreConfig::default();
        config.lockfile_path = String::new();
        assert_eq!(lol_store_engine_settings(Some(&config)).poll, Duration::ZERO);

        config.lockfile_path = "lockfile".to_owned();
        config.poll_secs = 0;
        assert_eq!(lol_store_engine_settings(Some(&config)).poll, Duration::ZERO);

        config.poll_secs = 30;
        assert_eq!(lol_store_engine_settings(Some(&config)).poll, Duration::from_secs(30));
    }

    /// The status rotator's disable matrix: absent section, zero interval,
    /// and an empty status list all map to the disabled state; a valid
    /// section maps interval and statuses through verbatim.
    #[test]
    fn status_settings_disable_matrix() {
        // Absent section.
        let mut config = Configuration::default();
        assert_eq!(status_settings(&config), StatusSettings::disabled());

        // A valid section passes through.
        config.status = Some(
            serde_json::from_str(r#"{"interval_seconds": 30, "statuses": ["one", "two"]}"#)
                .expect("valid [status] section deserializes"),
        );
        let settings = status_settings(&config);
        assert_eq!(settings.interval, Duration::from_secs(30));
        assert_eq!(settings.statuses, ["one", "two"]);

        // Zero interval: disabled, no panic.
        config.status = Some(
            serde_json::from_str(r#"{"interval_seconds": 0, "statuses": ["one"]}"#)
                .expect("zero-interval [status] section deserializes"),
        );
        assert_eq!(status_settings(&config), StatusSettings::disabled());

        // Empty status list: disabled, no panic.
        config.status = Some(
            serde_json::from_str(r#"{"interval_seconds": 30, "statuses": []}"#)
                .expect("empty-list [status] section deserializes"),
        );
        assert_eq!(status_settings(&config), StatusSettings::disabled());
    }

    struct StubSource;

    #[async_trait]
    impl LeaderboardSourcePort for StubSource {
        fn known_regions(&self) -> &'static [&'static str] {
            &[]
        }

        async fn leaderboard(
            &self,
            _region: &str,
            _depth: u32,
        ) -> Result<RegionLeaderboard, SourceError> {
            unreachable!("the settings mapping never touches the source");
        }

        async fn champion_names(&self) -> Result<HashMap<String, String>, SourceError> {
            unreachable!("the settings mapping never touches the source");
        }
    }

    /// The leaderboard's degrade matrix: absent section and every zeroed
    /// knob map to "not configured"; a valid section enables.
    #[test]
    fn leaderboard_settings_disable_matrix() {
        let source = StubSource;
        let source: &dyn LeaderboardSourcePort = &source;

        assert!(leaderboard_settings(None, source).is_none());

        let mut config =
            LolLeaderboardConfig { request_interval_secs: 0, ..LolLeaderboardConfig::default() };
        assert!(leaderboard_settings(Some(&config), source).is_none());

        config.request_interval_secs = 1;
        config.parse_depth = 0;
        assert!(leaderboard_settings(Some(&config), source).is_none());

        config.parse_depth = 1000;
        config.cache_ttl_secs = 0;
        assert!(leaderboard_settings(Some(&config), source).is_none());

        config.cache_ttl_secs = 3600;
        assert!(leaderboard_settings(Some(&config), source).is_some());
    }

    /// The reload mapping shares the boot degrade path: an absent or
    /// invalid section resolves to the not-configured mode (empty served
    /// regions), never an error.
    #[test]
    fn leaderboard_engine_settings_falls_back_to_not_configured() {
        let source = StubSource;
        let source: &dyn LeaderboardSourcePort = &source;

        assert!(leaderboard_engine_settings(None, source).regions.is_empty());

        let config =
            LolLeaderboardConfig { request_interval_secs: 0, ..LolLeaderboardConfig::default() };
        assert!(leaderboard_engine_settings(Some(&config), source).regions.is_empty());
    }

    /// The section diff names every changed block and splits hot-applied
    /// from startup-only. Exhaustive over the `Configuration` fields.
    #[test]
    fn section_changes_names_and_classifies_blocks() {
        let base = Configuration::default();

        // Hot blocks: applied by the reloaders.
        let mut current = Configuration::default();
        current.owners = vec!["42".to_owned()];
        current.status = Some(
            serde_json::from_str(r#"{"interval_seconds": 30, "statuses": ["one"]}"#)
                .expect("valid [status] section deserializes"),
        );
        let (hot, startup_only) = section_changes(&base, &current);
        assert_eq!(hot, vec!["status", "owners"]);
        assert!(startup_only.is_empty());

        // Startup-only blocks: named as kept, never applied.
        let mut current = Configuration::default();
        current.debug = true;
        current.llm = Some(LlmConfig::default());
        let (hot, startup_only) = section_changes(&base, &current);
        assert!(hot.is_empty());
        assert_eq!(startup_only, vec!["debug", "llm"]);

        // No change: nothing named (defensive - the watcher only fires on
        // a differing snapshot).
        let (hot, startup_only) = section_changes(&base, &base);
        assert!(hot.is_empty() && startup_only.is_empty());
    }

    /// The startup parse-depth cap: an absurd `parse_depth` clamps to the
    /// 10 000 cap constant (with a warning); a sane value passes through
    /// unchanged.
    #[test]
    fn parse_depth_caps_at_the_limit() {
        let source = StubSource;
        let source: &dyn LeaderboardSourcePort = &source;

        let config =
            LolLeaderboardConfig { parse_depth: 20_000, ..LolLeaderboardConfig::default() };
        let settings = leaderboard_settings(Some(&config), source).expect("valid section");
        assert_eq!(settings.parse_depth, 10_000);

        // A sane depth passes through unchanged.
        let config = LolLeaderboardConfig { parse_depth: 1000, ..LolLeaderboardConfig::default() };
        let settings = leaderboard_settings(Some(&config), source).expect("valid section");
        assert_eq!(settings.parse_depth, 1000);
    }
}
