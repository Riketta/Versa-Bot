use std::sync::Arc;

use config::{Config, Environment, File};
use serenity::all::{ClientBuilder, GatewayIntents, Http, HttpBuilder};

use versa_bot::infrastructure::{
    Configuration,
    inbound_adapters::{DiscordGatewayAdapter, SerenityChatOutputFactory},
    observability,
    outbound_adapters::{DiscordCommandRegistrar, SqlxStorage},
    plugin_adapters::{InMemoryCommandRegistry, InMemoryEventBus},
};
use versa_bot::kernel::{
    plugin_ports::{CommandRegistryPort, MiddlewarePluginPort, PluginPort},
    services::KernelService,
    spi_ports::{ChatOutputFactoryPort, StoragePort},
};
use versa_bot::plugins::auth::AuthPlugin;
use versa_bot::plugins::command::CommandPlugin;
use versa_bot::plugins::tracker::UserActivityTrackerPlugin;

#[tokio::main]
async fn main() {
    let config = Config::builder()
        .add_source(File::with_name("versabot").required(false))
        .add_source(Environment::with_prefix("VERSABOT").separator("__"))
        .build()
        .expect("config expected to exist")
        .try_deserialize::<Configuration>()
        .expect("config expected to be valid");

    // Sentry/GlitchTip endpoint is DSN-driven; absent DSN means stdout only.
    // The guard must outlive the whole run - bound at `main`'s top level.
    let _sentry_guard = observability::init(
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
    let auth = Arc::new(AuthPlugin);
    let command =
        Arc::new(CommandPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>));
    // The bus is kernel-owned; each plugin receives its own clone at
    // construction (same instance, per the cardinality rule). The registry is
    // shared the same way: the tracker declares its commands in `init()`.
    let tracker = Arc::new(UserActivityTrackerPlugin::new(
        event_bus.clone(),
        Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
    ));

    let kernel = Arc::new(
        KernelService::builder()
            .plugins(vec![
                Arc::clone(&auth) as Arc<dyn PluginPort>,
                Arc::clone(&command) as Arc<dyn PluginPort>,
                Arc::clone(&tracker) as Arc<dyn PluginPort>,
            ])
            .middleware(vec![
                Arc::clone(&auth) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&command) as Arc<dyn MiddlewarePluginPort>,
                Arc::clone(&tracker) as Arc<dyn MiddlewarePluginPort>,
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
    // intents"). GUILD_MEMBERS is privileged: enable "Server Members Intent"
    // in the Developer Portal, or the gateway disconnects on start.
    // MESSAGE_CONTENT is deliberately not requested - no current feature
    // reads guild message content; the LLM chat plugin will.
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::GUILD_MEMBERS;

    let mut client = ClientBuilder::new_with_http(
        build_http(&config.discord.token, config.discord.proxy, Some(app_id)),
        intents,
    )
    .event_handler(DiscordGatewayAdapter::new(Arc::clone(&kernel)))
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

    if let Err(err) = client.start().await {
        tracing::error!(?err, "client error");
    }

    kernel.shutdown();
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
