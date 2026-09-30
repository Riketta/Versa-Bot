use std::sync::Arc;

use config::{Config, Environment, File};
use serenity::all::{ClientBuilder, GatewayIntents, HttpBuilder};

use serenity::all::Http;

use versa_bot::infrastructure::{
    inbound_adapters::{DiscordGatewayAdapter, SerenityChatOutputFactory},
    observability,
    plugin_adapters::InMemoryEventBus,
    Configuration,
};
use versa_bot::kernel::{
    plugin_ports::{MiddlewarePluginPort, PluginPort},
    services::KernelService,
    spi_ports::ChatOutputFactoryPort,
};
use versa_bot::plugins::command::CommandPlugin;

#[tokio::main]
async fn main() {
    let config = Config::builder()
        .add_source(File::with_name("VersaBot").required(false))
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

    let http = build_http(&config.discord.token, config.discord.proxy.clone());
    let chat_output_factory = Arc::new(SerenityChatOutputFactory::new(build_http(
        &config.discord.token,
        config.discord.proxy,
    )));

    let command = Arc::new(CommandPlugin::new("!"));

    let kernel = Arc::new(
        KernelService::builder()
            .plugins(vec![Arc::clone(&command) as Arc<dyn PluginPort>])
            .middleware(vec![Arc::clone(&command) as Arc<dyn MiddlewarePluginPort>])
            .event_bus(InMemoryEventBus::new())
            .chat_output_factory(Arc::clone(&chat_output_factory) as Arc<dyn ChatOutputFactoryPort>)
            .build(),
    );

    kernel.boot().expect("kernel boot failed");

    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;

    let mut client = ClientBuilder::new_with_http(http, intents)
        .event_handler(DiscordGatewayAdapter::new(Arc::clone(&kernel)))
        .await
        .expect("failed to create client");

    if let Err(err) = client.start().await {
        tracing::error!(?err, "client error");
    }

    kernel.shutdown();
}

fn build_http(token: &str, proxy: Option<String>) -> Http {
    let mut http_builder = HttpBuilder::new(token);
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
