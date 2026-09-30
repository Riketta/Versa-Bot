mod serenity_messaging_adapter;

pub use serenity_messaging_adapter::{DiscordGatewayAdapter, SerenityChatOutputFactory};

use tokio_util::sync::CancellationToken;

/// Lifecycle for driving adapters, run by the composition root.
pub trait ServerBootstrap {
    fn name(&self) -> &str;
    async fn start(self, shutdown: CancellationToken);
}
