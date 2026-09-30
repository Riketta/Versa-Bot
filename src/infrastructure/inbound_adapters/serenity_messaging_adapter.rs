use std::sync::Arc;

use serenity::all::{
    ChannelId as SerenityChannelId, Context, CreateMessage, EventHandler, Http, Message, Ready,
};
use serenity::async_trait;

use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{
        ChannelId, GuildId, MessageId, Origin, OutboundError, OutboundMessage, Platform,
        RequestContext, UserId,
    },
    spi_ports::{ChatOutputFactoryPort, ChatOutputPort},
};

/// Kernel driving adapter: normalizes Discord gateway events onto the
/// chat-agnostic `RequestContext` taxonomy and pushes each through the
/// pipeline. Platform specifics (serenity types) never cross this boundary.
pub struct DiscordGatewayAdapter<H: RequestHandlerPort> {
    handler: H,
}

impl<H: RequestHandlerPort> DiscordGatewayAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl<H: RequestHandlerPort> EventHandler for DiscordGatewayAdapter<H> {
    async fn message(&self, _ctx: Context, message: Message) {
        if message.author.bot {
            return;
        }

        let origin = Origin {
            platform: Platform::Discord,
            guild_id: message.guild_id.map(|guild_id| GuildId(guild_id.get())),
            channel_id: ChannelId(message.channel_id.get()),
            user_id: UserId(message.author.id.get()),
            message_id: Some(MessageId(message.id.get())),
        };

        self.handler
            .handle(RequestContext::message_received(origin, message.content))
            .await;
    }

    async fn ready(&self, _ctx: Context, ready: Ready) {
        tracing::info!("connected as {}", ready.user.name);
    }
}

/// `ChatOutputFactoryPort` bound to Discord: produces `ChatOutputPort`s
/// scoped to a specific event origin (channel, inside its guild). This is
/// how the kernel builds event-scoped output ports without knowing Discord.
pub struct SerenityChatOutputFactory {
    http: Arc<Http>,
}

impl SerenityChatOutputFactory {
    pub fn new(http: Http) -> Self {
        Self {
            http: Arc::new(http),
        }
    }
}

impl ChatOutputFactoryPort for SerenityChatOutputFactory {
    fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort> {
        Arc::new(SerenityChatOutput {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
        })
    }
}

struct SerenityChatOutput {
    http: Arc<Http>,
    channel_id: SerenityChannelId,
}

#[async_trait]
impl ChatOutputPort for SerenityChatOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        self.http
            .send_message(
                self.channel_id,
                Vec::new(),
                &CreateMessage::new().content(message.content),
            )
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}
