use async_trait::async_trait;

use crate::kernel::{
    models::{EventKind, EventPayload, OutboundMessage, RequestContext},
    plugin_ports::{MiddlewarePluginPort, Next, PluginPort},
    services::KernelServices,
};

/// Middleware plugin owning the chat command system: matches
/// `MessageReceived` events starting with the configured prefix, executes
/// the command, replies via the event-scoped `ChatOutputPort`, and stops the
/// chain (no other plugin should react to a handled command).
pub struct CommandPlugin {
    prefix: String,
}

impl CommandPlugin {
    #[must_use]
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl PluginPort for CommandPlugin {
    fn name(&self) -> &'static str {
        "command"
    }
}

#[async_trait]
impl MiddlewarePluginPort for CommandPlugin {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        if event.kind != EventKind::MessageReceived {
            return Next::Continue;
        }

        let EventPayload::Message(message) = &event.payload else {
            return Next::Continue;
        };

        let Some(command) = message.content.strip_prefix(self.prefix.as_str()) else {
            return Next::Continue;
        };

        match command.trim() {
            "ping" => {
                if let Err(err) = services
                    .chat_output
                    .send(OutboundMessage::text("Pong!"))
                    .await
                {
                    tracing::error!(%err, "failed to send command reply");
                }
                Next::Stop
            }
            _ => Next::Continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{
        models::{ChannelId, GuildId, MessageId, OutboundError, Origin, Platform, UserId},
        spi_ports::ChatOutputPort,
    };
    use parking_lot::Mutex;
    use std::sync::Arc;

    struct FakeChatOutput {
        messages: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ChatOutputPort for FakeChatOutput {
        async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
            self.messages.lock().push(message.content);
            Ok(())
        }
    }

    fn test_origin() -> Origin {
        Origin {
            platform: Platform::Discord,
            guild_id: Some(GuildId(1)),
            channel_id: ChannelId(2),
            user_id: UserId(3),
            message_id: Some(MessageId(4)),
        }
    }

    fn test_services(messages: Arc<Mutex<Vec<String>>>) -> KernelServices {
        KernelServices {
            chat_output: Arc::new(FakeChatOutput { messages }),
        }
    }

    #[tokio::test]
    async fn ping_command_replies_and_stops() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let services = test_services(Arc::clone(&messages));
        let plugin = CommandPlugin::new("!");
        let mut event = RequestContext::message_received(test_origin(), "!ping");

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        assert_eq!(messages.lock().as_slice(), ["Pong!"]);
    }

    #[tokio::test]
    async fn unknown_command_and_plain_message_continue_without_output() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        let services = test_services(Arc::clone(&messages));
        let plugin = CommandPlugin::new("!");

        let mut unknown = RequestContext::message_received(test_origin(), "!unknown");
        assert!(matches!(
            plugin.pre(&mut unknown, &services).await,
            Next::Continue
        ));

        let mut plain = RequestContext::message_received(test_origin(), "hello");
        assert!(matches!(
            plugin.pre(&mut plain, &services).await,
            Next::Continue
        ));

        assert!(messages.lock().is_empty());
    }
}
