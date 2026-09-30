use std::sync::Arc;

use async_trait::async_trait;

use crate::kernel::{
    models::{Embed, EventPayload, OutboundMessage, RequestContext},
    plugin_ports::{
        CommandArgs, CommandDescriptor, CommandHandler, CommandRegistryPort, MiddlewarePluginPort,
        Next, PluginPort,
    },
    services::KernelServices,
};

/// Command dispatcher: routes native [`EventKind::CommandInvoked`] events to
/// handlers registered in the `CommandRegistryPort`. Meaning lives in the
/// owning plugins; this plugin only dispatches. An unknown command yields no
/// output - there is no "not found" default. A handler failure on a
/// transactional invocation (reply token) sends a generic ephemeral failure
/// notice; plain events stay silent.
pub struct CommandPlugin {
    registry: Arc<dyn CommandRegistryPort>,
}

impl CommandPlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>) -> Self {
        Self { registry }
    }
}

impl PluginPort for CommandPlugin {
    fn name(&self) -> &'static str {
        "command"
    }

    fn init(&self) -> Result<(), crate::kernel::models::PluginError> {
        // Demo command proving the full loop (register -> Discord sync ->
        // interaction -> dispatch -> respond). Real commands belong to the
        // feature plugins that own their meaning.
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "ping".to_owned(),
                description: "Replies with Pong!".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                guild_only: false,
            },
            Arc::new(PingHandler),
        );
        Ok(())
    }
}

#[async_trait]
impl MiddlewarePluginPort for CommandPlugin {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        let EventPayload::Command(command) = &event.payload else {
            return Next::Continue;
        };

        let Some(handler) = self.registry.lookup(&command.name) else {
            return Next::Continue;
        };

        let args = CommandArgs(command.args.clone());
        if let Err(err) = handler.invoke(event, &args, services).await {
            tracing::error!(command = %command.name, %err, "command handler failed");
            // Transactional invocations (slash commands) owe the platform an
            // answer: a generic ephemeral notice keeps the interaction from
            // hanging; details stay in the log, not in the channel. Plain
            // events stay silent, like denied plain messages.
            if event.origin.reply_token.is_some() {
                let notice = OutboundMessage::embed(Embed {
                    title: "⚠️ Command failed".to_owned(),
                    description: "The command could not be executed. Try again later.".to_owned(),
                })
                .ephemeral();
                if let Err(notice_err) = services.chat_output.send(notice).await {
                    tracing::warn!(%notice_err, "failed to deliver command failure notice");
                }
            }
        }

        // Deliberate handling: like auth, a resolved command stops the chain.
        Next::Stop
    }
}

struct PingHandler;

#[async_trait]
impl CommandHandler for PingHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        services.chat_output.send(OutboundMessage::text("Pong!")).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryCommandRegistry;
    use crate::kernel::{
        models::{ChannelId, EventKind, GuildId, MessageId, Origin, Platform, UserId},
        spi_ports::StoragePort,
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
    use std::sync::Arc;

    struct StaticHandler {
        reply: &'static str,
    }

    #[async_trait]
    impl CommandHandler for StaticHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            _args: &CommandArgs,
            services: &KernelServices,
        ) -> anyhow::Result<()> {
            services.chat_output.send(OutboundMessage::text(self.reply)).await?;
            Ok(())
        }
    }

    fn origin() -> Origin {
        Origin {
            platform: Platform::Discord,
            guild_id: Some(GuildId(1)),
            channel_id: ChannelId(2),
            user_id: UserId(3),
            message_id: Some(MessageId(4)),
            reply_token: None,
        }
    }

    fn command_event(name: &str, args: &[(&str, &str)]) -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: origin(),
            payload: EventPayload::Command(crate::kernel::models::CommandPayload {
                name: name.to_owned(),
                args: args.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        }
    }

    fn registry_with(command: &str, reply: &'static str) -> Arc<InMemoryCommandRegistry> {
        let registry = InMemoryCommandRegistry::new();
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: command.to_owned(),
                description: "test".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                guild_only: false,
            },
            Arc::new(StaticHandler { reply }),
        );
        Arc::new(registry)
    }

    fn test_services(output: &Arc<RecordingChatOutput>) -> KernelServices {
        let storage = InMemoryStorage::new();
        KernelServices {
            chat_output: Arc::clone(output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(output)).boxed(),
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        }
    }

    #[tokio::test]
    async fn registered_command_is_dispatched_with_args() {
        let registry = registry_with("greet", "hello!");
        let plugin = CommandPlugin::new(registry);
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("greet", &[("target", "world")]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        assert_eq!(output.messages(), ["hello!"]);
    }

    #[tokio::test]
    async fn handler_receives_arguments_by_name() {
        let registry = InMemoryCommandRegistry::new();
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: "echo".to_owned(),
                description: "test".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                guild_only: false,
            },
            Arc::new(EchoTextHandler),
        );
        let plugin = CommandPlugin::new(Arc::new(registry));
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("echo", &[("text", "privit")]);

        let _ = plugin.pre(&mut event, &services).await;

        assert_eq!(output.messages(), ["privit"]);
    }

    #[tokio::test]
    async fn unknown_command_continues_without_output() {
        let plugin = CommandPlugin::new(Arc::new(InMemoryCommandRegistry::new()));
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("unknown", &[]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn non_command_events_are_ignored() {
        let registry = registry_with("greet", "hello!");
        let plugin = CommandPlugin::new(registry);
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = RequestContext::message_received(origin(), "hello");

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Continue));
        assert!(output.messages().is_empty());
    }

    struct EchoTextHandler;

    #[async_trait]
    impl CommandHandler for EchoTextHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            args: &CommandArgs,
            services: &KernelServices,
        ) -> anyhow::Result<()> {
            let text = args.get("text").unwrap_or("nothing");
            services.chat_output.send(OutboundMessage::text(text)).await?;
            Ok(())
        }
    }

    struct FailingHandler;

    #[async_trait]
    impl CommandHandler for FailingHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            _args: &CommandArgs,
            _services: &KernelServices,
        ) -> anyhow::Result<()> {
            Err(anyhow::anyhow!("boom"))
        }
    }

    fn failing_registry() -> Arc<InMemoryCommandRegistry> {
        let registry = InMemoryCommandRegistry::new();
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: "fail".to_owned(),
                description: "test".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                guild_only: false,
            },
            Arc::new(FailingHandler),
        );
        Arc::new(registry)
    }

    /// A handler failure on a transactional invocation answers the invoker
    /// with exactly one generic ephemeral notice; details stay in the log.
    #[tokio::test]
    async fn failing_handler_answers_ephemerally_with_reply_token() {
        let plugin = CommandPlugin::new(failing_registry());
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("fail", &[]);
        event.origin.reply_token = Some("token".to_owned());

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        let sent = output.sent();
        assert_eq!(sent.len(), 1, "exactly one failure notice expected");
        let notice = sent.first().expect("notice expected");
        assert!(notice.ephemeral, "failure notice must be visible to the invoker only");
        let text = output.messages().into_iter().next().expect("notice expected");
        assert!(text.contains("⚠️ Command failed"));
        assert!(text.contains("Try again later."));
    }

    /// Plain events have no transactional answer: a handler failure stays
    /// silent (a public rejection would be a spam vector).
    #[tokio::test]
    async fn failing_handler_without_reply_token_stays_silent() {
        let plugin = CommandPlugin::new(failing_registry());
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("fail", &[]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        assert!(output.messages().is_empty());
    }
}
