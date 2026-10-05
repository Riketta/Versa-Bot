use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::kernel::plugin_ports::{CommandDescriptor, CommandHandler, CommandRegistryPort};

/// The single active `CommandRegistryPort` adapter: name-keyed, last
/// registration wins (logged). Anything the platform sync or the dispatcher
/// needs comes back out through the same trait.
#[derive(Default)]
pub struct InMemoryCommandRegistry {
    /// Name-keyed, sorted: `descriptors()` order is deterministic, so the
    /// Discord bulk sync payload is reproducible across boots.
    commands: RwLock<BTreeMap<String, (CommandDescriptor, Arc<dyn CommandHandler>)>>,
}

impl InMemoryCommandRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl CommandRegistryPort for InMemoryCommandRegistry {
    fn register(&self, descriptor: CommandDescriptor, handler: Arc<dyn CommandHandler>) {
        let name = descriptor.name.clone();
        let new_plugin_id = descriptor.plugin_id.clone();
        let argument_count = descriptor.arguments.len();
        let mut commands = self.commands.write();
        if let Some((previous, _)) = commands.insert(name.clone(), (descriptor, handler)) {
            tracing::warn!(
                command = %name,
                previous_plugin = %previous.plugin_id,
                new_plugin = %new_plugin_id,
                "command name collision: previous handler replaced (last wins)"
            );
        } else {
            tracing::info!(
                plugin = %new_plugin_id,
                command = %name,
                arguments = argument_count,
                "command registered"
            );
        }
    }

    fn lookup(&self, name: &str) -> Option<Arc<dyn CommandHandler>> {
        self.commands.read().get(name).map(|(_, handler)| Arc::clone(handler))
    }

    fn descriptor(&self, name: &str) -> Option<CommandDescriptor> {
        self.commands.read().get(name).map(|(descriptor, _)| descriptor.clone())
    }

    fn descriptors(&self) -> Vec<CommandDescriptor> {
        self.commands.read().values().map(|(d, _)| d.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    use crate::kernel::models::{
        ChannelId, CommandPayload, EventKind, EventPayload, GuildId, MessageId, Origin,
        RequestContext, UserId,
    };
    use crate::kernel::plugin_ports::CommandArgs;
    use crate::kernel::services::KernelServices;
    use crate::kernel::spi_ports::ChatOutputPort;
    use crate::test_support::{
        RecordingChatOutput, RecordingChatOutputFactory, test_platform_info,
    };

    struct NoopHandler;

    #[async_trait::async_trait]
    impl CommandHandler for NoopHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            _args: &CommandArgs,
            _services: &KernelServices,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn descriptor(name: &str) -> CommandDescriptor {
        CommandDescriptor {
            plugin_id: "test".to_owned(),
            name: name.to_owned(),
            description: "test command".to_owned(),
            arguments: Vec::new(),
            required_permission: None,
            required_tier: None,
            guild_only: false,
        }
    }

    /// Records its tag through a shared log - the fixture that makes WHICH
    /// handler ran observable (`NoopHandler`s are indistinguishable).
    struct RecordingHandler {
        tag: &'static str,
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl CommandHandler for RecordingHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            _args: &CommandArgs,
            _services: &KernelServices,
        ) -> anyhow::Result<()> {
            self.log.lock().push(self.tag);
            Ok(())
        }
    }

    fn command_event() -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: Origin {
                guild_id: Some(GuildId(1)),
                channel_id: ChannelId(2),
                user_id: UserId(3),
                message_id: Some(MessageId(4)),
                reply_token: None,
            },
            payload: EventPayload::Command(CommandPayload {
                name: "ping".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        }
    }

    /// The dispatch context for `invoke` - the handler under test never
    /// replies, so no guild storage is needed (DM-shaped services).
    fn services() -> KernelServices {
        let output = RecordingChatOutput::new();
        KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: None,
            platform_info: test_platform_info(),
        }
    }

    #[test]
    fn register_lookup_roundtrip() {
        let registry = InMemoryCommandRegistry::new();

        registry.register(descriptor("ping"), Arc::new(NoopHandler));

        assert!(registry.lookup("ping").is_some());
        assert!(registry.lookup("unknown").is_none());
        assert_eq!(registry.descriptor("ping").map(|d| d.name), Some("ping".to_owned()));
        assert!(registry.descriptor("unknown").is_none());
        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(descriptors.first().expect("command expected registered").name, "ping");
    }

    #[test]
    fn re_registration_replaces_handler() {
        let registry = InMemoryCommandRegistry::new();

        registry.register(descriptor("ping"), Arc::new(NoopHandler));
        registry.register(descriptor("ping"), Arc::new(NoopHandler));

        assert_eq!(registry.descriptors().len(), 1, "same name must not duplicate");
    }

    /// Contract: last registration wins AND the winner is actually served -
    /// a regression keeping the replaced handler must be observable, so both
    /// handlers are tagged and the invocation traced through a shared log.
    #[tokio::test]
    async fn re_registration_serves_the_new_handler() {
        let registry = InMemoryCommandRegistry::new();
        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

        registry.register(
            descriptor("ping"),
            Arc::new(RecordingHandler { tag: "old", log: Arc::clone(&log) }),
        );
        registry.register(
            descriptor("ping"),
            Arc::new(RecordingHandler { tag: "new", log: Arc::clone(&log) }),
        );

        let handler = registry.lookup("ping").expect("command expected registered");
        handler
            .invoke(&command_event(), &CommandArgs::default(), &services())
            .await
            .expect("invoke expected to succeed");

        assert_eq!(
            *log.lock(),
            vec!["new"],
            "lookup must yield the latest registration, never the replaced one"
        );
    }

    /// Contract: `descriptors()` is name-sorted regardless of registration
    /// order - the deterministic Discord bulk sync payload across boots.
    #[test]
    fn descriptors_are_name_sorted_regardless_of_registration_order() {
        let registry = InMemoryCommandRegistry::new();

        registry.register(descriptor("zebra"), Arc::new(NoopHandler));
        registry.register(descriptor("midway"), Arc::new(NoopHandler));
        registry.register(descriptor("alpha"), Arc::new(NoopHandler));

        let names: Vec<String> = registry.descriptors().into_iter().map(|d| d.name).collect();
        assert_eq!(
            names,
            vec!["alpha".to_owned(), "midway".to_owned(), "zebra".to_owned()],
            "descriptors must come back sorted by name"
        );
    }
}
