use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::kernel::plugin_ports::{
    CommandDescriptor, CommandHandler, CommandRegistryPort,
};

/// The single active `CommandRegistryPort` adapter: name-keyed, last
/// registration wins (logged). Anything the platform sync or the dispatcher
/// needs comes back out through the same trait.
#[derive(Default)]
pub struct InMemoryCommandRegistry {
    commands: RwLock<HashMap<String, (CommandDescriptor, Arc<dyn CommandHandler>)>>,
}

impl InMemoryCommandRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl CommandRegistryPort for InMemoryCommandRegistry {
    fn register(&self, descriptor: CommandDescriptor, handler: Arc<dyn CommandHandler>) {
        let mut commands = self.commands.write();
        if commands
            .insert(descriptor.name.clone(), (descriptor, handler))
            .is_some()
        {
            tracing::debug!("command re-registered, previous handler replaced");
        }
    }

    fn lookup(&self, name: &str) -> Option<Arc<dyn CommandHandler>> {
        self.commands
            .read()
            .get(name)
            .map(|(_, handler)| Arc::clone(handler))
    }

    fn descriptors(&self) -> Vec<CommandDescriptor> {
        self.commands.read().values().map(|(d, _)| d.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::plugin_ports::CommandArgs;
    use crate::kernel::services::KernelServices;

    struct NoopHandler;

    #[async_trait::async_trait]
    impl CommandHandler for NoopHandler {
        async fn invoke(&self, _args: &CommandArgs, _services: &KernelServices) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn descriptor(name: &str) -> CommandDescriptor {
        CommandDescriptor {
            plugin_id: "test".to_owned(),
            name: name.to_owned(),
            aliases: None,
            description: "test command".to_owned(),
            arguments: Vec::new(),
            required_permission: None,
        }
    }

    #[test]
    fn register_lookup_roundtrip() {
        let registry = InMemoryCommandRegistry::new();

        registry.register(descriptor("ping"), Arc::new(NoopHandler));

        assert!(registry.lookup("ping").is_some());
        assert!(registry.lookup("unknown").is_none());
        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 1);
        assert_eq!(
            descriptors.first().expect("command expected registered").name,
            "ping"
        );
    }

    #[test]
    fn re_registration_replaces_handler() {
        let registry = InMemoryCommandRegistry::new();

        registry.register(descriptor("ping"), Arc::new(NoopHandler));
        registry.register(descriptor("ping"), Arc::new(NoopHandler));

        assert_eq!(registry.descriptors().len(), 1, "same name must not duplicate");
    }
}
