pub trait CommandRegistryPort: Send + Sync {
    // fn register(&self, descriptor: CommandDescriptor) -> Result<()>;
    // fn unregister(&self, plugin_id: &str, command_name: &str) -> Result<()>;
    // fn list_commands(&self, plugin_id: Option<&str>) -> Vec<CommandDescriptor>;
}

pub struct CommandDescriptor {
    pub plugin_id: String,
    pub name: String,
    pub aliases: Option<Vec<String>>,
    pub description: String,
    pub arguments: Vec<ArgDescriptor>,
    pub required_permission: Option<Permission>,
}

#[derive(Debug, Clone)]
pub struct ArgDescriptor {
    pub name: String,
    pub description: String,
    pub required: bool,
}

/// Placeholder until `AuthPlugin` lands.
#[derive(Debug, Clone)]
pub struct Permission {
    pub name: String,
}
