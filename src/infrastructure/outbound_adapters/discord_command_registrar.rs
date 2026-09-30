use serenity::all::Http;

use crate::kernel::{models::OutboundError, plugin_ports::CommandDescriptor};

/// Pushes the aggregated command descriptors to Discord as global
/// application (slash) commands. Discord-specific by design: no universal
/// registration port exists; each platform adapter wires this itself at its
/// composition root.
pub struct DiscordCommandRegistrar {
    http: Http,
}

impl DiscordCommandRegistrar {
    pub fn new(http: Http) -> Self {
        Self { http }
    }

    /// Bulk-overwrites global application commands with the registry's
    /// descriptors. All arguments are registered as Discord STRING options;
    /// resolved entities (users, channels, ...) arrive as ID strings.
    pub async fn sync(&self, descriptors: &[CommandDescriptor]) -> Result<(), OutboundError> {
        let commands: Vec<serde_json::Value> = descriptors
            .iter()
            .map(|descriptor| {
                // Discord rejects an empty `options` array - omit it when there
                // are no arguments.
                let options: Vec<serde_json::Value> = descriptor
                    .arguments
                    .iter()
                    .map(|argument| {
                        serde_json::json!({
                            "name": argument.name,
                            "description": argument.description,
                            "type": 3, // STRING
                            "required": argument.required,
                        })
                    })
                    .collect();

                let mut command = serde_json::Map::new();
                command.insert("name".to_owned(), serde_json::json!(descriptor.name));
                command.insert("description".to_owned(), serde_json::json!(descriptor.description));
                // Platform-native permission gating: Discord itself hides the
                // command from members lacking the permission. Kernel-side
                // per-command ACL checks are the auth plugin's territory.
                if let Some(bits) = descriptor
                    .required_permission
                    .as_ref()
                    .and_then(|permission| discord_permission_bits(&permission.name))
                {
                    command
                        .insert("default_member_permissions".to_owned(), serde_json::json!(bits));
                }
                if !options.is_empty() {
                    command.insert("options".to_owned(), serde_json::json!(options));
                }

                serde_json::Value::Object(command)
            })
            .collect();

        self.http
            .create_global_commands(&serde_json::json!(commands))
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}

/// Maps kernel permission names onto Discord permission bits (API v10 expects
/// `default_member_permissions` as a string). Unknown names are ignored:
/// the descriptor stays declarative, enforcement beyond Discord's own gating
/// is not this adapter's business.
fn discord_permission_bits(name: &str) -> Option<&'static str> {
    match name {
        "administrator" => Some("8"), // ADMINISTRATOR
        "manage_guild" => Some("32"), // MANAGE_GUILD
        _ => None,
    }
}
