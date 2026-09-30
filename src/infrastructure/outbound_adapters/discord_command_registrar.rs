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

                if options.is_empty() {
                    serde_json::json!({
                        "name": descriptor.name,
                        "description": descriptor.description,
                    })
                } else {
                    serde_json::json!({
                        "name": descriptor.name,
                        "description": descriptor.description,
                        "options": options,
                    })
                }
            })
            .collect();

        self.http
            .create_global_commands(&serde_json::json!(commands))
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}
