use serenity::all::Http;

use crate::kernel::{
    models::OutboundError,
    plugin_ports::{ArgKind, CommandDescriptor},
};

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
    /// descriptors. Resolved entities (users, roles, ...) arrive as ID
    /// strings; string arguments may carry plugin-declared choices.
    pub async fn sync(&self, descriptors: &[CommandDescriptor]) -> Result<(), OutboundError> {
        let commands: Vec<serde_json::Value> =
            descriptors.iter().map(application_command_json).collect();

        self.http
            .create_global_commands(&serde_json::json!(commands))
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}

/// Maps a plugin-declared descriptor onto Discord's application command
/// JSON. Pure so the mapping is unit-testable without HTTP.
fn application_command_json(descriptor: &CommandDescriptor) -> serde_json::Value {
    let options: Vec<serde_json::Value> = descriptor
        .arguments
        .iter()
        .map(|argument| {
            let mut option = serde_json::Map::new();
            option.insert("name".to_owned(), serde_json::json!(argument.name));
            option.insert("description".to_owned(), serde_json::json!(argument.description));
            option.insert("type".to_owned(), serde_json::json!(option_type(argument.kind)));
            option.insert("required".to_owned(), serde_json::json!(argument.required));
            if let Some(choices) = &argument.choices {
                let rendered: Vec<serde_json::Value> = choices
                    .iter()
                    .map(|choice| serde_json::json!({ "name": choice, "value": choice }))
                    .collect();
                option.insert("choices".to_owned(), serde_json::json!(rendered));
            }
            serde_json::Value::Object(option)
        })
        .collect();

    let mut command = serde_json::Map::new();
    command.insert("name".to_owned(), serde_json::json!(descriptor.name));
    command.insert("description".to_owned(), serde_json::json!(descriptor.description));
    // Platform-native permission gating: Discord itself hides the command
    // from members lacking the permission. Kernel-side per-command ACL
    // checks are the auth plugin's territory.
    if let Some(bits) = descriptor
        .required_permission
        .as_ref()
        .and_then(|permission| permission_bits(&permission.name))
    {
        command.insert("default_member_permissions".to_owned(), serde_json::json!(bits));
    }
    command.insert("dm_permission".to_owned(), serde_json::json!(!descriptor.guild_only));
    if !options.is_empty() {
        command.insert("options".to_owned(), serde_json::json!(options));
    }

    serde_json::Value::Object(command)
}

/// Maps kernel argument kinds onto Discord option types.
fn option_type(kind: ArgKind) -> u8 {
    match kind {
        ArgKind::String => 3,
        ArgKind::User => 6,
        ArgKind::Role => 8,
    }
}

/// Maps kernel permission names onto Discord permission bits (API v10 expects
/// `default_member_permissions` as a string). Unknown names are ignored:
/// the descriptor stays declarative, enforcement beyond Discord's own gating
/// is not this adapter's business.
fn permission_bits(name: &str) -> Option<&'static str> {
    match name {
        "administrator" => Some("8"), // ADMINISTRATOR
        "manage_guild" => Some("32"), // MANAGE_GUILD
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::plugin_ports::{ArgDescriptor, Permission};

    /// Panic-free field access: the deny-level `indexing_slicing` lint also
    /// applies to `serde_json::Value` indexing in tests.
    fn field<'a>(value: &'a serde_json::Value, key: &str) -> &'a serde_json::Value {
        value.get(key).unwrap_or(&serde_json::Value::Null)
    }

    #[test]
    fn descriptor_maps_onto_application_command_json() {
        let descriptor = CommandDescriptor {
            plugin_id: "auth".to_owned(),
            name: "auth".to_owned(),
            aliases: None,
            description: "Manage access".to_owned(),
            arguments: vec![
                ArgDescriptor {
                    name: "action".to_owned(),
                    description: "What to do".to_owned(),
                    required: true,
                    kind: ArgKind::String,
                    choices: Some(vec!["allow".to_owned(), "deny".to_owned()]),
                },
                ArgDescriptor {
                    name: "user".to_owned(),
                    description: "Target user".to_owned(),
                    required: false,
                    kind: ArgKind::User,
                    choices: None,
                },
            ],
            required_permission: Some(Permission { name: "manage_guild".to_owned() }),
            guild_only: true,
        };

        let json = application_command_json(&descriptor);

        assert_eq!(field(&json, "name").as_str(), Some("auth"));
        assert_eq!(field(&json, "default_member_permissions").as_str(), Some("32"));
        assert_eq!(field(&json, "dm_permission").as_bool(), Some(false));
        let options = field(&json, "options").as_array().expect("options expected");
        assert_eq!(options.len(), 2);
        let action = options.first().expect("action option expected");
        assert_eq!(field(action, "type").as_u64(), Some(3));
        let choices = field(action, "choices").as_array().expect("choices expected");
        let deny = choices.get(1).expect("deny choice expected");
        assert_eq!(field(deny, "value").as_str(), Some("deny"));
        let user = options.get(1).expect("user option expected");
        assert_eq!(field(user, "type").as_u64(), Some(6));
    }

    #[test]
    fn dm_enabled_command_omits_nothing_but_defaults() {
        let descriptor = CommandDescriptor {
            plugin_id: "command".to_owned(),
            name: "ping".to_owned(),
            aliases: None,
            description: "Pong".to_owned(),
            arguments: Vec::new(),
            required_permission: None,
            guild_only: false,
        };

        let json = application_command_json(&descriptor);

        assert_eq!(field(&json, "dm_permission").as_bool(), Some(true));
        assert!(json.get("options").is_none());
        assert!(json.get("default_member_permissions").is_none());
    }
}
