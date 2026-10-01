use std::sync::Arc;

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
    http: Arc<Http>,
}

impl DiscordCommandRegistrar {
    /// Takes the shared REST client: serenity rate limiting is per `Http`,
    /// so every driven Discord caller must share one instance.
    pub fn new(http: Arc<Http>) -> Self {
        Self { http }
    }

    /// Bulk-overwrites global application commands with the registry's
    /// descriptors. Resolved entities (users, roles, ...) arrive as ID
    /// strings; string arguments may carry plugin-declared choices.
    ///
    /// # Errors
    /// Fails when Discord rejects the bulk overwrite; the caller aborts boot.
    pub async fn sync(&self, descriptors: &[CommandDescriptor]) -> Result<(), OutboundError> {
        let commands: Vec<serde_json::Value> =
            descriptors.iter().map(application_command_json).collect();

        let started = std::time::Instant::now();
        self.http
            .create_global_commands(&serde_json::json!(commands))
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        // The registration trail: each local registration is logged by the
        // registry; this is the platform-side completion of the same event.
        tracing::info!(
            count = descriptors.len(),
            elapsed_ms = started.elapsed().as_millis(),
            "global slash commands synced to Discord"
        );
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
            // Discord accepts `choices` only on string options - a plugin bug
            // must not fail the whole bulk sync at boot, so other kinds are
            // logged and dropped.
            if let Some(choices) = &argument.choices {
                if argument.kind == ArgKind::String {
                    let rendered: Vec<serde_json::Value> = choices
                        .iter()
                        .map(|choice| serde_json::json!({ "name": choice, "value": choice }))
                        .collect();
                    option.insert("choices".to_owned(), serde_json::json!(rendered));
                } else {
                    tracing::warn!(
                        command = %descriptor.name,
                        argument = %argument.name,
                        "choices ignored: Discord supports them only on string arguments"
                    );
                }
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
    match descriptor
        .required_permission
        .as_ref()
        .map(|permission| (permission.name.as_str(), permission_bits(&permission.name)))
    {
        Some((_, Some(bits))) => {
            command.insert("default_member_permissions".to_owned(), serde_json::json!(bits));
        }
        // A typo'd permission name must not silently publish an ungated
        // command - make the gap visible until kernel-side ACL exists.
        Some((name, None)) => tracing::warn!(
            command = %descriptor.name,
            permission = name,
            "required_permission has no Discord mapping - published without a platform gate"
        ),
        None => {}
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
        ArgKind::Attachment => 11,
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

    /// Discord accepts `choices` only on string options: the adapter must
    /// drop them elsewhere instead of failing the whole sync at boot.
    #[test]
    fn choices_are_attached_only_to_string_arguments() {
        let descriptor = CommandDescriptor {
            plugin_id: "test".to_owned(),
            name: "x".to_owned(),
            description: "test".to_owned(),
            arguments: vec![
                ArgDescriptor {
                    name: "action".to_owned(),
                    description: "what".to_owned(),
                    required: true,
                    kind: ArgKind::String,
                    choices: Some(vec!["a".to_owned()]),
                },
                ArgDescriptor {
                    name: "user".to_owned(),
                    description: "who".to_owned(),
                    required: false,
                    kind: ArgKind::User,
                    choices: Some(vec!["b".to_owned()]),
                },
            ],
            required_permission: None,
            guild_only: false,
        };

        let json = application_command_json(&descriptor);
        let options = field(&json, "options").as_array().expect("options expected");

        let action = options.first().expect("action option expected");
        assert!(action.get("choices").is_some());
        let user = options.get(1).expect("user option expected");
        assert!(user.get("choices").is_none());
    }
}
