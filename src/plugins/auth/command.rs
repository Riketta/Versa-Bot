//! `/auth` - the interactive half of the auth plugin. Writes the same policy
//! document the middleware gate reads; meaning (who may use the bot) lives
//! in this plugin, not in the dispatcher.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;

use crate::kernel::{
    models::{Embed, OutboundMessage, RequestContext},
    plugin_ports::{CommandArgs, CommandHandler},
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::{AuthConfig, CONFIG_KEY, NAMESPACE};

/// `/auth action:[allow|deny|show] [user] [role]` - manage the guild's bot
/// access policy. Runs through the pipeline like every command, so the auth
/// gate has already vetted the invoker; Discord additionally hides the
/// command behind Manage Server (`default_member_permissions`). Every
/// answer is ephemeral: policy data stays between the bot and the admin.
/// Policy writes serialize on one plugin-wide lock: the read-modify-write of
/// the policy document must not lose one of two concurrent admin updates.
pub struct AuthCommandHandler {
    policy_writes: Arc<AsyncMutex<()>>,
}

impl AuthCommandHandler {
    #[must_use]
    pub fn new() -> Self {
        Self { policy_writes: Arc::new(AsyncMutex::new(())) }
    }
}

impl Default for AuthCommandHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CommandHandler for AuthCommandHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = &services.guild_storage else {
            return reply(services, text("This command only works inside a server.")).await;
        };

        match args.get("action") {
            Some("show") => show_policy(&**storage, services).await,
            Some(action @ ("allow" | "deny")) => {
                mutate_policy(&**storage, &self.policy_writes, services, args, action).await
            }
            _ => reply(services, usage()).await,
        }
    }
}

enum Target {
    User(String),
    Role(String),
}

async fn mutate_policy(
    storage: &dyn GuildStorage,
    policy_writes: &AsyncMutex<()>,
    services: &KernelServices,
    args: &CommandArgs,
    action: &str,
) -> anyhow::Result<()> {
    // One write at a time: allow/deny is a document read-modify-write, and
    // two concurrent invocations must not lose one update.
    let _write = policy_writes.lock().await;
    let target = match (args.get("user"), args.get("role")) {
        (Some(user), None) => Target::User(user.to_owned()),
        (None, Some(role)) => Target::Role(role.to_owned()),
        (Some(_), Some(_)) => {
            return reply(services, text("Specify either `user` or `role`, not both.")).await;
        }
        (None, None) => return reply(services, usage()).await,
    };

    // Read-modify-write of the policy document. The document is never
    // deleted: an empty policy admits only guild administrators (see the
    // gate in `super`), a missing one would re-open the guild.
    let mut policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };

    let id = match &target {
        Target::User(id) | Target::Role(id) => id.clone(),
    };
    let (list, mention, noun) = match &target {
        Target::User(id) => (&mut policy.allowed_users, format!("<@{id}>"), "user"),
        Target::Role(id) => (&mut policy.allowed_roles, format!("<@&{id}>"), "role"),
    };
    let listed = list.iter().any(|candidate| candidate == &id);

    let answer = if action == "allow" {
        if listed {
            format!("{noun} {mention} is already allowed.")
        } else {
            list.push(id);
            storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&policy)?).await?;
            format!("✅ {noun} {mention} can now use the bot.")
        }
    } else if listed {
        list.retain(|candidate| candidate != &id);
        // An emptied policy silently widens access: the gate's empty-policy
        // fallback admits every Discord guild administrator. Say so.
        let emptied = policy.allowed_users.is_empty() && policy.allowed_roles.is_empty();
        storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&policy)?).await?;
        if emptied {
            format!(
                "🚫 {noun} {mention} is no longer allowed.\n⚠️ The policy is now empty: only \
                 Discord guild administrators can use the bot."
            )
        } else {
            format!("🚫 {noun} {mention} is no longer allowed.")
        }
    } else {
        format!("{noun} {mention} was not in the policy.")
    };

    reply(services, text(answer)).await
}

async fn show_policy(storage: &dyn GuildStorage, services: &KernelServices) -> anyhow::Result<()> {
    let policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };

    reply(
        services,
        OutboundMessage::embed(Embed {
            title: "🛡 Bot access policy".to_owned(),
            description: format!(
                "Allowed users: {}\nAllowed roles: {}",
                render_list(&policy.allowed_users, user_mention),
                render_list(&policy.allowed_roles, role_mention),
            ),
        })
        .ephemeral(),
    )
    .await
}

/// Reads the policy document. Any failure - storage error or malformed JSON -
/// yields the ephemeral "unavailable" answer instead of a policy: writes
/// must never replace an unreadable policy with a blank one.
async fn read_policy(storage: &dyn GuildStorage) -> Result<AuthConfig, OutboundMessage> {
    let value = match storage.get(NAMESPACE, CONFIG_KEY).await {
        Ok(None) => return Ok(AuthConfig::default()),
        Ok(Some(value)) => value,
        Err(_) => return Err(unavailable()),
    };
    serde_json::from_value(value).map_err(|_| unavailable())
}

fn render_list(ids: &[String], mention: fn(&str) -> String) -> String {
    if ids.is_empty() {
        return "none".to_owned();
    }
    ids.iter().map(|id| mention(id)).collect::<Vec<_>>().join(", ")
}

fn user_mention(id: &str) -> String {
    format!("<@{id}>")
}

fn role_mention(id: &str) -> String {
    format!("<@&{id}>")
}

fn unavailable() -> OutboundMessage {
    text(
        "This guild's access policy is unreadable. Ask a guild admin to fix the bot configuration.",
    )
}

fn usage() -> OutboundMessage {
    text("Usage: `/auth action:show`, or `/auth action:allow|deny` with a `user` or a `role`.")
}

/// Every `/auth` answer is ephemeral - visible to the invoking admin only.
fn text(content: impl Into<String>) -> OutboundMessage {
    OutboundMessage::text(content).ephemeral()
}

async fn reply(services: &KernelServices, message: OutboundMessage) -> anyhow::Result<()> {
    services.chat_output.send(message).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryCommandRegistry;
    use crate::kernel::{
        models::{
            ChannelId, CommandPayload, EventKind, EventPayload, GuildId, MessageId, Origin,
            Platform, UserId,
        },
        plugin_ports::{ArgKind, CommandRegistryPort as _, PluginPort as _},
        spi_ports::{ChatOutputPort, StoragePort},
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
    use std::sync::Arc;

    use crate::plugins::auth::AuthPlugin;

    fn fixture() -> (Arc<InMemoryStorage>, KernelServices, Arc<RecordingChatOutput>) {
        let storage = Arc::new(InMemoryStorage::new());
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: Some(storage.guild_scoped(Platform::Discord, GuildId(1))),
        };
        (storage, services, output)
    }

    fn dm_services(output: &Arc<RecordingChatOutput>) -> KernelServices {
        KernelServices {
            chat_output: Arc::clone(output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(output)).boxed(),
            guild_storage: None,
        }
    }

    fn auth_args(action: &str, user: Option<&str>, role: Option<&str>) -> CommandArgs {
        let mut pairs = vec![("action".to_owned(), action.to_owned())];
        if let Some(user) = user {
            pairs.push(("user".to_owned(), user.to_owned()));
        }
        if let Some(role) = role {
            pairs.push(("role".to_owned(), role.to_owned()));
        }
        CommandArgs(pairs)
    }

    fn command_event() -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: Origin {
                platform: Platform::Discord,
                guild_id: Some(GuildId(1)),
                channel_id: ChannelId(2),
                user_id: UserId(3),
                message_id: Some(MessageId(4)),
                reply_token: Some("token".to_owned()),
            },
            payload: EventPayload::Command(CommandPayload {
                name: "auth".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        }
    }

    async fn stored_policy(storage: &InMemoryStorage) -> Option<serde_json::Value> {
        storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed")
    }

    #[tokio::test]
    async fn allow_user_adds_to_policy() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("allow", Some("42"), None), &services)
            .await
            .expect("allow expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({ "allowed_users": ["42"], "allowed_roles": [] }))
        );
        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("✅"));
        assert!(message.contains("<@42>"));
    }

    #[tokio::test]
    async fn allow_role_adds_to_policy() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("allow", None, Some("7")), &services)
            .await
            .expect("allow expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({ "allowed_users": [], "allowed_roles": ["7"] }))
        );
        assert!(
            output.messages().into_iter().next().is_some_and(|message| message.contains("<@&7>"))
        );
    }

    #[tokio::test]
    async fn allow_duplicate_is_idempotent() {
        let (storage, services, output) = fixture();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "allowed_users": ["42"], "allowed_roles": [] }),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("allow", Some("42"), None), &services)
            .await
            .expect("allow expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({ "allowed_users": ["42"], "allowed_roles": [] }))
        );
        assert!(
            output.messages().into_iter().next().is_some_and(|message| message.contains("already"))
        );
    }

    #[tokio::test]
    async fn deny_removes_from_policy() {
        let (storage, services, output) = fixture();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "allowed_users": ["42", "43"], "allowed_roles": [] }),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("deny", Some("42"), None), &services)
            .await
            .expect("deny expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({ "allowed_users": ["43"], "allowed_roles": [] }))
        );
        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("no longer allowed"))
        );
    }

    #[tokio::test]
    async fn deny_missing_reports_without_saving() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("deny", Some("42"), None), &services)
            .await
            .expect("deny expected to succeed");

        assert_eq!(stored_policy(&storage).await, None);
        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("was not in the policy"))
        );
    }

    /// Denying the final listed entry empties the policy - which silently
    /// FLIPS the gate's semantics: every Discord guild administrator becomes
    /// allowed (empty-policy fallback). The reply must warn about exactly
    /// that widening.
    #[tokio::test]
    async fn denying_the_last_entry_warns_about_the_admin_fallback() {
        let (storage, services, output) = fixture();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "allowed_users": ["42"], "allowed_roles": [] }),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("deny", Some("42"), None), &services)
            .await
            .expect("deny expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({ "allowed_users": [], "allowed_roles": [] }))
        );
        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("no longer allowed"));
        assert!(message.contains("The policy is now empty"));
        assert!(message.contains("only Discord guild administrators"));
    }

    #[tokio::test]
    async fn show_renders_policy_with_mentions() {
        let (storage, services, output) = fixture();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "allowed_users": ["42"], "allowed_roles": ["7"] }),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("show", None, None), &services)
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("🛡 Bot access policy"));
        assert!(message.contains("<@42>"));
        assert!(message.contains("<@&7>"));
    }

    #[tokio::test]
    async fn show_empty_policy_reports_none() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("show", None, None), &services)
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("none"));
    }

    /// A malformed policy blocks writes: the answer points at the config,
    /// and the corrupted document is never replaced by a blank one.
    #[tokio::test]
    async fn malformed_policy_blocks_writes() {
        let (storage, services, output) = fixture();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!("not an object"),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("allow", Some("42"), None), &services)
            .await
            .expect("allow expected to succeed");

        assert_eq!(stored_policy(&storage).await, Some(serde_json::json!("not an object")));
        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("unreadable"))
        );
    }

    #[tokio::test]
    async fn missing_target_shows_usage() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("allow", None, None), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(
            output.messages().into_iter().next().is_some_and(|message| message.contains("Usage:"))
        );
    }

    #[tokio::test]
    async fn both_targets_shows_usage() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("allow", Some("42"), Some("7")), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("not both"))
        );
    }

    #[tokio::test]
    async fn unknown_action_shows_usage() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("teleport", Some("42"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(
            output.messages().into_iter().next().is_some_and(|message| message.contains("Usage:"))
        );
    }

    #[tokio::test]
    async fn direct_message_invocation_replies_guild_only() {
        let output = RecordingChatOutput::new();
        let services = dm_services(&output);

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("allow", Some("42"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("only works inside a server"))
        );
    }

    /// `init` declares the `/auth` descriptor: Manage Server permission,
    /// guild-only, action choices, typed user/role arguments.
    #[test]
    fn init_registers_auth_command() {
        let registry = Arc::new(InMemoryCommandRegistry::new());
        let plugin = AuthPlugin::new(registry.clone());

        plugin.init().expect("init expected to succeed");

        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 1);
        let descriptor = descriptors.first().expect("descriptor expected");
        crate::test_support::assert_descriptions_fit_discord(&descriptors);
        assert_eq!(descriptor.name, "auth");
        assert_eq!(descriptor.plugin_id, "auth");
        assert!(descriptor.guild_only);
        assert!(
            descriptor
                .required_permission
                .as_ref()
                .is_some_and(|permission| permission.name == "manage_guild")
        );
        let action = descriptor.arguments.first().expect("action expected");
        assert_eq!(
            action.choices.as_deref(),
            Some(["allow".to_owned(), "deny".to_owned(), "show".to_owned()].as_slice())
        );
        assert!(descriptor.arguments.iter().any(|argument| argument.kind == ArgKind::User));
        assert!(descriptor.arguments.iter().any(|argument| argument.kind == ArgKind::Role));
    }
}
