//! `/auth` - the interactive half of the auth plugin. Writes the same tier
//! policy document the middleware gate reads; meaning (who holds which
//! tier) lives in this plugin, not in the dispatcher.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;

use crate::kernel::{
    models::{Embed, OutboundMessage, RequestContext},
    plugin_ports::{AccessTier, CommandArgs, CommandHandler},
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::{AuthConfig, CONFIG_KEY, NAMESPACE};

/// `/auth action:[show|set|clear|default] [tier] [user] [role]` - manage the
/// guild's tier policy. Runs through the pipeline like every command, so the
/// auth gate has already vetted the invoker (the descriptor requires the
/// `Admin` tier). Every answer is ephemeral: policy data stays between the
/// bot and the admin. Policy writes serialize on one plugin-wide lock: the
/// read-modify-write of the policy document must not lose one of two
/// concurrent admin updates.
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
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = &services.guild_storage else {
            return reply(services, text("This command only works inside a server.")).await;
        };

        match args.get("action") {
            Some("show") => show_policy(&**storage, services).await,
            Some("set") => set_tier(&**storage, &self.policy_writes, event, services, args).await,
            Some("clear") => clear_target(&**storage, &self.policy_writes, services, args).await,
            Some("default") => {
                set_default_tier(&**storage, &self.policy_writes, services, args).await
            }
            _ => reply(services, usage()).await,
        }
    }
}

enum Target {
    User(String),
    Role(String),
}

fn all_tiers() -> [AccessTier; 5] {
    [
        AccessTier::Banned,
        AccessTier::Guest,
        AccessTier::User,
        AccessTier::Moderator,
        AccessTier::Admin,
    ]
}

fn parse_tier(raw: &str) -> Option<AccessTier> {
    all_tiers().into_iter().find(|tier| tier.as_str() == raw)
}

/// `set`/`clear` address exactly one of user/role.
fn target_of(args: &CommandArgs) -> Result<Target, OutboundMessage> {
    match (args.get("user"), args.get("role")) {
        (Some(user), None) => Ok(Target::User(user.to_owned())),
        (None, Some(role)) => Ok(Target::Role(role.to_owned())),
        (Some(_), Some(_)) => Err(text("Specify either `user` or `role`, not both.")),
        (None, None) => Err(usage()),
    }
}

/// The `tier` argument, required by `set` and `default`.
async fn tier_of(services: &KernelServices, args: &CommandArgs) -> Result<AccessTier, ()> {
    let Some(raw) = args.get("tier") else {
        reply(services, text("Specify a `tier`: banned, guest, user, moderator, admin."))
            .await
            .ok();
        return Err(());
    };
    let Some(tier) = parse_tier(raw) else {
        reply(
            services,
            text(format!(
                "Unknown tier `{raw}`. Use one of: banned, guest, user, moderator, admin."
            )),
        )
        .await
        .ok();
        return Err(());
    };
    Ok(tier)
}

async fn set_tier(
    storage: &dyn GuildStorage,
    policy_writes: &AsyncMutex<()>,
    event: &RequestContext,
    services: &KernelServices,
    args: &CommandArgs,
) -> anyhow::Result<()> {
    // Argument validation runs before the write lock: a malformed
    // invocation must not queue behind another admin's write.
    let target = match target_of(args) {
        Ok(target) => target,
        Err(message) => return reply(services, message).await,
    };
    // One write at a time: set is a document read-modify-write, and two
    // concurrent invocations must not lose one update.
    let _write = policy_writes.lock().await;
    let Ok(tier) = tier_of(services, args).await else {
        return Ok(());
    };

    let mut policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };

    let (map, mention) = match &target {
        Target::User(id) => (&mut policy.users, format!("<@{id}>")),
        Target::Role(id) => (&mut policy.roles, format!("<@&{id}>")),
    };
    let id = match &target {
        Target::User(id) | Target::Role(id) => id.clone(),
    };

    let mut answer = if map.get(&id) == Some(&tier) {
        format!("{mention} already has the {tier} tier.")
    } else {
        map.insert(id, tier);
        storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&policy)?).await?;
        match &target {
            Target::User(_) => format!("✅ User {mention} is now {tier}."),
            Target::Role(_) => format!("✅ Role {mention} now grants {tier}."),
        }
    };

    // A self-demotion is possible (the gate already passed), warn before it
    // locks the invoker out; guild administrators are immune by the clamp.
    if let Target::User(id) = &target
        && *id == event.origin.user_id.get().to_string()
        && tier < AccessTier::Admin
    {
        answer.push_str(
            "\n⚠️ You lowered your own tier. Discord administrators always keep Admin; \
             without that you may need another admin to undo this.",
        );
    }

    reply(services, text(answer)).await
}

async fn clear_target(
    storage: &dyn GuildStorage,
    policy_writes: &AsyncMutex<()>,
    services: &KernelServices,
    args: &CommandArgs,
) -> anyhow::Result<()> {
    // Argument validation runs before the write lock: a malformed
    // invocation must not queue behind another admin's write.
    let target = match target_of(args) {
        Ok(target) => target,
        Err(message) => return reply(services, message).await,
    };
    // One write at a time: clear is a document read-modify-write, and two
    // concurrent invocations must not lose one update.
    let _write = policy_writes.lock().await;

    let mut policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };

    let (map, mention) = match &target {
        Target::User(id) => (&mut policy.users, format!("<@{id}>")),
        Target::Role(id) => (&mut policy.roles, format!("<@&{id}>")),
    };
    let id = match &target {
        Target::User(id) | Target::Role(id) => id.clone(),
    };

    let answer = if map.remove(&id).is_some() {
        storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&policy)?).await?;
        format!("✅ Removed the tier assignment for {mention}.")
    } else {
        format!("{mention} has no tier assignment.")
    };

    reply(services, text(answer)).await
}

async fn set_default_tier(
    storage: &dyn GuildStorage,
    policy_writes: &AsyncMutex<()>,
    services: &KernelServices,
    args: &CommandArgs,
) -> anyhow::Result<()> {
    let Ok(tier) = tier_of(services, args).await else {
        return Ok(());
    };
    let _write = policy_writes.lock().await;

    let mut policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };
    policy.default_tier = tier;
    storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&policy)?).await?;

    // Legal but destructive settings get an explicit consequence line: the
    // reply is ephemeral, so the warning costs nothing but an oversight.
    let warning = match policy.default_tier {
        AccessTier::Admin => {
            "\n⚠️ Every member - including strangers - can now run admin commands."
        }
        AccessTier::Banned => {
            "\n⚠️ The whole guild is now ignored silently: no replies, no denials."
        }
        _ => "",
    };
    reply(services, text(format!("✅ Default tier is now {}.{}", tier, warning))).await
}

async fn show_policy(storage: &dyn GuildStorage, services: &KernelServices) -> anyhow::Result<()> {
    let policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };

    let mut description = format!("Default tier: {}", policy.default_tier);
    description.push_str("\n\nUsers:");
    description.push_str(&render_assignments(&policy.users, user_mention));
    description.push_str("\n\nRoles:");
    description.push_str(&render_assignments(&policy.roles, role_mention));
    description.push_str("\n\nDiscord administrators always act as Admin.");

    reply(
        services,
        OutboundMessage::embed(Embed { title: "🛡 Bot access tiers".to_owned(), description })
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

/// Assignments rendered per embed section. Discord caps one embed's
/// description at 4096 characters - a guild with hundreds of assignments
/// would otherwise turn `/auth show` into a generic failure notice.
const MAX_RENDERED_ASSIGNMENTS: usize = 20;

fn render_assignments(
    assignments: &BTreeMap<String, AccessTier>,
    mention: fn(&str) -> String,
) -> String {
    if assignments.is_empty() {
        return " none".to_owned();
    }
    let mut rendered = String::new();
    let mut shown = 0usize;
    for (id, tier) in assignments {
        if shown >= MAX_RENDERED_ASSIGNMENTS {
            let remaining = assignments.len() - shown;
            rendered.push_str(&format!("\n…and {remaining} more"));
            break;
        }
        rendered.push('\n');
        rendered.push_str(&mention(id));
        rendered.push_str(": ");
        rendered.push_str(&tier.to_string());
        shown += 1;
    }
    rendered
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
    text(
        "Usage: `/auth action:show`, `/auth action:set` + `tier` + a `user` or a `role`, \
         `/auth action:clear` + a `user` or a `role`, or `/auth action:default` + `tier`.",
    )
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
            ChannelId, CommandPayload, EventKind, EventPayload, GuildId, MessageId, Origin, UserId,
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
            guild_storage: Some(storage.guild_scoped("test", GuildId(1))),
            platform_info: crate::test_support::test_platform_info(),
        };
        (storage, services, output)
    }

    fn dm_services(output: &Arc<RecordingChatOutput>) -> KernelServices {
        KernelServices {
            chat_output: Arc::clone(output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(output)).boxed(),
            guild_storage: None,
            platform_info: crate::test_support::test_platform_info(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn auth_args(
        action: &str,
        tier: Option<&str>,
        user: Option<&str>,
        role: Option<&str>,
    ) -> CommandArgs {
        let mut pairs = vec![("action".to_owned(), action.to_owned())];
        if let Some(tier) = tier {
            pairs.push(("tier".to_owned(), tier.to_owned()));
        }
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
            .guild_scoped("test", GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed")
    }

    #[tokio::test]
    async fn set_user_tier_persists() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(
                &command_event(),
                &auth_args("set", Some("moderator"), Some("42"), None),
                &services,
            )
            .await
            .expect("set expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({
                "default_tier": "user",
                "users": { "42": "moderator" },
                "roles": {},
            }))
        );
        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("✅"));
        assert!(message.contains("<@42>"));
        assert!(message.contains("Moderator"));
    }

    #[tokio::test]
    async fn set_role_tier_persists() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("set", Some("guest"), None, Some("7")), &services)
            .await
            .expect("set expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({
                "default_tier": "user",
                "users": {},
                "roles": { "7": "guest" },
            }))
        );
        assert!(
            output.messages().into_iter().next().is_some_and(|message| message.contains("<@&7>"))
        );
    }

    #[tokio::test]
    async fn set_same_tier_is_idempotent() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": { "42": "moderator" }, "roles": {} }),
        );

        AuthCommandHandler::default()
            .invoke(
                &command_event(),
                &auth_args("set", Some("moderator"), Some("42"), None),
                &services,
            )
            .await
            .expect("set expected to succeed");

        assert!(
            output.messages().into_iter().next().is_some_and(|message| message.contains("already"))
        );
    }

    #[tokio::test]
    async fn clear_user_removes_assignment() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({
                "default_tier": "user",
                "users": { "42": "moderator", "43": "guest" },
                "roles": {},
            }),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("clear", None, Some("42"), None), &services)
            .await
            .expect("clear expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({
                "default_tier": "user",
                "users": { "43": "guest" },
                "roles": {},
            }))
        );
        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("Removed the tier assignment"))
        );
    }

    #[tokio::test]
    async fn clear_missing_reports_without_saving() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("clear", None, Some("42"), None), &services)
            .await
            .expect("clear expected to succeed");

        assert_eq!(stored_policy(&storage).await, None);
        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("no tier assignment"))
        );
    }

    #[tokio::test]
    async fn default_sets_default_tier() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(
                &command_event(),
                &auth_args("default", Some("moderator"), None, None),
                &services,
            )
            .await
            .expect("default expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({
                "default_tier": "moderator",
                "users": {},
                "roles": {},
            }))
        );
        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("Default tier is now Moderator"))
        );
    }

    /// Lowering your own tier is possible (the gate already passed) - the
    /// reply must warn about the lockout before it happens.
    #[tokio::test]
    async fn self_demotion_warns() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("set", Some("banned"), Some("3"), None), &services)
            .await
            .expect("set expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("lowered your own tier"));
        assert!(message.contains("Discord administrators"));
    }

    #[tokio::test]
    async fn no_self_demotion_warning_for_other_targets() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(
                &command_event(),
                &auth_args("set", Some("banned"), Some("42"), None),
                &services,
            )
            .await
            .expect("set expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(!message.contains("lowered your own tier"));
    }

    #[tokio::test]
    async fn show_renders_tiers_with_mentions() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({
                "default_tier": "guest",
                "users": { "42": "moderator" },
                "roles": { "7": "user" },
            }),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("show", None, None, None), &services)
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("🛡 Bot access tiers"));
        assert!(message.contains("Default tier: Guest"));
        assert!(message.contains("<@42>: Moderator"));
        assert!(message.contains("<@&7>: User"));
        assert!(message.contains("Discord administrators always act as Admin"));
    }

    #[tokio::test]
    async fn show_empty_policy_reports_none() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("show", None, None, None), &services)
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("none"));
    }

    /// Destructive defaults get an explicit consequence line in the
    /// ephemeral reply: admin opens the bot to everyone, banned silences it
    /// for everyone.
    #[tokio::test]
    async fn destructive_default_tiers_warn() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("default", Some("admin"), None, None), &services)
            .await
            .expect("default expected to succeed");
        assert!(output.messages().into_iter().next().is_some_and(|message| {
            message.contains("Default tier is now Admin") && message.contains("Every member")
        }));

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("default", Some("banned"), None, None), &services)
            .await
            .expect("default expected to succeed");
        let message = output.messages().into_iter().last().expect("reply expected");
        assert!(message.contains("Default tier is now Banned"));
        assert!(message.contains("silently"));

        // A normal tier carries no warning.
        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("default", Some("user"), None, None), &services)
            .await
            .expect("default expected to succeed");
        let message = output.messages().into_iter().last().expect("reply expected");
        assert!(message.contains("Default tier is now User"));
        assert!(!message.contains("⚠️"));
        let policy = stored_policy(&storage).await.expect("policy stored");
        assert_eq!(policy.get("default_tier").and_then(serde_json::Value::as_str), Some("user"));
    }

    /// The show embed must stay within Discord's embed description cap: a
    /// guild with many assignments renders the first few and a remainder
    /// count instead of failing the whole command.
    #[tokio::test]
    async fn show_caps_long_assignment_lists() {
        let (storage, services, output) = fixture();
        let mut users = serde_json::Map::new();
        for id in 1..=25 {
            users.insert(format!("u{id:02}"), serde_json::json!("user"));
        }
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": users, "roles": {} }),
        );

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("show", None, None, None), &services)
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("<@u20>: User"), "20th assignment expected: {message}");
        assert!(!message.contains("<@u21>"), "cap exceeded: {message}");
        assert!(message.contains("and 5 more"), "remainder count expected: {message}");
    }

    /// A malformed policy blocks writes: the answer points at the config,
    /// and the corrupted document is never replaced by a blank one.
    #[tokio::test]
    async fn malformed_policy_blocks_writes() {
        let (storage, services, output) = fixture();
        storage.seed("test", GuildId(1), NAMESPACE, CONFIG_KEY, serde_json::json!("not an object"));

        AuthCommandHandler::default()
            .invoke(
                &command_event(),
                &auth_args("set", Some("moderator"), Some("42"), None),
                &services,
            )
            .await
            .expect("set expected to succeed");

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
    async fn set_without_tier_shows_hint() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("set", None, Some("42"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("Specify a `tier`"))
        );
    }

    #[tokio::test]
    async fn set_with_unknown_tier_errors() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(
                &command_event(),
                &auth_args("set", Some("wizard"), Some("42"), None),
                &services,
            )
            .await
            .expect("invoke expected to succeed");

        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("Unknown tier `wizard`"))
        );
    }

    #[tokio::test]
    async fn missing_target_shows_usage() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("set", Some("moderator"), None, None), &services)
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
            .invoke(
                &command_event(),
                &auth_args("set", Some("moderator"), Some("42"), Some("7")),
                &services,
            )
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
            .invoke(
                &command_event(),
                &auth_args("teleport", Some("moderator"), Some("42"), None),
                &services,
            )
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
            .invoke(
                &command_event(),
                &auth_args("set", Some("moderator"), Some("42"), None),
                &services,
            )
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

    /// `init` declares the `/auth` descriptor: Admin tier, Manage Server
    /// platform gate, guild-only, action/tier choices, typed user/role
    /// arguments.
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
        assert_eq!(descriptor.required_tier, Some(AccessTier::Admin));
        assert!(
            descriptor
                .required_permission
                .as_ref()
                .is_some_and(|permission| permission.name == "manage_guild")
        );
        let arguments = &descriptor.arguments;
        let action = arguments.first().expect("action expected");
        assert_eq!(
            action.choices.as_deref(),
            Some(
                ["show".to_owned(), "set".to_owned(), "clear".to_owned(), "default".to_owned()]
                    .as_slice()
            )
        );
        let tier = arguments.get(1).expect("tier argument expected");
        assert_eq!(
            tier.choices.as_deref(),
            Some(
                [
                    "banned".to_owned(),
                    "guest".to_owned(),
                    "user".to_owned(),
                    "moderator".to_owned(),
                    "admin".to_owned(),
                ]
                .as_slice()
            )
        );
        assert!(arguments.iter().any(|argument| argument.kind == ArgKind::User));
        assert!(arguments.iter().any(|argument| argument.kind == ArgKind::Role));
    }
}
