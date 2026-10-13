//! `/auth` - the interactive half of the auth plugin. Writes the same tier
//! policy document the middleware gate reads; meaning (who holds which
//! tier) lives in this plugin, not in the dispatcher.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;

use crate::kernel::{
    models::{Embed, EventPayload, OutboundMessage, RequestContext},
    plugin_ports::{AccessTier, CommandArgs, CommandHandler},
    services::KernelServices,
    spi_ports::GuildStorage,
};

use super::{
    AuthConfig, CONFIG_KEY, NAMESPACE, OwnerList, PolicyCache, resolve_tier, stored_user_tier,
};

/// `/auth action:[show|set|clear|default] [tier] [user] [role]` - manage the
/// guild's tier policy. Runs through the pipeline like every command, so the
/// auth gate has already vetted the invoker (the descriptor requires the
/// `Moderator` tier); this handler enforces the capability matrix on top:
/// every write must stay below the invoker's own rank, self-writes are
/// refused, and moderators get a limited `show` and no role/default writes.
/// Every answer is ephemeral: policy data stays between the bot and the
/// invoker. Policy writes serialize on one plugin-wide lock: the
/// read-modify-write of the policy document must not lose one of two
/// concurrent admin updates.
pub struct AuthCommandHandler {
    policy_writes: Arc<AsyncMutex<()>>,
    owners: OwnerList,
    /// The gate's policy cache, shared so a successful write invalidates
    /// immediately - same "one policy source" the two halves already
    /// share through the storage document.
    policy_cache: PolicyCache,
}

impl AuthCommandHandler {
    #[must_use]
    pub fn new() -> Self {
        Self {
            policy_writes: Arc::new(AsyncMutex::new(())),
            owners: OwnerList::default(),
            policy_cache: PolicyCache::default(),
        }
    }

    /// Wires the deployment-global owner list (production path; the default
    /// is empty, which disables the owner guards in tests).
    #[must_use]
    pub fn with_owners(mut self, owners: OwnerList) -> Self {
        self.owners = owners;
        self
    }

    /// Shares the gate's policy cache: every successful write drops the
    /// cached policies so the next event resolves the fresh document.
    #[must_use]
    pub fn with_policy_cache(mut self, cache: PolicyCache) -> Self {
        self.policy_cache = cache;
        self
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

        // Bot owners are config-managed: set/clear refuse to touch them.
        // Tier resolution ignores guild-side entries for owners anyway; this
        // is the explicit feedback that the write did not happen.
        match args.get("action") {
            Some("set") | Some("clear")
                if args.get("user").is_some_and(|user| self.owners.contains(user)) =>
            {
                return reply(
                    services,
                    text(
                        "Bot owners are managed in the bot's configuration and cannot be \
                         changed here.",
                    ),
                )
                .await;
            }
            _ => {}
        }

        match args.get("action") {
            Some("show") => {
                let policy = match read_policy(&**storage).await {
                    Ok(policy) => policy,
                    Err(message) => return reply(services, message).await,
                };
                let invoker = Invoker::resolve(&policy, &self.owners, event);
                show_policy(services, &invoker, &policy).await
            }
            Some("set") => {
                set_tier(
                    &**storage,
                    &self.policy_writes,
                    &self.policy_cache,
                    &self.owners,
                    event,
                    services,
                    args,
                )
                .await
            }
            Some("clear") => {
                clear_target(
                    &**storage,
                    &self.policy_writes,
                    &self.policy_cache,
                    &self.owners,
                    event,
                    services,
                    args,
                )
                .await
            }
            Some("default") => {
                set_default_tier(
                    &**storage,
                    &self.policy_writes,
                    &self.policy_cache,
                    &self.owners,
                    event,
                    services,
                    args,
                )
                .await
            }
            _ => reply(services, usage()).await,
        }
    }
}

/// The `/auth` invoker, resolved after the policy read: their own rank
/// depends on it (stored assignments can lift a member to Moderator or
/// Admin, permission bits clamp server managers above everything stored).
/// Shared by every action - the capability matrix compares against this.
struct Invoker {
    tier: AccessTier,
    user_id: String,
}

impl Invoker {
    fn resolve(policy: &AuthConfig, owners: &OwnerList, event: &RequestContext) -> Self {
        let user_id = event.origin.user_id.get().to_string();
        let (author_roles, author_permissions) = match &event.payload {
            EventPayload::Command(command) => (&command.author_roles, command.author_permissions),
            // Only commands reach this handler; the arm keeps `resolve`
            // total for direct test invocations.
            _ => (&Vec::new(), 0),
        };
        Self {
            tier: resolve_tier(policy, owners, &user_id, author_roles, author_permissions),
            user_id,
        }
    }
}

enum Target {
    User(String),
    Role(String),
}

/// The assignable tiers. [`AccessTier::Owner`] is deliberately absent: it
/// is config-injected and must never be offered - not in the `/auth`
/// dropdown, not accepted by [`parse_tier`].
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

/// Wording for "who can change what I cannot": the rank one step above the
/// invoker's own.
fn senior_rank(invoker: AccessTier) -> &'static str {
    match invoker {
        AccessTier::Admin => "a Guild Admin or above",
        AccessTier::Banned | AccessTier::Guest | AccessTier::User | AccessTier::Moderator => {
            "an Admin or above"
        }
        // Stored ranks top out at Admin, so a Guild Admin or Owner invoker
        // never trips a too-high-target denial; the arm keeps the match
        // exhaustive.
        AccessTier::GuildAdmin | AccessTier::Owner => "a higher rank",
    }
}

/// Capability matrix for a member write (`set`/`clear` on a user target):
/// `tier` is the written tier for `set`, `None` for `clear`. A write is
/// legal only when the written tier AND the target's stored rank both sit
/// strictly below the invoker's own - a moderator manages guests and users,
/// an admin manages moderators, a guild admin manages admins, an owner is
/// unrestricted. Self-writes are refused for everyone: no rank can promote
/// or demote itself through the bot.
fn member_write_denial(
    policy: &AuthConfig,
    invoker: &Invoker,
    tier: Option<AccessTier>,
    target_id: &str,
) -> Option<String> {
    if invoker.user_id == target_id {
        return Some("You cannot change your own tier assignment here.".to_owned());
    }
    if let Some(tier) = tier
        && tier >= invoker.tier
    {
        return Some(match invoker.tier {
            AccessTier::Admin => {
                format!("Only a Guild Admin or above can assign the {tier} tier.")
            }
            _ => format!("Only an Admin or above can assign the {tier} tier."),
        });
    }
    let current = stored_user_tier(policy, target_id);
    (current >= invoker.tier).then(|| {
        format!(
            "<@{target_id}> currently ranks as {current} - only {} can change their tier.",
            senior_rank(invoker.tier)
        )
    })
}

/// Role grants decide tiers for whole membership slices - Admins only.
fn role_write_denial(invoker: &Invoker) -> Option<String> {
    (invoker.tier < AccessTier::Admin)
        .then(|| "Assigning tiers to roles is reserved for Admins and above.".to_owned())
}

/// The guild-wide fallback tier shapes every unlisted member - Admins only.
fn default_write_denial(invoker: &Invoker) -> Option<String> {
    (invoker.tier < AccessTier::Admin)
        .then(|| "Changing the default tier is reserved for Admins and above.".to_owned())
}

/// The `tier` argument, required by `set` and `default`.
async fn tier_of(services: &KernelServices, args: &CommandArgs) -> Result<AccessTier, ()> {
    let Some(raw) = args.get("tier") else {
        if let Err(err) =
            reply(services, text("Specify a `tier`: banned, guest, user, moderator, admin.")).await
        {
            tracing::warn!(%err, "failed to deliver /auth usage notice");
        }
        return Err(());
    };
    let Some(tier) = parse_tier(raw) else {
        if let Err(err) = reply(
            services,
            text(format!(
                "Unknown tier `{raw}`. Use one of: banned, guest, user, moderator, admin."
            )),
        )
        .await
        {
            tracing::warn!(%err, "failed to deliver /auth usage notice");
        }
        return Err(());
    };
    Ok(tier)
}

async fn set_tier(
    storage: &dyn GuildStorage,
    policy_writes: &AsyncMutex<()>,
    policy_cache: &PolicyCache,
    owners: &OwnerList,
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
    let Ok(tier) = tier_of(services, args).await else {
        return Ok(());
    };
    // One write at a time: set is a document read-modify-write, and two
    // concurrent invocations must not lose one update.
    let _write = policy_writes.lock().await;

    let mut policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };

    let invoker = Invoker::resolve(&policy, owners, event);
    let denial = match &target {
        Target::User(id) => member_write_denial(&policy, &invoker, Some(tier), id),
        Target::Role(_) => role_write_denial(&invoker),
    };
    if let Some(denial) = denial {
        return reply(services, text(denial)).await;
    }

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
        policy_cache.invalidate_all();
        match &target {
            Target::User(_) => format!("✅ User {mention} is now {tier}."),
            Target::Role(_) => format!("✅ Role {mention} now grants {tier}."),
        }
    };

    // A banned assignment on a target that carries a permission clamp never
    // takes effect - server managers resolve to Guild Admin and Discord
    // administrators to at least Admin (pinned by tests). The handler
    // cannot see the target's permission bits, so the caveat is
    // unconditional on this one tier: a success reply must not claim a ban
    // that may not hold.
    if tier == AccessTier::Banned {
        answer.push_str(
            "\n⚠️ Members with Manage Server or Discord's Administrator permission always rank \
             above this assignment.",
        );
    }

    reply(services, text(answer)).await
}

async fn clear_target(
    storage: &dyn GuildStorage,
    policy_writes: &AsyncMutex<()>,
    policy_cache: &PolicyCache,
    owners: &OwnerList,
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
    // One write at a time: clear is a document read-modify-write, and two
    // concurrent invocations must not lose one update.
    let _write = policy_writes.lock().await;

    let mut policy = match read_policy(storage).await {
        Ok(policy) => policy,
        Err(message) => return reply(services, message).await,
    };

    let invoker = Invoker::resolve(&policy, owners, event);
    let denial = match &target {
        Target::User(id) => member_write_denial(&policy, &invoker, None, id),
        Target::Role(_) => role_write_denial(&invoker),
    };
    if let Some(denial) = denial {
        return reply(services, text(denial)).await;
    }

    let (map, mention) = match &target {
        Target::User(id) => (&mut policy.users, format!("<@{id}>")),
        Target::Role(id) => (&mut policy.roles, format!("<@&{id}>")),
    };
    let id = match &target {
        Target::User(id) | Target::Role(id) => id.clone(),
    };

    let answer = if map.remove(&id).is_some() {
        storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&policy)?).await?;
        policy_cache.invalidate_all();
        format!("✅ Removed the tier assignment for {mention}.")
    } else {
        format!("{mention} has no tier assignment.")
    };

    reply(services, text(answer)).await
}

async fn set_default_tier(
    storage: &dyn GuildStorage,
    policy_writes: &AsyncMutex<()>,
    policy_cache: &PolicyCache,
    owners: &OwnerList,
    event: &RequestContext,
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

    let invoker = Invoker::resolve(&policy, owners, event);
    if let Some(denial) = default_write_denial(&invoker) {
        return reply(services, text(denial)).await;
    }

    if policy.default_tier == tier {
        return reply(services, text(format!("The default tier is already {tier}."))).await;
    }
    policy.default_tier = tier;
    storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&policy)?).await?;
    policy_cache.invalidate_all();

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

async fn show_policy(
    services: &KernelServices,
    invoker: &Invoker,
    policy: &AuthConfig,
) -> anyhow::Result<()> {
    // Moderators get the member-facing slice only: the default tier when it
    // sits at or below their own rank, user rows at or below Moderator, and
    // nothing about roles or higher ranks. Admins and above see everything.
    let full = invoker.tier >= AccessTier::Admin;

    let mut description = if full || policy.default_tier <= AccessTier::Moderator {
        format!("Default tier: {}", policy.default_tier)
    } else {
        "Default tier: (hidden - ask an Admin or above)".to_owned()
    };
    description.push_str("\n\nUsers:");
    if full {
        description.push_str(&render_assignments(&policy.users, user_mention));
    } else {
        description.push_str(&render_limited_assignments(&policy.users, user_mention));
    }
    description.push_str("\n\nRoles:");
    if full {
        description.push_str(&render_assignments(&policy.roles, role_mention));
    } else {
        description.push_str(" (hidden - ask an Admin or above)");
    }
    description.push_str(
        "\n\nMembers with Manage Server always act as Guild Admin; Discord administrators \
         act as Admin or above.",
    );
    if invoker.tier == AccessTier::Owner {
        description.push_str(
            "\nYou are a bot owner: your access outranks this policy and comes from the bot's \
             configuration.",
        );
    }

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
        Err(err) => {
            tracing::warn!(
                namespace = NAMESPACE,
                %err,
                "auth policy unreadable on the command path"
            );
            return Err(unavailable());
        }
    };
    match serde_json::from_value::<AuthConfig>(value) {
        // Sanitized like the gate's read: a hand-edited document carrying
        // `owner` tiers must not render in `/auth show` - and, more
        // importantly, must not be persisted verbatim by the next write.
        Ok(policy) => Ok(policy.sanitized()),
        Err(err) => {
            tracing::warn!(
                namespace = NAMESPACE,
                %err,
                "auth policy malformed on the command path"
            );
            Err(unavailable())
        }
    }
}

/// Assignments rendered per embed section. Discord caps one embed's
/// description at 4096 characters - a guild with hundreds of assignments
/// would otherwise turn `/auth show` into a generic failure notice.
const MAX_RENDERED_ASSIGNMENTS: usize = 20;

fn render_assignments(
    assignments: &BTreeMap<String, AccessTier>,
    mention: fn(&str) -> String,
) -> String {
    render_rows(assignments.iter(), assignments.len(), mention)
}

/// The moderator's slice of a section: rows at or below `Moderator` render;
/// higher ranks collapse into a count (admin-level detail stays hidden).
fn render_limited_assignments(
    assignments: &BTreeMap<String, AccessTier>,
    mention: fn(&str) -> String,
) -> String {
    let visible: Vec<(&String, &AccessTier)> =
        assignments.iter().filter(|(_, tier)| **tier <= AccessTier::Moderator).collect();
    let hidden = assignments.len() - visible.len();
    let mut rendered = render_rows(visible.into_iter(), assignments.len(), mention);
    if hidden > 0 {
        if rendered == " none" {
            rendered = format!(" none ({hidden} higher ranks hidden)");
        } else {
            rendered.push_str(&format!("\n…and {hidden} higher ranks hidden"));
        }
    }
    rendered
}

/// Shared row renderer with the embed cap. `total` feeds only the
/// "and N more" remainder line; empty input renders as ` none`.
fn render_rows<'a>(
    rows: impl Iterator<Item = (&'a String, &'a AccessTier)>,
    total: usize,
    mention: fn(&str) -> String,
) -> String {
    if total == 0 {
        return " none".to_owned();
    }
    let mut rendered = String::new();
    let mut shown = 0usize;
    for (id, tier) in rows {
        if shown >= MAX_RENDERED_ASSIGNMENTS {
            let remaining = total - shown;
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
            invite_directory: crate::test_support::test_invite_directory(),
            guild_storage: Some(storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        (storage, services, output)
    }

    fn dm_services(output: &Arc<RecordingChatOutput>) -> KernelServices {
        KernelServices {
            chat_output: Arc::clone(output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(output)).boxed(),
            invite_directory: crate::test_support::test_invite_directory(),
            guild_storage: None,
            plugin_storage: crate::test_support::test_plugin_storage(),
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
                // Manage Server: the default invoker resolves to Guild
                // Admin - the everyday write-everything-below-owner case.
                // Matrix tests override the bits deliberately.
                author_permissions: 0x20,
            }),
        }
    }

    /// Command event with explicit platform permission bits (opaque
    /// pass-through data; `0x20` marks a Manage Server holder, `0x8` a
    /// Discord guild administrator).
    fn command_event_with_permissions(user_id: u64, author_permissions: u64) -> RequestContext {
        let mut event = command_event();
        event.origin.user_id = UserId(user_id);
        if let EventPayload::Command(payload) = &mut event.payload {
            payload.author_permissions = author_permissions;
        }
        event
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

    /// The plugin-wide `policy_writes` lock serializes policy writes: two
    /// concurrent `set` invocations queue behind the in-flight write instead
    /// of racing its read-modify-write, so neither update is lost and both
    /// assignments land in the policy document.
    #[tokio::test]
    async fn concurrent_policy_writes_do_not_lose_updates() {
        let (storage, services, output) = fixture();
        let handler = Arc::new(AuthCommandHandler::default());
        // Hold the plugin-wide write lock, as an in-flight admin write would.
        let guard = handler.policy_writes.lock().await;

        let services_a = services.clone();
        let services_b = services.clone();
        let set_user = {
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                handler
                    .invoke(
                        &command_event(),
                        &auth_args("set", Some("user"), Some("42"), None),
                        &services_a,
                    )
                    .await
                    .expect("set expected to succeed");
            })
        };
        let set_role = {
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                handler
                    .invoke(
                        &command_event(),
                        &auth_args("set", Some("banned"), None, Some("7")),
                        &services_b,
                    )
                    .await
                    .expect("set expected to succeed");
            })
        };

        // Both writes must queue: until the lock is released, nothing lands.
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            stored_policy(&storage).await,
            None,
            "queued writes must wait for the in-flight write to release the lock"
        );

        drop(guard);
        set_user.await.expect("set task expected to finish");
        set_role.await.expect("set task expected to finish");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({
                "default_tier": "user",
                "users": { "42": "user" },
                "roles": { "7": "banned" },
            })),
            "the second write must merge onto the first, not lose one update"
        );
        let messages = output.messages();
        assert_eq!(messages.len(), 2, "both invocations must be answered: {messages:?}");
        assert!(
            messages.iter().all(|message| message.contains("✅")),
            "both writes must succeed: {messages:?}"
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

    /// Self-writes are refused for every rank: no one may promote or
    /// demote themselves through the bot (the old warning became a refusal
    /// when the capability matrix landed).
    #[tokio::test]
    async fn self_write_is_refused() {
        let (storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("set", Some("banned"), Some("3"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert_eq!(stored_policy(&storage).await, None, "no write must land");
        assert!(
            output.messages().into_iter().next().is_some_and(|message| {
                message.contains("cannot change your own tier assignment")
            })
        );

        // Clearing yourself is a self-write too.
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": { "3": "guest" }, "roles": {} }),
        );
        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("clear", None, Some("3"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(
            output.messages().into_iter().last().is_some_and(|message| {
                message.contains("cannot change your own tier assignment")
            })
        );
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
        assert!(message.contains("Members with Manage Server always act as Guild Admin"));
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

    /// `owner` is a config-injected rank, not an assignable tier: the
    /// dropdown never offers it, and submitting it directly is rejected -
    /// the reply lists the assignable tiers only.
    #[tokio::test]
    async fn set_with_owner_tier_is_rejected() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("set", Some("owner"), Some("42"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(
            output
                .messages()
                .into_iter()
                .next()
                .is_some_and(|message| message.contains("Unknown tier `owner`"))
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

    /// `init` declares the `/auth` descriptor: Moderator floor (visible to
    /// everyone - the handler enforces the matrix), guild-only, action/tier
    /// choices, typed user/role arguments, and no platform permission gate
    /// (derived ranks like Guild Admin cannot be expressed natively).
    #[test]
    fn init_registers_auth_command() {
        let registry = Arc::new(InMemoryCommandRegistry::new());
        let plugin = AuthPlugin::new(registry.clone(), OwnerList::default());

        plugin.init().expect("init expected to succeed");

        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 1);
        let descriptor = descriptors.first().expect("descriptor expected");
        crate::test_support::assert_descriptions_fit_discord(&descriptors);
        assert_eq!(descriptor.name, "auth");
        assert_eq!(descriptor.plugin_id, "auth");
        assert!(descriptor.guild_only);
        assert_eq!(descriptor.required_tier, Some(AccessTier::Moderator));
        assert!(descriptor.required_permission.is_none());
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

    fn handler_with_owners(ids: &[&str]) -> AuthCommandHandler {
        let owned: Vec<String> = ids.iter().map(|id| (*id).to_owned()).collect();
        AuthCommandHandler::new().with_owners(OwnerList::from_ids(&owned))
    }

    /// `set` refuses to write a policy entry for a bot owner: the identity
    /// is config-managed, and the refusal is the explicit feedback.
    #[tokio::test]
    async fn set_targeting_bot_owner_is_rejected() {
        let (storage, services, output) = fixture();
        let handler = handler_with_owners(&["7"]);

        handler
            .invoke(&command_event(), &auth_args("set", Some("banned"), Some("7"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert_eq!(stored_policy(&storage).await, None);
        assert!(output.messages().into_iter().any(|message| message.contains("cannot be changed")));
    }

    /// `clear` refuses the same way, and the stored policy is untouched.
    #[tokio::test]
    async fn clear_targeting_bot_owner_is_rejected() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({
                "default_tier": "user",
                "users": { "7": "moderator" },
                "roles": {}
            }),
        );
        let handler = handler_with_owners(&["7"]);

        handler
            .invoke(&command_event(), &auth_args("clear", None, Some("7"), None), &services)
            .await
            .expect("invoke expected to succeed");

        assert_eq!(
            stored_policy(&storage).await,
            Some(serde_json::json!({
                "default_tier": "user",
                "users": { "7": "moderator" },
                "roles": {}
            }))
        );
        assert!(output.messages().into_iter().any(|message| message.contains("cannot be changed")));
    }

    /// The owner guard is scoped to the `user` argument: other targets and
    /// non-owner users keep working with owners configured.
    #[tokio::test]
    async fn set_non_owner_target_unaffected_by_owner_guard() {
        let (storage, services, output) = fixture();
        let handler = handler_with_owners(&["7"]);

        handler
            .invoke(
                &command_event(),
                &auth_args("set", Some("moderator"), Some("42"), None),
                &services,
            )
            .await
            .expect("invoke expected to succeed");

        assert!(stored_policy(&storage).await.is_some());
        assert!(output.messages().into_iter().any(|message| message.contains("is now Moderator")));
    }

    /// A bot owner running `/auth show` sees their own status - a
    /// self-verification hint, not the list (the list never enters
    /// guild-visible surfaces).
    #[tokio::test]
    async fn show_announces_owner_status_to_owner() {
        let (_storage, services, output) = fixture();
        // "3" is the invoker used by `command_event`.
        let handler = handler_with_owners(&["3"]);

        handler
            .invoke(&command_event(), &auth_args("show", None, None, None), &services)
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("You are a bot owner"));
    }

    /// Non-owners get no owner line at all.
    #[tokio::test]
    async fn show_hides_owner_status_from_non_owners() {
        let (_storage, services, output) = fixture();

        AuthCommandHandler::default()
            .invoke(&command_event(), &auth_args("show", None, None, None), &services)
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(!message.contains("bot owner"));
    }

    /// The capability matrix: a moderator's `show` renders only the
    /// member-facing slice - low default tiers, user rows at or below
    /// Moderator - while higher ranks, counts included only as a number,
    /// and the whole roles section stay hidden.
    #[tokio::test]
    async fn moderator_sees_limited_view() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({
                "default_tier": "guest",
                "users": { "3": "moderator", "42": "guest", "43": "admin" },
                "roles": { "7": "user" }
            }),
        );

        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0),
                &auth_args("show", None, None, None),
                &services,
            )
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("Default tier: Guest"));
        assert!(message.contains("<@3>: Moderator"));
        assert!(message.contains("<@42>: Guest"));
        assert!(message.contains("1 higher ranks hidden"), "admin row hidden: {message}");
        assert!(!message.contains("<@43>"), "no admin row: {message}");
        assert!(message.contains("(hidden - ask an Admin or above)"), "roles hidden: {message}");
        assert!(!message.contains("<@&7>"), "no role rows: {message}");
    }

    /// A moderator whose policy default sits above their rank does not see
    /// that default - admin-level detail.
    #[tokio::test]
    async fn moderator_does_not_see_higher_default_tier() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "admin", "users": { "3": "moderator" }, "roles": {} }),
        );

        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0),
                &auth_args("show", None, None, None),
                &services,
            )
            .await
            .expect("show expected to succeed");

        let message = output.messages().into_iter().next().expect("reply expected");
        assert!(message.contains("Default tier: (hidden"));
        assert!(!message.contains("Default tier: Admin"));
    }

    /// A moderator manages members below their own rank only - both bounds
    /// of the matrix: the written tier and the target's stored rank.
    #[tokio::test]
    async fn moderator_write_bounds_are_enforced() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({
                "default_tier": "user",
                "users": { "3": "moderator", "43": "moderator" },
                "roles": {}
            }),
        );

        // Written tier too high (moderator ≥ invoker moderator).
        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0),
                &auth_args("set", Some("moderator"), Some("42"), None),
                &services,
            )
            .await
            .expect("invoke expected to succeed");
        assert!(
            output.messages().into_iter().last().is_some_and(|message| {
                message.contains("Only an Admin or above can assign the Moderator tier.")
            }),
            "written-tier bound: {:?}",
            output.messages()
        );
        assert_eq!(
            stored_policy(&storage).await.as_ref().and_then(|policy| policy.get("users")),
            Some(&serde_json::json!({ "3": "moderator", "43": "moderator" })),
            "no write must land"
        );

        // Target too high (stored moderator ≥ invoker moderator) - also
        // blocks re-setting to a lower tier (no demotions of equals).
        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0),
                &auth_args("set", Some("guest"), Some("43"), None),
                &services,
            )
            .await
            .expect("invoke expected to succeed");
        assert!(output.messages().into_iter().last().is_some_and(|message| {
            message
                .contains("<@43> currently ranks as Moderator - only an Admin or above can change their tier.")
        }));

        // Clear hits the same bound.
        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0),
                &auth_args("clear", None, Some("43"), None),
                &services,
            )
            .await
            .expect("invoke expected to succeed");
        assert!(output.messages().into_iter().last().is_some_and(|message| {
            message.contains("only an Admin or above can change their tier")
        }));
    }

    /// Within their bounds a moderator can manage: guest/user tiers on
    /// unranked members.
    #[tokio::test]
    async fn moderator_can_manage_lower_ranked_members() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": { "3": "moderator" }, "roles": {} }),
        );

        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0),
                &auth_args("set", Some("guest"), Some("42"), None),
                &services,
            )
            .await
            .expect("set expected to succeed");

        assert!(output.messages().into_iter().any(|message| message.contains("is now Guest")));
        assert_eq!(
            stored_policy(&storage).await.as_ref().and_then(|policy| policy.get("users")),
            Some(&serde_json::json!({ "3": "moderator", "42": "guest" }))
        );
    }

    /// Role grants and the guild-wide default shape whole membership
    /// slices: Admins only, even for a moderator invoker.
    #[tokio::test]
    async fn moderator_cannot_write_roles_or_default() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": { "3": "moderator" }, "roles": {} }),
        );
        let invoker = command_event_with_permissions(3, 0);

        AuthCommandHandler::default()
            .invoke(&invoker, &auth_args("set", Some("guest"), None, Some("7")), &services)
            .await
            .expect("invoke expected to succeed");
        assert!(output.messages().into_iter().last().is_some_and(|message| {
            message.contains("Assigning tiers to roles is reserved for Admins and above.")
        }));

        AuthCommandHandler::default()
            .invoke(&invoker, &auth_args("clear", None, None, Some("7")), &services)
            .await
            .expect("invoke expected to succeed");
        assert!(output.messages().into_iter().last().is_some_and(|message| {
            message.contains("Assigning tiers to roles is reserved for Admins and above.")
        }));

        AuthCommandHandler::default()
            .invoke(&invoker, &auth_args("default", Some("guest"), None, None), &services)
            .await
            .expect("invoke expected to succeed");
        assert!(output.messages().into_iter().last().is_some_and(|message| {
            message.contains("Changing the default tier is reserved for Admins and above.")
        }));

        assert_eq!(
            stored_policy(&storage).await.as_ref().and_then(|policy| policy.get("roles")),
            Some(&serde_json::json!({})),
            "no role write landed"
        );
        assert_eq!(
            stored_policy(&storage).await.as_ref().and_then(|policy| policy.get("default_tier")),
            Some(&serde_json::json!("user")),
            "no default write landed"
        );
    }

    /// An admin invoker cannot write Admin-ranked members or tiers - both
    /// matrix bounds name the rank above them.
    #[tokio::test]
    async fn admin_bounds_stop_at_admin() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": { "42": "admin" }, "roles": {} }),
        );
        // 0x8 alone: the bare administrator clamp resolves to Admin.
        let invoker = command_event_with_permissions(3, 0x8);

        // Written tier too high (admin ≥ invoker admin).
        AuthCommandHandler::default()
            .invoke(&invoker, &auth_args("set", Some("admin"), Some("43"), None), &services)
            .await
            .expect("invoke expected to succeed");
        assert!(output.messages().into_iter().last().is_some_and(|message| {
            message.contains("Only a Guild Admin or above can assign the Admin tier.")
        }));

        // Target too high (stored admin ≥ invoker admin) - including a
        // demotion attempt to a lower tier.
        AuthCommandHandler::default()
            .invoke(&invoker, &auth_args("set", Some("user"), Some("42"), None), &services)
            .await
            .expect("invoke expected to succeed");
        assert!(output.messages().into_iter().last().is_some_and(|message| {
            message.contains(
                "<@42> currently ranks as Admin - only a Guild Admin or above can change their tier.",
            )
        }));
        assert_eq!(
            stored_policy(&storage).await.as_ref().and_then(|policy| policy.get("users")),
            Some(&serde_json::json!({ "42": "admin" })),
            "no write landed"
        );
    }

    /// The step above admin: a Manage Server invoker appoints and removes
    /// Admins - the delegation story that makes Guild Admin the guild's
    /// top authority over the bot.
    #[tokio::test]
    async fn guild_admin_appoints_and_removes_admins() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": { "42": "admin" }, "roles": {} }),
        );

        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0x20),
                &auth_args("set", Some("admin"), Some("43"), None),
                &services,
            )
            .await
            .expect("set expected to succeed");
        assert!(output.messages().into_iter().any(|message| message.contains("is now Admin")));

        AuthCommandHandler::default()
            .invoke(
                &command_event_with_permissions(3, 0x20),
                &auth_args("clear", None, Some("42"), None),
                &services,
            )
            .await
            .expect("clear expected to succeed");
        assert!(
            output
                .messages()
                .into_iter()
                .any(|message| message.contains("Removed the tier assignment"))
        );
        assert_eq!(
            stored_policy(&storage).await.as_ref().and_then(|policy| policy.get("users")),
            Some(&serde_json::json!({ "43": "admin" }))
        );
    }

    /// A bot owner invoker bypasses the matrix: every target and tier is
    /// writable (targets who are owners themselves are refused earlier by
    /// the config-managed guard).
    #[tokio::test]
    async fn owner_invoker_bypasses_the_matrix() {
        let (storage, services, output) = fixture();
        storage.seed(
            "test",
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "default_tier": "user", "users": { "42": "admin" }, "roles": {} }),
        );
        let handler = handler_with_owners(&["3"]);
        // Plain event: no permission bits - the owner list is the rank.
        let invoker = command_event_with_permissions(3, 0);

        handler
            .invoke(&invoker, &auth_args("set", Some("guest"), Some("42"), None), &services)
            .await
            .expect("set expected to succeed");
        assert!(
            output.messages().into_iter().any(|message| message.contains("is now Guest")),
            "owner demotes an admin: {:?}",
            output.messages()
        );

        handler
            .invoke(&invoker, &auth_args("default", Some("admin"), None, None), &services)
            .await
            .expect("default expected to succeed");
        assert!(
            output
                .messages()
                .into_iter()
                .any(|message| message.contains("Default tier is now Admin")),
            "owner sets an admin default: {:?}",
            output.messages()
        );
    }
}
