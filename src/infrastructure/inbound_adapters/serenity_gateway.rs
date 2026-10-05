use std::collections::HashMap;
use std::sync::Arc;

use serenity::all::{
    ChannelId as SerenityChannelId, CommandDataOption, CommandDataOptionValue, CommandDataResolved,
    Context, EventHandler, GuildId as SerenityGuildId, Interaction, Member, Message, Permissions,
    Ready, RoleId as SerenityRoleId, User, UserId as SerenityUserId,
};
use serenity::async_trait;

use crate::infrastructure::outbound_adapters::{GatewayContext, MentionKind, parse_mention_tag};
use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{
        AttachmentPayload, ChannelId, CommandPayload, EventKind, EventPayload, GuildId,
        MemberPayload, MessageId, Origin, RequestContext, UserId,
    },
    spi_ports::PlatformInfoPort,
};

/// Stable storage/telemetry slug of this adapter's platform - the value
/// [`DiscordPlatform`] serves and the namespace label stamped into every
/// storage row this deployment writes.
pub const PLATFORM_SLUG: &str = "discord";

/// The deployment's platform identity, owned by this adapter: the port
/// values and every `Origin` the adapter builds share one constant.
pub struct DiscordPlatform;

impl PlatformInfoPort for DiscordPlatform {
    fn slug(&self) -> &'static str {
        PLATFORM_SLUG
    }

    fn display_name(&self) -> &'static str {
        "Discord"
    }

    fn message_limit(&self) -> Option<usize> {
        // Bot accounts: the documented hard content limit. User-side Nitro
        // extensions do not apply to bots.
        Some(2000)
    }

    fn embed_limit(&self) -> Option<usize> {
        // The embed description cap.
        Some(4096)
    }
}

/// Kernel driving adapter: normalizes Discord gateway events onto the
/// chat-agnostic `RequestContext` taxonomy and pushes each through the
/// pipeline. Platform specifics (serenity types) never cross this boundary.
pub struct DiscordGatewayAdapter<H: RequestHandlerPort> {
    handler: H,
    /// Attached on `ready` so outbound adapters (presence) can drive the
    /// gateway; this adapter is the only writer. Presence requests that
    /// arrived before connect are queued in the handle and flush here.
    context: Arc<GatewayContext>,
}

impl<H: RequestHandlerPort> DiscordGatewayAdapter<H> {
    pub fn new(handler: H, context: Arc<GatewayContext>) -> Self {
        Self { handler, context }
    }
}

#[async_trait]
impl<H: RequestHandlerPort> EventHandler for DiscordGatewayAdapter<H> {
    async fn message(&self, ctx: Context, message: Message) {
        // Bots and webhooks both feed noise into the pipeline. The bot's own
        // replies never re-enter through the gateway either: plugins that
        // need their own turns in a conversation record them at send time.
        if message.author.bot || message.webhook_id.is_some() {
            return;
        }

        // Best effort: role and permission data are only present when Discord
        // included the member in the payload. Authorization treats missing
        // roles as "no roles" and missing permissions as unknown (0), never
        // as a hard failure.
        let author_roles: Vec<String> = message
            .member
            .as_ref()
            .map(|member| member.roles.iter().map(|role| role.get().to_string()).collect())
            .unwrap_or_default();
        // Message-path permission resolution: payload bits when the platform
        // included them, else the gateway cache - so the auth plugin's
        // Discord-admin clamp holds everywhere.
        let author_permissions = resolve_author_permissions(&ctx, &message);
        // Same best-effort display naming: channel nick when present, else
        // the platform username, frozen at capture time for history renders.
        let author_name = message
            .member
            .as_ref()
            .and_then(|member| member.nick.clone())
            .unwrap_or_else(|| message.author.name.clone());
        // The reply reference survives even when the referenced message is
        // not cached (or was deleted) - unlike `referenced_message`.
        let reply_to = message
            .message_reference
            .and_then(|reference| reference.message_id)
            .map(|message_id| MessageId(message_id.get()));
        // Mention matching needs the bot identity, which lives in the
        // gateway cache and is guaranteed present once messages flow.
        let current_user_id = ctx.cache.current_user().id;
        let mentions_bot = message.mentions.iter().any(|user| user.id == current_user_id);
        // Platform-blind attachment DTOs; the URLs are Discord's own CDN
        // links (the pinned trusted host for plugin-side downloads).
        let attachments: Vec<AttachmentPayload> = message
            .attachments
            .iter()
            .map(|attachment| AttachmentPayload {
                url: attachment.url.clone(),
                content_type: attachment.content_type.clone(),
                file_name: Some(attachment.filename.clone()),
                size_bytes: u64::from(attachment.size),
                width: attachment.width,
                height: attachment.height,
            })
            .collect();

        let origin = Origin {
            guild_id: message.guild_id.map(|guild_id| GuildId(guild_id.get())),
            channel_id: ChannelId(message.channel_id.get()),
            user_id: UserId(message.author.id.get()),
            message_id: Some(MessageId(message.id.get())),
            reply_token: None,
        };

        // Discord meta-tags are unreadable to consumers (`<@123>`): rewrite
        // them as `[Name]<@123>` so history readers see who was named AND
        // the raw tag to imitate in replies. Names resolve from the
        // payload's mentions first, then the gateway cache; anything
        // unresolvable stays raw. Outbound sends invert the shape (see
        // `denormalize_mention_tags`). The cache guard is scoped: it must
        // be dropped before the pipeline await below.
        let content = {
            let guild = message.guild_id.and_then(|guild_id| ctx.cache.guild(guild_id));
            let mentioned: HashMap<u64, &User> =
                message.mentions.iter().map(|user| (user.id.get(), user)).collect();
            let resolve = |kind: MentionKind, id: u64| match kind {
                MentionKind::User => {
                    mentioned.get(&id).map(|user| display_name(user)).or_else(|| {
                        guild
                            .as_ref()
                            .and_then(|guild| guild.members.get(&SerenityUserId::new(id)))
                            .map(|member| {
                                member.nick.clone().unwrap_or_else(|| member.user.name.clone())
                            })
                    })
                }
                MentionKind::Role => guild
                    .as_ref()
                    .and_then(|guild| guild.roles.get(&SerenityRoleId::new(id)))
                    .map(|role| role.name.clone()),
                MentionKind::Channel => guild
                    .as_ref()
                    .and_then(|guild| guild.channels.get(&SerenityChannelId::new(id)))
                    .map(|channel| channel.name.clone()),
            };
            normalize_mention_tags(&message.content, &resolve)
        };

        let mut event = RequestContext::message_received(origin, content);
        // Guild display name for the `{guild_name}` turn-template parameter:
        // best effort from the gateway cache, frozen at capture time like
        // the author name. `None` in DMs.
        let guild_name = message
            .guild_id
            .and_then(|guild_id| ctx.cache.guild(guild_id))
            .map(|guild| guild.name.clone());
        if let EventPayload::Message(payload) = &mut event.payload {
            payload.author_name = Some(author_name);
            payload.guild_name = guild_name;
            payload.attachments = attachments;
            payload.author_roles = author_roles;
            payload.author_permissions = author_permissions;
            payload.reply_to = reply_to;
            payload.mentions_bot = mentions_bot;
        }

        self.handler.handle(event).await;
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let Some(command) = interaction.command() else {
            return; // components/autocomplete/modal: no taxonomy kind yet
        };

        // Interaction responses owe Discord an answer within ~3 seconds.
        // Acknowledge deferred right here, at ingestion, so plugins can take
        // as long as they need; their replies go out as followups bound to
        // the interaction token. Type 5 = DeferredChannelMessageWithSource.
        // `flags` must live INSIDE `data`: a top-level `flags` field is
        // ignored, and a non-ephemeral defer makes the whole exchange public.
        // The defer carries EPHEMERAL (64): the loading state shows to the
        // invoker alone, ephemeral replies edit it in place (inheriting its
        // state), and public replies are routed around that inheritance rule
        // by `InteractionFollowupOutput` - see there.
        //
        // The age/duration pair in the logs below separates the two ways
        // this deadline can blow: a large `interaction_age_ms` means the
        // gateway event arrived late (network path), a large `defer_ms`
        // means the callback POST itself crawled.
        let age_ms = interaction_age_ms(command.id.get());
        let defer_started = std::time::Instant::now();
        if let Err(err) = ctx
            .http
            .create_interaction_response(
                command.id,
                &command.token,
                &deferred_ephemeral_response(),
                Vec::new(),
            )
            .await
        {
            let defer_ms = u64::try_from(defer_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            tracing::error!(
                %err,
                interaction_age_ms = age_ms,
                defer_ms,
                "failed to defer interaction"
            );
            return;
        }
        let defer_ms = u64::try_from(defer_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        tracing::debug!(interaction_age_ms = age_ms, defer_ms, "interaction deferred");

        let author_roles: Vec<String> = command
            .member
            .as_ref()
            .map(|member| member.roles.iter().map(|role| role.get().to_string()).collect())
            .unwrap_or_default();

        // Same best-effort contract as the message path above.
        let author_permissions = command
            .member
            .as_ref()
            .and_then(|member| member.permissions)
            .map_or(0, |permissions| permissions.bits());

        let origin = Origin {
            guild_id: command.guild_id.map(|guild_id| GuildId(guild_id.get())),
            channel_id: ChannelId(command.channel_id.get()),
            user_id: UserId(command.user.id.get()),
            message_id: Some(MessageId(command.id.get())),
            reply_token: Some(command.token.clone()),
        };

        let event = RequestContext {
            kind: EventKind::CommandInvoked,
            origin,
            payload: EventPayload::Command(CommandPayload {
                name: command.data.name.clone(),
                args: flatten_options(&command.data.options, &command.data.resolved),
                author_roles,
                author_permissions,
            }),
        };

        self.handler.handle(event).await;
    }

    /// Member lifecycle events carry no channel; the audit destination is a
    /// per-guild plugin decision, not an event property.
    async fn guild_member_addition(&self, _ctx: Context, new_member: Member) {
        let event = RequestContext {
            kind: EventKind::MemberJoined,
            origin: member_origin(new_member.guild_id, &new_member.user),
            payload: EventPayload::Member(MemberPayload {
                username: Some(new_member.user.name.clone()),
            }),
        };
        self.handler.handle(event).await;
    }

    async fn guild_member_removal(
        &self,
        _ctx: Context,
        guild_id: SerenityGuildId,
        user: User,
        _member_data_if_available: Option<Member>,
    ) {
        let event = RequestContext {
            kind: EventKind::MemberLeft,
            origin: member_origin(guild_id, &user),
            payload: EventPayload::Member(MemberPayload { username: Some(user.name.clone()) }),
        };
        self.handler.handle(event).await;
    }

    async fn ready(&self, ctx: Context, ready: Ready) {
        // Flushes a presence queued before connect - the first rotation
        // status lands exactly when the bot comes online.
        self.context.attach(ctx);
        tracing::info!("connected as {}", ready.user.name);
    }
}

/// Origin for channel-less member lifecycle events (`ChannelId(0)` sentinel).
fn member_origin(guild_id: SerenityGuildId, user: &User) -> Origin {
    Origin {
        guild_id: Some(GuildId(guild_id.get())),
        channel_id: ChannelId(0),
        user_id: UserId(user.id.get()),
        message_id: None,
        reply_token: None,
    }
}

/// The acknowledgement sent for every slash-command interaction: a deferred
/// channel message whose loading state carries the EPHEMERAL flag (64).
/// `flags` sits inside `data` per the interaction-callback data shape - a
/// top-level `flags` field is silently ignored by Discord and would make the
/// defer (and with it every command reply) public. Delivery halves live in
/// [`InteractionFollowupOutput`]: ephemeral replies edit this response
/// (inheriting its ephemeral state), public replies are posted past it.
fn deferred_ephemeral_response() -> serde_json::Value {
    serde_json::json!({ "type": 5, "data": { "flags": 64 } })
}

/// Display name of a mention payload user: global display name when set,
/// else the username.
fn display_name(user: &User) -> String {
    user.global_name.clone().unwrap_or_else(|| user.name.clone())
}

/// Rewrites mention tags in message content to the normalized shape:
/// `<@123>` -> `[Name]<@123>`. The lookup receives the entity kind and id
/// and returns the display name; unresolvable ids and non-mention shapes
/// pass through untouched. `[`/`]` are stripped from resolved names so the
/// outbound inverse scan stays unambiguous.
fn normalize_mention_tags(
    content: &str,
    resolve: &dyn Fn(MentionKind, u64) -> Option<String>,
) -> String {
    let mut out = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(position) = rest.find('<') {
        out.push_str(rest.get(..position).unwrap_or(""));
        // `tail` starts at the '<' - `parse_mention_tag` expects it.
        let tail = rest.get(position..).unwrap_or("");
        if let Some((kind, id, length)) = parse_mention_tag(tail) {
            if let Some(name) = resolve(kind, id) {
                let name = name.replace(['[', ']'], "");
                if !name.is_empty() {
                    out.push('[');
                    out.push_str(&name);
                    out.push(']');
                }
            }
            out.push_str(tail.get(..length).unwrap_or(""));
            rest = tail.get(length..).unwrap_or("");
        } else {
            out.push('<');
            rest = tail.get(1..).unwrap_or("");
        }
    }
    out.push_str(rest);
    out
}

/// Message-path permission resolution: the payload's permission bits when
/// the platform included them (interaction-shaped payloads do), otherwise
/// the gateway cache - owner => everything, else @everyone + member roles
/// union. 0 = unknown (no bits, no cached guild): authorization treats
/// unknown as "no admin clamp", never as a hard failure.
fn resolve_author_permissions(ctx: &Context, message: &Message) -> u64 {
    if let Some(permissions) = message.member.as_ref().and_then(|member| member.permissions) {
        return permissions.bits();
    }
    let Some(guild_id) = message.guild_id else { return 0 };
    let Some(guild) = ctx.cache.guild(guild_id) else { return 0 };
    let everyone =
        guild.roles.get(&SerenityRoleId::new(guild.id.get())).map(|role| role.permissions.bits());
    let member_roles: Vec<u64> = guild
        .members
        .get(&message.author.id)
        .map(|member| {
            member
                .roles
                .iter()
                .filter_map(|role_id| guild.roles.get(role_id))
                .map(|role| role.permissions.bits())
                .collect()
        })
        .unwrap_or_default();
    unioned_member_permissions(
        guild.owner_id.get(),
        message.author.id.get(),
        everyone,
        &member_roles,
    )
}

/// Effective Discord permissions of a guild member from the gateway cache:
/// the owner gets everything, anyone else the union of the @everyone role
/// and their member roles (Discord's own algorithm minus channel
/// overwrites - sufficient for the admin-bit check the auth plugin makes).
/// Pure so the owner/union rules stay testable without a live cache.
fn unioned_member_permissions(
    owner_id: u64,
    user_id: u64,
    everyone: Option<u64>,
    member_role_bits: &[u64],
) -> u64 {
    if owner_id == user_id {
        return Permissions::all().bits();
    }
    let mut permissions = Permissions::from_bits_truncate(everyone.unwrap_or(0));
    for bits in member_role_bits {
        permissions |= Permissions::from_bits_truncate(*bits);
    }
    permissions.bits()
}

/// The Discord epoch: snowflakes count milliseconds from 2015-01-01.
const DISCORD_EPOCH_MS: u64 = 1_420_070_400_000;

/// An interaction's age at handling time, decoded from its snowflake id
/// (top 42 bits are the creation timestamp). The ~3s acknowledge deadline
/// runs from the CREATION moment, not from when we see the event - so this
/// number, logged next to the defer duration, tells late gateway delivery
/// apart from a slow callback.
fn interaction_age_ms(id: u64) -> u64 {
    let created_ms = (id >> 22).saturating_add(DISCORD_EPOCH_MS);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(u64::MAX, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    now_ms.saturating_sub(created_ms)
}

/// Flattens Discord's option tree into `name -> value` string pairs.
/// Subcommand boundaries are flattened away; resolved entities (users,
/// channels, roles) arrive as their IDs. Attachment options resolve to the
/// attachment's CDN URL - a pinned trusted host (Discord's CDN), which is
/// the only network peer an attachment argument may ever name.
fn flatten_options(
    options: &[CommandDataOption],
    resolved: &CommandDataResolved,
) -> Vec<(String, String)> {
    fn walk(
        options: &[CommandDataOption],
        resolved: &CommandDataResolved,
        args: &mut Vec<(String, String)>,
    ) {
        fn push_id(name: &str, id: impl std::fmt::Display, args: &mut Vec<(String, String)>) {
            args.push((name.to_owned(), id.to_string()));
        }

        for option in options {
            match &option.value {
                CommandDataOptionValue::SubCommand(inner)
                | CommandDataOptionValue::SubCommandGroup(inner) => walk(inner, resolved, args),
                CommandDataOptionValue::String(value) => {
                    args.push((option.name.clone(), value.clone()));
                }
                CommandDataOptionValue::Integer(value) => {
                    args.push((option.name.clone(), value.to_string()));
                }
                CommandDataOptionValue::Number(value) => {
                    args.push((option.name.clone(), value.to_string()));
                }
                CommandDataOptionValue::Boolean(value) => {
                    args.push((option.name.clone(), value.to_string()));
                }
                CommandDataOptionValue::Channel(value) => {
                    push_id(&option.name, *value, args);
                }
                CommandDataOptionValue::User(value) => {
                    push_id(&option.name, *value, args);
                }
                CommandDataOptionValue::Role(value) => {
                    push_id(&option.name, *value, args);
                }
                CommandDataOptionValue::Mentionable(value) => {
                    push_id(&option.name, *value, args);
                }
                CommandDataOptionValue::Attachment(value) => {
                    if let Some(attachment) = resolved.attachments.get(value) {
                        args.push((option.name.clone(), attachment.url.clone()));
                    } else {
                        tracing::warn!(
                            argument = %option.name,
                            "attachment option without resolved metadata - argument dropped"
                        );
                    }
                }
                other => {
                    // The enum is non_exhaustive upstream: an unknown option
                    // shape would silently vanish from `args` - make the
                    // drift visible instead.
                    tracing::warn!(
                        argument = %option.name,
                        value = ?other,
                        "unknown option value shape - argument dropped"
                    );
                }
            }
        }
    }

    let mut args = Vec::new();
    walk(options, resolved, &mut args);
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver(kind: MentionKind, id: u64) -> Option<String> {
        match (kind, id) {
            (MentionKind::User, 123) => Some("Alice".to_owned()),
            (MentionKind::User, 77) => Some("[weird]name".to_owned()),
            (MentionKind::Role, 7) => Some("Mods".to_owned()),
            (MentionKind::Channel, 5) => Some("general".to_owned()),
            _ => None,
        }
    }

    /// User (both wire shapes), role and channel tags get the name prefix;
    /// custom emoji already carries its name and stays raw.
    #[test]
    fn mention_tags_are_normalized_with_names() {
        let out =
            normalize_mention_tags("hey <@123> and <@!123>: <@&7> in <#5> (<:face:9>)", &resolver);
        assert_eq!(
            out,
            "hey [Alice]<@123> and [Alice]<@!123>: [Mods]<@&7> in [general]<#5> (<:face:9>)"
        );
    }

    #[test]
    fn unresolvable_and_non_tag_input_stays_raw() {
        assert_eq!(normalize_mention_tags("hi <@55>", &resolver), "hi <@55>");
        assert_eq!(
            normalize_mention_tags("<@abc> <@> <:face:9> <https://x>", &resolver),
            "<@abc> <@> <:face:9> <https://x>"
        );
        assert_eq!(normalize_mention_tags("unclosed <@123", &resolver), "unclosed <@123");
        assert_eq!(normalize_mention_tags("no tags at all", &resolver), "no tags at all");
    }

    /// Brackets in resolved names would break the outbound inverse scan -
    /// they are stripped before the name goes into the prompt.
    #[test]
    fn resolved_names_are_sanitized_against_brackets() {
        assert_eq!(normalize_mention_tags("<@77>", &resolver), "[weirdname]<@77>");
    }

    // ---- Reaction token parsing (the react tool's adapter half) ----

    #[test]
    fn unioned_permissions_owner_gets_everything() {
        let all = unioned_member_permissions(1, 1, None, &[]);
        assert_eq!(all, Permissions::all().bits());
    }

    #[test]
    fn unioned_permissions_union_of_everyone_and_member_roles() {
        let bits = unioned_member_permissions(1, 2, Some(0x400), &[0x800, 0x10]);
        assert_eq!(bits, 0x400 | 0x800 | 0x10);

        // Bits outside Discord's valid mask are truncated, not propagated.
        let bits = unioned_member_permissions(1, 2, Some(u64::MAX), &[]);
        assert_eq!(bits, Permissions::all().bits());
    }

    #[test]
    fn unioned_permissions_unknown_member_is_zero() {
        assert_eq!(unioned_member_permissions(1, 2, None, &[]), 0);
        assert_eq!(unioned_member_permissions(1, 2, Some(0x400), &[]), 0x400);
    }

    /// The snowflake's embedded birth time drives the age: a snowflake born
    /// now is moments old; the zero id (born 2015) is over a decade old.
    #[test]
    fn interaction_age_reflects_the_snowflake_birth_time() {
        let now_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after 1970")
                .as_millis(),
        )
        .expect("millis fit u64");
        let born_now = (now_ms.saturating_sub(DISCORD_EPOCH_MS)) << 22;

        let age = interaction_age_ms(born_now);
        assert!(age < 1_000, "an interaction born now is moments old, got {age}ms");
        assert!(
            interaction_age_ms(0) > age,
            "the zero snowflake (born 2015) must be older than one born now"
        );
    }

    /// The defer carries EPHEMERAL inside `data` - Discord ignores a
    /// top-level `flags` field, and a public defer makes every command
    /// reply public (the first followup inherits the defer's visibility).
    #[test]
    fn interaction_defer_is_ephemeral() {
        let response = deferred_ephemeral_response();

        assert_eq!(response.get("type").and_then(serde_json::Value::as_u64), Some(5));
        let data = response.get("data").expect("callback data expected");
        assert_eq!(data.get("flags").and_then(serde_json::Value::as_u64), Some(64));
        // The flag must not leak to the top level, where Discord drops it.
        assert!(response.get("flags").is_none());
    }

    /// The followup half of the same contract: an ephemeral message carries
    /// the EPHEMERAL flag (64); a public one carries none.
    #[test]
    fn flattens_subcommands_and_resolved_entities() {
        let options: Vec<CommandDataOption> = serde_json::from_value(serde_json::json!([
            { "name": "text", "type": 3, "value": "privit" },
            { "name": "count", "type": 4, "value": 3 },
            { "name": "target", "type": 6, "value": "130000000000000000" },
            { "name": "sub", "type": 1, "options": [
                { "name": "inner", "type": 3, "value": "value" }
            ] }
        ]))
        .expect("test options expected to deserialize");

        let args = flatten_options(&options, &CommandDataResolved::default());

        assert_eq!(
            args,
            vec![
                ("text".to_owned(), "privit".to_owned()),
                ("count".to_owned(), "3".to_owned()),
                ("target".to_owned(), "130000000000000000".to_owned()),
                ("inner".to_owned(), "value".to_owned()),
            ]
        );
    }

    /// Attachment options resolve to the attachment's CDN URL - the pinned
    /// trusted host the downloading plugin may fetch.
    #[test]
    fn attachment_options_resolve_to_their_cdn_url() {
        let options: Vec<CommandDataOption> = serde_json::from_value(serde_json::json!([
            { "name": "file", "type": 11, "value": "130000000000000001" }
        ]))
        .expect("test options expected to deserialize");
        let resolved: CommandDataResolved = serde_json::from_value(serde_json::json!({
            "attachments": {
                "130000000000000001": {
                    "id": "130000000000000001",
                    "filename": "prompt.md",
                    "size": 42,
                    "url": "https://cdn.discordapp.com/attachments/1/2/prompt.md",
                    "proxy_url": "https://media.discordapp.net/attachments/1/2/prompt.md"
                }
            }
        }))
        .expect("resolved attachments expected to deserialize");

        let args = flatten_options(&options, &resolved);

        assert_eq!(
            args,
            vec![(
                "file".to_owned(),
                "https://cdn.discordapp.com/attachments/1/2/prompt.md".to_owned()
            )]
        );
    }
}
