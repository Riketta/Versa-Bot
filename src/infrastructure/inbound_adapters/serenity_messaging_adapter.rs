use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serenity::all::{
    ChannelId as SerenityChannelId, CommandDataOption, CommandDataOptionValue, CommandDataResolved,
    Context, CreateMessage, EditMessage, EmojiId, EventHandler, GuildId as SerenityGuildId, Http,
    Interaction, Member, Message, MessageId as SerenityMessageId, MessageReference,
    MessageReferenceKind, Permissions, ReactionType, Ready, RoleId as SerenityRoleId, User,
    UserId as SerenityUserId,
};
use serenity::async_trait;
use tokio_util::sync::CancellationToken;

use crate::infrastructure::outbound_adapters::GatewayContext;
use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{
        AttachmentPayload, ChannelId, CommandPayload, Embed, EventKind, EventPayload, GuildId,
        MemberPayload, MessageId, Origin, OutboundError, OutboundMessage, RequestContext, UserId,
    },
    spi_ports::{
        ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, ChatTypingGuard, PlatformInfoPort,
        ReactionPort, UndeliverableReactionPort,
    },
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

/// Which Discord entity a mention tag points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MentionKind {
    User,
    Role,
    Channel,
}

/// Display name of a mention payload user: global display name when set,
/// else the username.
fn display_name(user: &User) -> String {
    user.global_name.clone().unwrap_or_else(|| user.name.clone())
}

/// Parses a Discord mention tag at the start of `text`: user `<@id>` /
/// `<@!id>`, role `<@&id>`, channel `<#id>`. Returns the kind, the id and
/// the tag's length in bytes. Custom emoji (`<:name:id>`) and any other
/// shape are not mention tags.
fn parse_mention_tag(text: &str) -> Option<(MentionKind, u64, usize)> {
    let bytes = text.as_bytes();
    let mut index = 1; // past '<'
    let kind = match bytes.get(index) {
        Some(b'@') => {
            index += 1;
            if bytes.get(index) == Some(&b'&') {
                index += 1;
                MentionKind::Role
            } else {
                if bytes.get(index) == Some(&b'!') {
                    index += 1;
                }
                MentionKind::User
            }
        }
        Some(b'#') => {
            index += 1;
            MentionKind::Channel
        }
        _ => return None,
    };

    let mut id: u64 = 0;
    let mut digits = 0;
    while let Some(&byte) = bytes.get(index) {
        match byte {
            b'0'..=b'9' => {
                id = id.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
                digits += 1;
                index += 1;
            }
            b'>' if digits > 0 => return Some((kind, id, index + 1)),
            _ => return None,
        }
    }
    None
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

/// The outbound inverse: `[Name]<@id>` -> `<@id>`, so tags the model
/// assembles from normalized history arrive as clean Discord mentions
/// (rendered output shows the resolved name anyway). The name segment must
/// be bracket-adjacent to the tag and free of nested brackets; literal user
/// text matching that same shape (e.g. `[at this]<@123>`) is inherently
/// ambiguous and is rewritten too - only non-adjacent or nested brackets
/// pass through byte-identical.
fn denormalize_mention_tags(content: &str) -> String {
    let mut out = String::with_capacity(content.len());
    let mut rest = content;
    while let Some((before, tail)) = rest.split_once(']') {
        if let Some((_, _, length)) = parse_mention_tag(tail) {
            if let Some((prefix, name)) = before.rsplit_once('[')
                && !name.is_empty()
                && !name.contains('[')
            {
                out.push_str(prefix);
                out.push_str(tail.get(..length).unwrap_or(""));
                rest = tail.get(length..).unwrap_or("");
                continue;
            }
            out.push_str(before);
            out.push(']');
            rest = tail;
        } else {
            out.push_str(before);
            out.push(']');
            rest = tail;
        }
    }
    out.push_str(rest);
    out
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

/// `ChatOutputFactoryPort` bound to Discord: produces `ChatOutputPort`s
/// scoped to a specific event origin. When the event carries an interaction
/// reply token, sends go through the interaction followup endpoint instead
/// of a plain channel message - plugins cannot tell the difference.
pub struct SerenityChatOutputFactory {
    http: Arc<Http>,
}

impl SerenityChatOutputFactory {
    /// Takes the shared REST client: serenity rate limiting is per `Http`,
    /// so every driven Discord caller must share one instance.
    pub fn new(http: Arc<Http>) -> Self {
        Self { http }
    }
}

impl ChatOutputFactoryPort for SerenityChatOutputFactory {
    fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort> {
        if let Some(token) = &origin.reply_token {
            return Arc::new(InteractionFollowupOutput {
                http: Arc::clone(&self.http),
                token: token.clone(),
                first_followup_used: AtomicBool::new(false),
            });
        }

        // Member lifecycle origins carry no channel (`ChannelId(0)`); a
        // Discord call for channel 0 is a guaranteed 404, so fail fast.
        if origin.channel_id.get() == 0 {
            return Arc::new(UndeliverableChatOutput);
        }

        Arc::new(SerenityChatOutput {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
        })
    }

    /// Configured-channel logging. Enforcement is degenerate-case-only (the
    /// origin must be guild-bound and the channel non-zero): verifying that
    /// a channel id actually belongs to the origin guild requires the
    /// gateway cache and is deferred.
    fn channel_output(&self, origin: &Origin, channel_id: ChannelId) -> Arc<dyn ChatOutputPort> {
        // Configured-channel logging is never an interaction reply, so a
        // reply token on the origin is deliberately ignored here.
        if origin.guild_id.is_none() || channel_id.get() == 0 {
            return Arc::new(UndeliverableChatOutput);
        }
        Arc::new(SerenityChatOutput {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(channel_id.get()),
        })
    }

    /// Streaming is plain-channel progressive rendering: never an interaction
    /// reply (followup edits are a different endpoint and no plugin needs
    /// them yet) and never a channel-less origin.
    fn stream_output(&self, origin: &Origin) -> Arc<dyn ChatStreamPort> {
        if origin.reply_token.is_some() || origin.guild_id.is_none() || origin.channel_id.get() == 0
        {
            return Arc::new(UndeliverableChatStream);
        }
        Arc::new(SerenityChatStream {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
        })
    }

    /// Discord message links are constructible from ids alone - no API call.
    fn message_link(
        &self,
        origin: &Origin,
        channel_id: ChannelId,
        message_id: MessageId,
    ) -> Option<String> {
        let guild_id = origin.guild_id?;
        Some(format!(
            "https://discord.com/channels/{}/{}/{}",
            guild_id.get(),
            channel_id.get(),
            message_id.get()
        ))
    }

    /// Typing refresh rides serenity's own `Typing` handle (re-broadcasts on
    /// its internal cadence, stops when dropped); the kernel guard's drop
    /// cancels the bridge task holding it. Channel-less origins get a dead
    /// guard - a typing call for channel 0 is a guaranteed 404.
    fn start_typing(&self, origin: &Origin) -> ChatTypingGuard {
        // Member lifecycle origins carry no channel - nothing to type into.
        if origin.channel_id.get() == 0 {
            return ChatTypingGuard::dead();
        }
        let http = Arc::clone(&self.http);
        let channel = SerenityChannelId::new(origin.channel_id.get());
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            let typing = http.start_typing(channel);
            cancel.cancelled().await;
            drop(typing);
        });
        ChatTypingGuard::new(token)
    }

    /// Reactions are guild-scoped (custom emoji resolution needs the
    /// guild's emoji list), so DM and channel-less origins yield the
    /// undeliverable port.
    fn react(&self, origin: &Origin) -> Arc<dyn ReactionPort> {
        let Some(guild_id) = origin.guild_id else {
            return Arc::new(UndeliverableReactionPort);
        };
        if origin.channel_id.get() == 0 {
            return Arc::new(UndeliverableReactionPort);
        }
        Arc::new(SerenityReaction {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
            guild_id: SerenityGuildId::new(guild_id.get()),
        })
    }
}

struct SerenityChatOutput {
    http: Arc<Http>,
    channel_id: SerenityChannelId,
}

#[async_trait]
impl ChatOutputPort for SerenityChatOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        // Discord rejects messages with neither content nor embeds (400).
        if message.is_empty() {
            tracing::warn!(channel = %self.channel_id, "dropping empty outbound message");
            return Ok(());
        }
        // Plain channel sends are always public - the ephemeral hint has no
        // meaning here and is ignored. Normalized mention tags
        // (`[Name]<@id>`) invert back to bare mentions on the way out.
        let mut create = CreateMessage::new();
        if !message.content.is_empty() {
            create = create.content(denormalize_mention_tags(&message.content));
        }
        if !message.embeds.is_empty() {
            create = create.embeds(message.embeds.iter().map(discord_embed).collect());
        }
        if let Some(reply_to) = message.reply_to {
            create = create.reference_message(reply_reference(self.channel_id, reply_to));
        }

        self.http
            .send_message(self.channel_id, Vec::new(), &create)
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}

/// Terminal output for origins with no deliverable Discord destination
/// (channel-less member lifecycle events, configured channels outside any
/// guild): every send logs a warning and reports failure instead of
/// building a Discord call that is guaranteed to 404.
struct UndeliverableChatOutput;

#[async_trait]
impl ChatOutputPort for UndeliverableChatOutput {
    async fn send(&self, _message: OutboundMessage) -> Result<(), OutboundError> {
        tracing::warn!("dropping outbound message: origin has no deliverable Discord channel");
        Err(OutboundError::Send("no deliverable Discord channel for this origin".to_owned()))
    }
}

/// Reaction port bound to one origin: applies emoji reactions to messages
/// in the origin channel. Custom `:name:` tokens resolve against the
/// guild's current emoji list (one REST fetch per bare-name token -
/// reactions are rare and capped, and the factory holds no gateway cache).
struct SerenityReaction {
    http: Arc<Http>,
    channel_id: SerenityChannelId,
    guild_id: SerenityGuildId,
}

#[async_trait]
impl ReactionPort for SerenityReaction {
    async fn add_reaction(&self, message_id: MessageId, emoji: &str) -> Result<(), OutboundError> {
        let reaction = self.resolve(emoji).await?;
        self.http
            .create_reaction(self.channel_id, SerenityMessageId::new(message_id.get()), &reaction)
            .await
            .map_err(|err| OutboundError::Reaction(err.to_string()))
    }
}

impl SerenityReaction {
    /// Maps a raw protocol token onto a Discord reaction type. Fully
    /// qualified forms (`<:name:id>`, `<a:name:id>`, `:name:id`) build the
    /// custom reaction directly; bare `:name:` needs the guild emoji list;
    /// anything else is a Unicode emoji (tokens the plugin already
    /// validated - an unparseable token here fails per-token, by design).
    async fn resolve(&self, emoji: &str) -> Result<ReactionType, OutboundError> {
        let parsed = parse_reaction_token(emoji);
        match parsed {
            ParsedReaction::Custom { animated, name, id } => {
                Ok(custom_reaction(animated, &name, &id))
            }
            ParsedReaction::Name(name) => {
                let emojis = self
                    .http
                    .get_emojis(self.guild_id)
                    .await
                    .map_err(|err| OutboundError::Reaction(err.to_string()))?;
                emojis
                    .into_iter()
                    .find(|known| known.name.as_str() == name.as_str())
                    .map(|known| ReactionType::Custom {
                        animated: known.animated,
                        id: known.id,
                        name: Some(known.name),
                    })
                    .ok_or_else(|| {
                        OutboundError::Reaction(format!(
                            "custom emoji `:{name}:` not found in this server"
                        ))
                    })
            }
            ParsedReaction::Unicode => Ok(ReactionType::Unicode(emoji.to_owned())),
            ParsedReaction::Invalid => {
                Err(OutboundError::Reaction(format!("unrecognized emoji token `{emoji}`")))
            }
        }
    }
}

/// The adapter-side parse of one reaction token.
#[derive(Debug, PartialEq, Eq)]
enum ParsedReaction {
    /// Fully qualified custom form - usable without resolution.
    Custom { animated: bool, name: String, id: String },
    /// Bare `:name:` - resolve against the guild's emojis.
    Name(String),
    /// Unicode emoji (any token with a non-ASCII character).
    Unicode,
    /// Not an emoji token at all.
    Invalid,
}

fn parse_reaction_token(token: &str) -> ParsedReaction {
    fn valid_name(name: &str) -> bool {
        !name.is_empty() && name.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    }
    fn numeric(id: &str) -> bool {
        !id.is_empty() && id.chars().all(|ch| ch.is_ascii_digit())
    }

    if let Some(body) = token.strip_prefix('<').and_then(|rest| rest.strip_suffix('>')) {
        let (animated, rest) = match body.strip_prefix("a:") {
            Some(rest) => (true, rest),
            None => match body.strip_prefix(':') {
                Some(rest) => (false, rest),
                None => return ParsedReaction::Invalid,
            },
        };
        return match rest.split_once(':') {
            Some((name, id)) if valid_name(name) && numeric(id) => {
                ParsedReaction::Custom { animated, name: name.to_owned(), id: id.to_owned() }
            }
            _ => ParsedReaction::Invalid,
        };
    }
    if let Some(body) = token.strip_prefix(':') {
        if let Some(name) = body.strip_suffix(':') {
            return if valid_name(name) {
                ParsedReaction::Name(name.to_owned())
            } else {
                ParsedReaction::Invalid
            };
        }
        // `:name:id` - the id makes it fully qualified.
        return match body.rsplit_once(':') {
            Some((name, id)) if valid_name(name) && numeric(id) => {
                ParsedReaction::Custom { animated: false, name: name.to_owned(), id: id.to_owned() }
            }
            _ => ParsedReaction::Invalid,
        };
    }
    if token.chars().any(|ch| !ch.is_ascii()) {
        ParsedReaction::Unicode
    } else {
        ParsedReaction::Invalid
    }
}

fn custom_reaction(animated: bool, name: &str, id: &str) -> ReactionType {
    // Digit-validated by the parsers; a failed parse cannot be reached.
    let id = id.parse::<u64>().map(EmojiId::new).unwrap_or_else(|_| EmojiId::new(0));
    ReactionType::Custom { animated, id, name: Some(name.to_owned()) }
}

/// Progressive-rendering output: creates the message on `begin`, then edits
/// it in place as content arrives. Content-only - embeds are ignored in a
/// message that exists to be overwritten.
struct SerenityChatStream {
    http: Arc<Http>,
    channel_id: SerenityChannelId,
}

#[async_trait]
impl ChatStreamPort for SerenityChatStream {
    async fn begin(&self, message: OutboundMessage) -> Result<MessageId, OutboundError> {
        if message.content.is_empty() {
            tracing::warn!(channel = %self.channel_id, "dropping empty streaming placeholder");
            return Err(OutboundError::Send("empty streaming placeholder".to_owned()));
        }
        let mut create = CreateMessage::new().content(denormalize_mention_tags(&message.content));
        if let Some(reply_to) = message.reply_to {
            create = create.reference_message(reply_reference(self.channel_id, reply_to));
        }
        let created = self
            .http
            .send_message(self.channel_id, Vec::new(), &create)
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(MessageId(created.id.get()))
    }

    async fn update(&self, message: MessageId, content: String) -> Result<(), OutboundError> {
        self.http
            .edit_message(
                self.channel_id,
                serenity::all::MessageId::new(message.get()),
                &EditMessage::new().content(denormalize_mention_tags(&content)),
                Vec::new(),
            )
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}

/// Terminal streaming output for origins with no streamable Discord channel
/// (transactional reply tokens, channel-less events, direct messages).
struct UndeliverableChatStream;

#[async_trait]
impl ChatStreamPort for UndeliverableChatStream {
    async fn begin(&self, _message: OutboundMessage) -> Result<MessageId, OutboundError> {
        tracing::warn!("dropping streaming begin: origin has no streamable Discord channel");
        Err(OutboundError::Send("no streamable Discord channel for this origin".to_owned()))
    }

    async fn update(&self, _message: MessageId, _content: String) -> Result<(), OutboundError> {
        tracing::warn!("dropping streaming update: origin has no streamable Discord channel");
        Err(OutboundError::Send("no streamable Discord channel for this origin".to_owned()))
    }
}

struct InteractionFollowupOutput {
    http: Arc<Http>,
    token: String,
    /// Whether the first-followup slot has been consumed. The first followup
    /// of an interaction edits the deferred ephemeral original response and
    /// inherits its ephemeral state regardless of the flags it carries, so a
    /// PUBLIC first reply must be routed around that slot (see [`send`]).
    /// Fresh per interaction: one output instance is built per event origin.
    first_followup_used: AtomicBool,
}

#[async_trait]
impl ChatOutputPort for InteractionFollowupOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        if message.is_empty() {
            tracing::warn!("dropping empty interaction followup (no content, no embeds)");
            return Ok(());
        }
        // Ephemeral replies are invoker-only wherever they land: the first
        // followup edits the deferred ephemeral original (inheriting its
        // state), later ones carry the flag explicitly. Single post.
        if message.ephemeral {
            let body = followup_body(&message);
            return self.post(&body).await;
        }
        // Public reply. The first followup would edit the deferred ephemeral
        // original and inherit its ephemeral state - public content cannot
        // ride it. Consume the slot on a small invoker-only placeholder,
        // then post the real content as a second followup (a new message
        // whose flags we control), and clean the placeholder up. Handlers
        // send sequentially, so "first" is well defined. If the placeholder
        // itself fails, the content post degrades to the inheritance
        // behavior (invoker-only) - the same shape the pre-dance adapter had.
        if !self.first_followup_used.swap(true, Ordering::Relaxed) {
            match self
                .http
                .create_followup_message(&self.token, &placeholder_body(), Vec::new())
                .await
            {
                Ok(placeholder) => {
                    if let Err(err) =
                        self.http.delete_followup_message(&self.token, placeholder.id).await
                    {
                        tracing::warn!(
                            %err,
                            "failed to delete the public-reply placeholder (invoker-only, harmless)"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        %err,
                        "public-reply placeholder failed; content may land invoker-only"
                    );
                }
            }
        }
        // Normalized mention tags invert back to bare mentions on the way
        // out, like every other Discord send - see `followup_body`.
        let body = followup_body(&message);
        self.post(&body).await
    }
}

impl InteractionFollowupOutput {
    /// Posts one followup body for this interaction.
    async fn post(&self, body: &serde_json::Value) -> Result<(), OutboundError> {
        self.http
            .create_followup_message(&self.token, body, Vec::new())
            .await
            .map(|_: serenity::all::Message| ())
            .map_err(|err| OutboundError::Send(err.to_string()))
    }
}

/// Body for the sacrificial first followup that precedes a public send (see
/// [`InteractionFollowupOutput::send`]): invoker-only, generic wording -
/// the adapter is command-agnostic. Deleted again once the real content is
/// out; if deletion fails it lingers as a harmless status line.
fn placeholder_body() -> serde_json::Value {
    serde_json::json!({ "content": "Processing…", "flags": 64 })
}

/// Builds the interaction-followup JSON body. Pure so the flag half of the
/// reply-visibility contract is verifiable side by side with the defer (see
/// `deferred_ephemeral_response`): a followup carries EPHEMERAL when the
/// message is ephemeral; public bodies omit flags - they are only ever
/// posted as second-or-later followups (or past a consumed first slot), so
/// they land as fresh public messages. `reply_to` is ignored: a
/// transactional reply is already anchored to its interaction. Normalized
/// mention tags invert back to bare mentions, like every other Discord send.
fn followup_body(message: &OutboundMessage) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    if !message.content.is_empty() {
        body.insert(
            "content".to_owned(),
            serde_json::json!(denormalize_mention_tags(&message.content)),
        );
    }
    if !message.embeds.is_empty() {
        let embeds: Vec<serde_json::Value> = message
            .embeds
            .iter()
            .map(|embed| {
                serde_json::json!({
                    "title": denormalize_mention_tags(&embed.title),
                    "description": denormalize_mention_tags(&embed.description),
                })
            })
            .collect();
        body.insert("embeds".to_owned(), serde_json::json!(embeds));
    }
    if message.ephemeral {
        // EPHEMERAL flag (1 << 6): visible to the invoking user only.
        body.insert("flags".to_owned(), serde_json::json!(64));
    }
    serde_json::Value::Object(body)
}

fn discord_embed(embed: &Embed) -> serenity::all::CreateEmbed {
    serenity::all::CreateEmbed::new()
        .title(denormalize_mention_tags(&embed.title))
        .description(denormalize_mention_tags(&embed.description))
}

/// Reply reference for a message in the destination channel. `fail_if_not_exists`
/// stays off: a deleted target must degrade to a normal send, never fail the
/// delivery (the LLM guaranteed-answer contract rides these sends).
fn reply_reference(channel_id: SerenityChannelId, reply_to: MessageId) -> MessageReference {
    MessageReference::new(MessageReferenceKind::Default, channel_id)
        .message_id(serenity::all::MessageId::new(reply_to.get()))
        .fail_if_not_exists(false)
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
    fn reaction_tokens_parse_into_their_reaction_kinds() {
        // Fully qualified custom forms build directly, animated flag kept.
        assert_eq!(
            parse_reaction_token("<:face:9>"),
            ParsedReaction::Custom { animated: false, name: "face".to_owned(), id: "9".to_owned() }
        );
        assert_eq!(
            parse_reaction_token("<a:spin:456>"),
            ParsedReaction::Custom {
                animated: true,
                name: "spin".to_owned(),
                id: "456".to_owned()
            }
        );
        assert_eq!(
            parse_reaction_token(":name_id:789"),
            ParsedReaction::Custom {
                animated: false,
                name: "name_id".to_owned(),
                id: "789".to_owned()
            }
        );
        // Bare name needs guild resolution.
        assert_eq!(parse_reaction_token(":dorkiS:"), ParsedReaction::Name("dorkiS".to_owned()));
        assert_eq!(parse_reaction_token(":x:"), ParsedReaction::Name("x".to_owned()));
        // Anything with a non-ASCII character is a Unicode emoji.
        assert_eq!(parse_reaction_token("🤓"), ParsedReaction::Unicode);
        assert_eq!(parse_reaction_token("👍🏽"), ParsedReaction::Unicode);
        // Malformed tokens fail per-token (the plugin's per-item rule); a
        // name-shaped token like `:x:` stays resolvable - the API has the
        // final say on whether the emoji exists.
        for bad in ["word", "::", ":bad name:", "<nope>", "<:bad>", "42", ":id:abc"] {
            assert_eq!(parse_reaction_token(bad), ParsedReaction::Invalid, "token `{bad}`");
        }
    }

    #[test]
    fn custom_reaction_builds_the_serenity_type() {
        let reaction = custom_reaction(true, "spin", "456");
        match reaction {
            ReactionType::Custom { animated, id, name } => {
                assert!(animated);
                assert_eq!(id, EmojiId::new(456));
                assert_eq!(name.as_deref(), Some("spin"));
            }
            other => panic!("expected a custom reaction, got {other:?}"),
        }
    }

    #[test]
    fn mention_tag_parsing_recognizes_the_wire_shapes() {
        assert_eq!(parse_mention_tag("<@123> x"), Some((MentionKind::User, 123, 6)));
        assert_eq!(parse_mention_tag("<@!123>"), Some((MentionKind::User, 123, 7)));
        assert_eq!(parse_mention_tag("<@&7>"), Some((MentionKind::Role, 7, 5)));
        assert_eq!(parse_mention_tag("<#5>"), Some((MentionKind::Channel, 5, 4)));
        assert_eq!(parse_mention_tag("<:face:9>"), None);
        assert_eq!(parse_mention_tag("<@>"), None);
        assert_eq!(parse_mention_tag("<@1x>"), None);
    }

    /// The outbound inverse strips the name prefix only when it is
    /// bracket-adjacent to a mention tag and free of nested brackets;
    /// everything else passes through byte-identical.
    #[test]
    fn denormalize_inverts_the_normalized_shape() {
        assert_eq!(denormalize_mention_tags("hey [Alice]<@123>!"), "hey <@123>!");
        assert_eq!(denormalize_mention_tags("[Mods]<@&7> [general]<#5>"), "<@&7> <#5>");
        // Literal brackets and non-tag shapes pass through.
        assert_eq!(
            denormalize_mention_tags("[b] <@123> [unclosed<@123>"),
            "[b] <@123> [unclosed<@123>"
        );
        assert_eq!(denormalize_mention_tags("[a[b]]<@123>"), "[a[b]]<@123>");
        assert_eq!(denormalize_mention_tags("plain text"), "plain text");
        // Markdown adjacent to a tag is never mangled: the link's bracket
        // pair stays untouched, and names containing parentheses still
        // strip (only nested `[` rejects a candidate).
        assert_eq!(denormalize_mention_tags("[text](url)<@123>"), "[text](url)<@123>");
        assert_eq!(denormalize_mention_tags("[Name (x)]<@123>"), "<@123>");
        assert_eq!(denormalize_mention_tags("[l](u) [Name]<@1>"), "[l](u) <@1>");
    }

    /// Owner => everything; anyone else => @everyone + member roles union.
    /// This is what makes the auth plugin's Discord-admin clamp hold on the
    /// message path, where payloads usually carry no permission bits.
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
    fn followup_body_carries_the_ephemeral_flag() {
        let message = OutboundMessage::text("not authorized".to_owned()).ephemeral();
        let body = followup_body(&message);
        assert_eq!(body.get("flags").and_then(serde_json::Value::as_u64), Some(64));

        let public = OutboundMessage::text("hello".to_owned());
        assert!(followup_body(&public).get("flags").is_none());
    }

    /// The public-reply placeholder consumes the first-followup slot: it
    /// must be non-empty (Discord rejects empty posts) and explicitly
    /// ephemeral, and it stays generic - the adapter is command-agnostic.
    #[test]
    fn placeholder_is_ephemeral_and_non_empty() {
        let body = placeholder_body();
        let content = body.get("content").and_then(serde_json::Value::as_str);
        assert!(content.is_some_and(|text| !text.is_empty()), "placeholder needs content");
        assert_eq!(body.get("flags").and_then(serde_json::Value::as_u64), Some(64));
    }

    /// Followup content and embeds denormalize mention tags back to bare
    /// tags, and a `reply_to` reference is ignored - the interaction itself
    /// is the reply anchor.
    #[test]
    fn followup_body_denormalizes_and_ignores_reply_reference() {
        let message = OutboundMessage::embed(Embed {
            title: "Hey [Alice]<@123>".to_owned(),
            description: "ping [Bob]<@77>".to_owned(),
        })
        .replying_to(MessageId(42));

        let body = followup_body(&message);
        let embeds =
            body.get("embeds").and_then(serde_json::Value::as_array).expect("embeds expected");
        let first = embeds.first().expect("one embed expected");
        assert_eq!(first.get("title").and_then(serde_json::Value::as_str), Some("Hey <@123>"));
        assert_eq!(
            first.get("description").and_then(serde_json::Value::as_str),
            Some("ping <@77>")
        );
        assert!(body.get("message_reference").is_none());
    }

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
