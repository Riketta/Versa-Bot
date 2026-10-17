use std::collections::HashMap;
use std::sync::Arc;

use serenity::all::{
    Attachment, ChannelId as SerenityChannelId, CommandDataOption, CommandDataOptionValue,
    CommandDataResolved, Context, EventHandler, GuildId as SerenityGuildId, Interaction,
    InviteCreateEvent, InviteDeleteEvent, Member, Message, MessageId as SerenityMessageId,
    MessageType, MessageUpdateEvent, Permissions, Ready, RoleId as SerenityRoleId, User,
    UserId as SerenityUserId,
};
use serenity::async_trait;

use crate::infrastructure::outbound_adapters::{GatewayContext, MentionKind, parse_mention_tag};
use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{
        AttachmentPayload, ChannelId, CommandPayload, EventKind, EventPayload, GuildId,
        InvitePayload, MemberPayload, MessageId, Origin, RequestContext, UserId,
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
        // Machines never enter the pipeline: skip bots and webhooks before
        // any cache work below. `message_event` re-checks (it owns the
        // contract, and the tests pin it there) - this early return just
        // keeps skipped messages from touching the cache at all.
        if message.author.bot || message.webhook_id.is_some() {
            return;
        }

        // Message-path permission resolution: payload bits when the platform
        // included them, else the gateway cache - so the auth plugin's
        // Discord-admin clamp holds everywhere.
        let author_permissions = resolve_author_permissions(&ctx, &message);
        // Mention matching needs the bot identity, which lives in the
        // gateway cache and is guaranteed present once messages flow.
        let bot_user_id = ctx.cache.current_user().id.get();
        // Guild display name for the `{guild_name}` turn-template parameter:
        // best effort from the gateway cache, frozen at capture time like
        // the author name. `None` in DMs.
        let guild_name = message
            .guild_id
            .and_then(|guild_id| ctx.cache.guild(guild_id))
            .map(|guild| guild.name.clone());
        // Discord meta-tags are unreadable to consumers (`<@123>`): rewrite
        // them as `[Name]<@123>` so history readers see who was named AND
        // the raw tag to imitate in replies. Names resolve from the
        // payload's mentions first, then the gateway cache; anything
        // unresolvable stays raw. Outbound sends invert the shape (see
        // `denormalize_mention_tags`). The cache guard is scoped: it must
        // be dropped before the pipeline await below.
        let event = {
            let guild = message.guild_id.and_then(|guild_id| ctx.cache.guild(guild_id));
            let mentioned: HashMap<u64, &User> =
                message.mentions.iter().map(|user| (user.id.get(), user)).collect();
            let resolve = |kind: MentionKind, id: u64| {
                resolve_mention_name(&mentioned, guild.as_deref(), kind, id)
            };
            message_event(&message, bot_user_id, author_permissions, guild_name, &resolve)
        };

        if let Some(event) = event {
            self.handler.handle(event).await;
        }
    }

    async fn message_update(
        &self,
        ctx: Context,
        _old_if_available: Option<Message>,
        _new: Option<Message>,
        event: MessageUpdateEvent,
    ) {
        // MESSAGE_UPDATE carries no guild id: resolve it - and the guild
        // display name - from the gateway cache through the channel. DM
        // channels are not cached, degrading to a guild-less event. The
        // lookup is deprecated in serenity without a reverse (channel ->
        // guild) replacement, so the deprecation is acknowledged here.
        // Guards are scoped to this block: they must be dropped before the
        // pipeline await below.
        #[allow(deprecated)]
        let guild_id = ctx.cache.channel(event.channel_id).map(|channel| channel.guild_id);
        let guild_name =
            guild_id.and_then(|guild_id| ctx.cache.guild(guild_id)).map(|guild| guild.name.clone());
        let bot_user_id = ctx.cache.current_user().id.get();

        let event = {
            let guild = guild_id.and_then(|guild_id| ctx.cache.guild(guild_id));
            let mentioned: HashMap<u64, &User> = event
                .mentions
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|user| (user.id.get(), user))
                .collect();
            let resolve = |kind: MentionKind, id: u64| {
                resolve_mention_name(&mentioned, guild.as_deref(), kind, id)
            };
            message_edited_event(&event, guild_id, guild_name, bot_user_id, &resolve)
        };
        if let Some(event) = event {
            self.handler.handle(event).await;
        }
    }

    async fn message_delete(
        &self,
        ctx: Context,
        channel_id: SerenityChannelId,
        deleted_message_id: SerenityMessageId,
        guild_id: Option<SerenityGuildId>,
    ) {
        // Best effort: the gateway cache often still holds the deleted
        // message, so its author is known; otherwise the event identifies
        // only WHICH message vanished (the tracker keeps its own capture
        // for the text).
        let author_id = ctx
            .cache
            .message(channel_id, deleted_message_id)
            .map(|message| message.author.id.get());
        let event = message_deleted_event(guild_id, channel_id, deleted_message_id, author_id);
        self.handler.handle(event).await;
    }

    async fn message_delete_bulk(
        &self,
        _ctx: Context,
        channel_id: SerenityChannelId,
        multiple_deleted_messages_ids: Vec<SerenityMessageId>,
        guild_id: Option<SerenityGuildId>,
    ) {
        // One purge fact, not one event per message - a bulk delete would
        // otherwise flood every consumer with duplicates of the same notice.
        let mut ids = multiple_deleted_messages_ids;
        ids.sort();
        ids.dedup();
        if ids.is_empty() {
            return;
        }
        let event = RequestContext::message_purged(
            Origin {
                guild_id: guild_id.map(|guild_id| GuildId(guild_id.get())),
                channel_id: ChannelId(channel_id.get()),
                // The purge actor is not part of the platform event.
                user_id: UserId(0),
                message_id: None,
                reply_token: None,
            },
            ids.len(),
        );
        self.handler.handle(event).await;
    }

    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        let kind = interaction.kind();
        let Some(command) = interaction.command() else {
            tracing::debug!(
                kind = ?kind,
                "non-command interaction ignored - no taxonomy kind yet"
            );
            return; // components/autocomplete/modal
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

        // Guild-scoped ranks: Discord scopes the interaction payload's
        // permission bits to the invoking channel (overwrites included),
        // so a channel overwrite must neither mint nor strip a rank - the
        // cache's guild-level union is preferred, payload bits only as the
        // best effort left when the guild is not cached. The cache guard is
        // scoped: it must be dropped before the pipeline await below.
        let author_permissions = {
            let cached_guild = command.guild_id.and_then(|guild_id| ctx.cache.guild(guild_id));
            interaction_author_permissions(
                cached_guild.as_deref(),
                command.member.as_ref().and_then(|member| member.permissions),
                command.user.id,
            )
        };

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

    async fn invite_create(&self, _ctx: Context, data: InviteCreateEvent) {
        if let Some(event) = invite_created_event(&data) {
            self.handler.handle(event).await;
        }
    }

    async fn invite_delete(&self, _ctx: Context, data: InviteDeleteEvent) {
        if let Some(event) = invite_deleted_event(&data) {
            self.handler.handle(event).await;
        }
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

/// Origin for invite lifecycle events: pointed at the invite's target
/// channel, with the creator as the acting user where the platform reports
/// one (deletions do not). `UserId(0)` = no acting user - nothing consumes
/// it on these events, and the code is the identity that matters.
fn invite_origin(
    guild_id: SerenityGuildId,
    channel_id: SerenityChannelId,
    creator_id: Option<SerenityUserId>,
) -> Origin {
    Origin {
        guild_id: Some(GuildId(guild_id.get())),
        channel_id: ChannelId(channel_id.get()),
        user_id: UserId(creator_id.map_or(0, |user| user.get())),
        message_id: None,
        reply_token: None,
    }
}

/// INVITE_CREATE normalized: the payload names the code, target channel and
/// creator directly. Guild-less payloads (never seen in practice) are
/// dropped - the event is meaningless without a guild to scope it to.
fn invite_created_event(data: &InviteCreateEvent) -> Option<RequestContext> {
    let guild_id = data.guild_id?;
    Some(RequestContext {
        kind: EventKind::InviteCreated,
        origin: invite_origin(
            guild_id,
            data.channel_id,
            data.inviter.as_ref().map(|u| u.id.clone()),
        ),
        payload: EventPayload::Invite(InvitePayload {
            code: data.code.clone(),
            channel_id: Some(ChannelId(data.channel_id.get())),
            creator_id: data.inviter.as_ref().map(|user| UserId(user.id.get())),
            creator_name: data
                .inviter
                .as_ref()
                .map(|user| user.global_name.clone().unwrap_or_else(|| user.name.clone())),
            max_uses: Some(u64::from(data.max_uses)),
            temporary: Some(data.temporary),
        }),
    })
}

/// INVITE_DELETE normalized: the platform reports only code and channel -
/// creator and limits stay unknown (the tracker's stored record fills the
/// gaps in its audit line).
fn invite_deleted_event(data: &InviteDeleteEvent) -> Option<RequestContext> {
    let guild_id = data.guild_id?;
    Some(RequestContext {
        kind: EventKind::InviteDeleted,
        origin: invite_origin(guild_id, data.channel_id, None),
        payload: EventPayload::Invite(InvitePayload {
            code: data.code.clone(),
            channel_id: Some(ChannelId(data.channel_id.get())),
            creator_id: None,
            creator_name: None,
            max_uses: None,
            temporary: None,
        }),
    })
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

/// Pure core of the message handler: maps a gateway message onto the
/// [`RequestContext`] taxonomy, or `None` for senders that must never enter
/// the pipeline - bots (including this bot's own replies: plugins that need
/// their own turns in a conversation record them at send time) and webhooks,
/// whose output would feed the conversation back into itself. Everything
/// gateway-bound is an input: the author's permission bits (see
/// [`resolve_author_permissions`]), the bot identity behind `mentions_bot`,
/// the cached guild display name, and the mention-name resolver
/// (payload-first, cache-fallback - see [`resolve_mention_name`]).
fn message_event(
    message: &Message,
    bot_user_id: u64,
    author_permissions: u64,
    guild_name: Option<String>,
    resolve: &dyn Fn(MentionKind, u64) -> Option<String>,
) -> Option<RequestContext> {
    // The pipeline never sees machines: a bot author (including this bot's
    // own replies - plugins that need their own turns record them at send
    // time) or a webhook would feed the conversation back into itself.
    if message.author.bot || message.webhook_id.is_some() {
        return None;
    }
    // System notices (member joins, pins, boosts) ride MESSAGE_CREATE with
    // empty content and a human author - they are not conversation turns
    // and must not be captured as one. Three non-Regular types ARE
    // conversation turns: a user reply (`InlineReply`, 19), the opening
    // turn of a composer-created thread (`ThreadStarterMessage`, 21), and
    // plain messages (0). Dropping any of them blacks the feature out at
    // the gateway - silence with no logs.
    if !matches!(
        message.kind,
        MessageType::Regular | MessageType::InlineReply | MessageType::ThreadStarterMessage
    ) {
        return None;
    }

    // Best effort: role data is only present when Discord included the
    // member in the payload; missing roles are treated as "no roles".
    let author_roles: Vec<String> = message
        .member
        .as_ref()
        .map(|member| member.roles.iter().map(|role| role.get().to_string()).collect())
        .unwrap_or_default();
    // Same best-effort display naming: channel nick when present, else
    // the platform display name (global name), else the username -
    // frozen at capture time for history renders.
    let author_name = message
        .member
        .as_ref()
        .and_then(|member| member.nick.clone())
        .or_else(|| message.author.global_name.clone())
        .unwrap_or_else(|| message.author.name.clone());
    // The reply reference survives even when the referenced message is
    // not cached (or was deleted) - unlike `referenced_message`.
    let reply_to = message
        .message_reference
        .as_ref()
        .and_then(|reference| reference.message_id)
        .map(|message_id| MessageId(message_id.get()));
    let mentions_bot = message.mentions.iter().any(|user| user.id.get() == bot_user_id);
    let attachments = attachment_dtos(&message.attachments);

    let origin = Origin {
        guild_id: message.guild_id.map(|guild_id| GuildId(guild_id.get())),
        channel_id: ChannelId(message.channel_id.get()),
        user_id: UserId(message.author.id.get()),
        message_id: Some(MessageId(message.id.get())),
        reply_token: None,
    };

    let content = normalize_mention_tags(&message.content, resolve);

    let mut event = RequestContext::message_received(origin, content);
    if let EventPayload::Message(payload) = &mut event.payload {
        payload.author_name = Some(author_name);
        payload.guild_name = guild_name;
        payload.attachments = attachments;
        payload.author_roles = author_roles;
        payload.author_permissions = author_permissions;
        payload.reply_to = reply_to;
        payload.mentions_bot = mentions_bot;
    }

    Some(event)
}

/// Pure core of the MESSAGE_UPDATE handler: maps a message edit onto
/// [`RequestContext::message_edited`], or `None` when the update is not a
/// human text edit - machine authors (including this bot's own streaming
/// edits), webhook authors, system-ish message types, and updates that
/// carry no text at all (embed unfurls, pin and suppress-embed toggles all
/// ride MESSAGE_UPDATE). The payload is the NEW snapshot; update payloads
/// omit unchanged fields, so roles and permission bits stay unknown and
/// whatever the platform repeats is carried best effort.
fn message_edited_event(
    event: &MessageUpdateEvent,
    guild_id: Option<SerenityGuildId>,
    guild_name: Option<String>,
    bot_user_id: u64,
    resolve: &dyn Fn(MentionKind, u64) -> Option<String>,
) -> Option<RequestContext> {
    let author = event.author.as_ref()?;
    if author.bot || matches!(event.webhook_id, Some(Some(_))) {
        return None;
    }
    let content = event.content.as_deref()?;
    // Edits arrive for conversation turns only; an absent `type` (common in
    // update payloads) counts as Regular.
    if matches!(
        event.kind,
        Some(kind)
            if !matches!(
                kind,
                MessageType::Regular | MessageType::InlineReply | MessageType::ThreadStarterMessage
            )
    ) {
        return None;
    }

    let mentioned: HashMap<u64, &User> =
        event.mentions.as_deref().unwrap_or(&[]).iter().map(|user| (user.id.get(), user)).collect();
    let mentions_bot = mentioned.contains_key(&bot_user_id);
    let reply_to = event
        .message_reference
        .as_ref()
        .and_then(|reference| reference.as_ref())
        .and_then(|reference| reference.message_id)
        .map(|message_id| MessageId(message_id.get()));

    let origin = Origin {
        guild_id: guild_id.map(|guild_id| GuildId(guild_id.get())),
        channel_id: ChannelId(event.channel_id.get()),
        user_id: UserId(author.id.get()),
        message_id: Some(MessageId(event.id.get())),
        reply_token: None,
    };
    let mut edited =
        RequestContext::message_edited(origin, normalize_mention_tags(content, resolve));
    if let EventPayload::Message(payload) = &mut edited.payload {
        payload.author_name = Some(display_name(author));
        payload.guild_name = guild_name;
        payload.attachments = attachment_dtos(event.attachments.as_deref().unwrap_or(&[]));
        payload.reply_to = reply_to;
        payload.mentions_bot = mentions_bot;
    }
    Some(edited)
}

/// Platform-blind attachment DTOs from Discord attachment payloads - the
/// URLs are Discord's own CDN links (the pinned trusted host for
/// plugin-side downloads).
fn attachment_dtos(attachments: &[Attachment]) -> Vec<AttachmentPayload> {
    attachments
        .iter()
        .map(|attachment| AttachmentPayload {
            url: attachment.url.clone(),
            content_type: attachment.content_type.clone(),
            file_name: Some(attachment.filename.clone()),
            size_bytes: u64::from(attachment.size),
            width: attachment.width,
            height: attachment.height,
        })
        .collect()
}

/// MESSAGE_DELETE normalized: the platform names only WHICH message
/// vanished - author and text are unknown here (the gateway cache may
/// still know the author, passed in as `author_id`), so `user_id` degrades
/// to `UserId(0)`.
fn message_deleted_event(
    guild_id: Option<SerenityGuildId>,
    channel_id: SerenityChannelId,
    message_id: SerenityMessageId,
    author_id: Option<u64>,
) -> RequestContext {
    RequestContext::message_deleted(Origin {
        guild_id: guild_id.map(|guild_id| GuildId(guild_id.get())),
        channel_id: ChannelId(channel_id.get()),
        user_id: UserId(author_id.unwrap_or(0)),
        message_id: Some(MessageId(message_id.get())),
        reply_token: None,
    })
}

/// Name behind a mention tag, for content normalization: the message
/// payload's mention list first (the users Discord expanded for us), then
/// the gateway cache (member nick, else username; role and channel names).
/// `None` leaves the raw tag in the content. Pure over its inputs so the
/// payload-first vs cache-fallback precedence stays testable without a
/// live cache.
fn resolve_mention_name(
    mentioned: &HashMap<u64, &User>,
    guild: Option<&serenity::all::Guild>,
    kind: MentionKind,
    id: u64,
) -> Option<String> {
    match kind {
        MentionKind::User => mentioned.get(&id).map(|user| display_name(user)).or_else(|| {
            guild
                .and_then(|guild| guild.members.get(&SerenityUserId::new(id)))
                .map(|member| member.nick.clone().unwrap_or_else(|| member.user.name.clone()))
        }),
        MentionKind::Role => guild
            .and_then(|guild| guild.roles.get(&SerenityRoleId::new(id)))
            .map(|role| role.name.clone()),
        MentionKind::Channel => guild
            .and_then(|guild| guild.channels.get(&SerenityChannelId::new(id)))
            .map(|channel| channel.name.clone()),
    }
}

/// [`Context`]-bound wrapper: turns the gateway payload and cache into the
/// pure decision's inputs.
fn resolve_author_permissions(ctx: &Context, message: &Message) -> u64 {
    let cached_guild = message.guild_id.and_then(|guild_id| ctx.cache.guild(guild_id));
    permissions_from_payload_or_cache(
        message
            .member
            .as_ref()
            .and_then(|member| member.permissions)
            .map(|permissions| permissions.bits()),
        cached_guild.as_deref(),
        message.author.id,
    )
}

/// Message-path permission resolution, pure over its inputs: the payload's
/// permission bits when the platform included them, otherwise the
/// gateway-cache result - owner => everything, else @everyone + member
/// roles union. `cached_guild` is `None` for DMs and for guilds the cache
/// has not seen. 0 = unknown (no bits, no cached guild): authorization
/// treats unknown as "no admin clamp", never as a hard failure.
fn permissions_from_payload_or_cache(
    payload_permissions: Option<u64>,
    cached_guild: Option<&serenity::all::Guild>,
    author_id: SerenityUserId,
) -> u64 {
    if let Some(bits) = payload_permissions {
        return bits;
    }
    match cached_guild {
        Some(guild) => cached_author_permissions(author_id, guild),
        None => 0,
    }
}

/// Interaction-path permission resolution: the cache's guild-level union
/// when the guild is cached (the payload's bits are channel-scoped), the
/// payload bits as the best effort left, 0 = unknown otherwise. Same
/// fail-closed reading of unknown as the message path.
fn interaction_author_permissions(
    cached_guild: Option<&serenity::all::Guild>,
    payload_permissions: Option<Permissions>,
    author_id: SerenityUserId,
) -> u64 {
    match cached_guild {
        Some(guild) => cached_author_permissions(author_id, guild),
        None => payload_permissions.map_or(0, |permissions| permissions.bits()),
    }
}

/// The cache half of [`resolve_author_permissions`]: the owner gets
/// everything, anyone else the union of the @everyone role and their member
/// roles. Pure over the cached guild so the fallback rules stay testable
/// without a live gateway cache.
fn cached_author_permissions(
    message_author_id: SerenityUserId,
    guild: &serenity::all::Guild,
) -> u64 {
    let everyone =
        guild.roles.get(&SerenityRoleId::new(guild.id.get())).map(|role| role.permissions.bits());
    let member_roles: Vec<u64> = guild
        .members
        .get(&message_author_id)
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
        message_author_id.get(),
        everyone,
        &member_roles,
    )
}

/// Effective Discord permissions of a guild member from the gateway cache:
/// the owner gets everything, anyone else the union of the @everyone role
/// and their member roles - with Discord's administrator rule applied (a
/// role carrying ADMINISTRATOR implies every permission), which the auth
/// plugin's rank clamps rely on. Pure so the owner/union rules stay
/// testable without a live cache.
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
    if permissions.contains(Permissions::ADMINISTRATOR) {
        return Permissions::all().bits();
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
/// Subcommand boundaries are flattened away (group identity is lost:
/// sibling subcommands sharing an option name yield duplicate pairs in
/// walk order - no plugin uses subcommands today); resolved entities
/// (users, channels, roles) arrive as their IDs. Attachment options
/// resolve to the attachment's CDN URL - a pinned trusted host (Discord's
/// CDN), which is the only network peer an attachment argument may ever
/// name.
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

    use serenity::all::{
        Attachment, MessageId as SerenityMessageId, MessageReference, MessageReferenceKind,
        MessageType, PartialMember, WebhookId,
    };

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

    // ---- Invite lifecycle normalization ----

    fn invite_create_fixture() -> InviteCreateEvent {
        serde_json::from_value(serde_json::json!({
            "channel_id": "55",
            "code": "abc123",
            "created_at": "2026-10-12T12:00:00Z",
            "guild_id": "1",
            "inviter": { "id": "7", "username": "rikk" },
            "max_age": 86400,
            "max_uses": 1,
            "temporary": false,
            "uses": 0,
        }))
        .expect("invite create fixture deserializes")
    }

    /// INVITE_CREATE arrives with code, target channel and creator - the
    /// taxonomy event carries all three platform-blind.
    #[test]
    fn invite_create_normalizes_code_channel_and_creator() {
        let event = invite_created_event(&invite_create_fixture()).expect("guild-scoped event");
        assert_eq!(event.kind, EventKind::InviteCreated);
        assert_eq!(event.origin.guild_id, Some(GuildId(1)));
        assert_eq!(event.origin.user_id, UserId(7));
        let EventPayload::Invite(invite) = &event.payload else {
            panic!("invite payload expected, got {:?}", event.payload);
        };
        assert_eq!(invite.code, "abc123");
        assert_eq!(invite.channel_id, Some(ChannelId(55)));
        assert_eq!(invite.creator_id, Some(UserId(7)));
        assert_eq!(invite.creator_name.as_deref(), Some("rikk"));
        assert_eq!(invite.max_uses, Some(1));
        assert_eq!(invite.temporary, Some(false));
    }

    /// INVITE_DELETE reports only code and channel - creator and limits
    /// stay unknown, and the acting user degrades to the `UserId(0)`
    /// sentinel.
    #[test]
    fn invite_delete_normalizes_code_only() {
        let data: InviteDeleteEvent = serde_json::from_value(serde_json::json!({
            "channel_id": "55",
            "guild_id": "1",
            "code": "abc123",
        }))
        .expect("invite delete fixture deserializes");

        let event = invite_deleted_event(&data).expect("guild-scoped event");
        assert_eq!(event.kind, EventKind::InviteDeleted);
        assert_eq!(event.origin.user_id, UserId(0));
        let EventPayload::Invite(invite) = &event.payload else {
            panic!("invite payload expected, got {:?}", event.payload);
        };
        assert_eq!(invite.code, "abc123");
        assert_eq!(invite.creator_id, None);
        assert_eq!(invite.max_uses, None);
    }

    /// Guild-less invite payloads (never seen in practice) are dropped -
    /// the event is meaningless without a guild to scope it to.
    #[test]
    fn guildless_invite_events_are_dropped() {
        let mut data = invite_create_fixture();
        data.guild_id = None;
        assert!(invite_created_event(&data).is_none());
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

    /// Discord's administrator rule: a role carrying ADMINISTRATOR implies
    /// every permission - the auth plugin's rank clamps rely on this.
    #[test]
    fn unioned_permissions_administrator_implies_everything() {
        let bits = unioned_member_permissions(
            1,
            2,
            Some(Permissions::VIEW_CHANNEL.bits()),
            &[Permissions::ADMINISTRATOR.bits()],
        );
        assert_eq!(bits, Permissions::all().bits());
    }

    /// Interaction permissions are guild-scoped: the cache's union wins
    /// over the payload's channel-scoped bits; without a cached guild the
    /// payload bits are the best effort left; without either, unknown = 0.
    #[test]
    fn interaction_permissions_prefer_guild_cache_over_payload() {
        let mut guild = serenity::all::Guild::default();
        guild.id = SerenityGuildId::new(7);
        guild.owner_id = SerenityUserId::new(1);
        let mut everyone = serenity::all::Role::default();
        everyone.id = SerenityRoleId::new(7);
        everyone.permissions = Permissions::VIEW_CHANNEL;
        guild.roles.insert(everyone.id, everyone);

        // Cached guild: the payload's channel-scoped Manage Server bit is
        // ignored in favor of the guild-level computation.
        let bits = interaction_author_permissions(
            Some(&guild),
            Some(Permissions::MANAGE_GUILD),
            SerenityUserId::new(2),
        );
        assert_eq!(bits, Permissions::VIEW_CHANNEL.bits());

        // No cache: the payload bits are all that is left.
        let bits = interaction_author_permissions(
            None,
            Some(Permissions::MANAGE_GUILD),
            SerenityUserId::new(2),
        );
        assert_eq!(bits, Permissions::MANAGE_GUILD.bits());

        // Neither: unknown, treated as no clamp.
        assert_eq!(interaction_author_permissions(None, None, SerenityUserId::new(2)), 0);
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
            { "name": "channel", "type": 7, "value": "130000000000000001" },
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
                ("channel".to_owned(), "130000000000000001".to_owned()),
                ("inner".to_owned(), "value".to_owned()),
            ]
        );
    }

    /// Group identity is flattened away: sibling subcommands sharing an
    /// option name yield duplicate pairs in walk order - pinned so the
    /// loss stays a conscious, visible contract.
    #[test]
    fn sibling_subcommand_options_keep_duplicate_names_in_walk_order() {
        let options: Vec<CommandDataOption> = serde_json::from_value(serde_json::json!([
            { "name": "group", "type": 2, "options": [
                { "name": "add", "type": 1, "options": [
                    { "name": "name", "type": 3, "value": "one" }
                ] },
                { "name": "remove", "type": 1, "options": [
                    { "name": "name", "type": 3, "value": "two" }
                ] }
            ]}
        ]))
        .expect("test options expected to deserialize");

        let args = flatten_options(&options, &CommandDataResolved::default());

        assert_eq!(
            args,
            vec![("name".to_owned(), "one".to_owned()), ("name".to_owned(), "two".to_owned()),]
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

    /// The cache fallback rules: owner shortcut, everyone+roles union,
    /// unknown-member default.
    #[test]
    fn cached_permissions_resolve_owner_and_role_union() {
        fn role(id: SerenityRoleId, permissions: Permissions) -> serenity::all::Role {
            let mut role = serenity::all::Role::default();
            role.id = id;
            role.permissions = permissions;
            role
        }

        let mut guild = serenity::all::Guild::default();
        guild.id = SerenityGuildId::new(7);
        guild.owner_id = SerenityUserId::new(1);
        guild.roles.insert(
            SerenityRoleId::new(7),
            role(SerenityRoleId::new(7), Permissions::SEND_MESSAGES),
        );
        guild.roles.insert(
            SerenityRoleId::new(8),
            role(SerenityRoleId::new(8), Permissions::ADMINISTRATOR),
        );

        let mut member = serenity::all::Member::default();
        member.roles = vec![SerenityRoleId::new(8)];
        guild.members.insert(SerenityUserId::new(2), member);

        // The owner shortcut.
        assert_eq!(
            cached_author_permissions(SerenityUserId::new(1), &guild),
            Permissions::all().bits()
        );
        // A member: union of @everyone and their own roles.
        let bits = cached_author_permissions(SerenityUserId::new(2), &guild);
        assert_ne!(bits & Permissions::SEND_MESSAGES.bits(), 0);
        assert_ne!(bits & Permissions::ADMINISTRATOR.bits(), 0);
        // An unknown member: @everyone only.
        assert_eq!(
            cached_author_permissions(SerenityUserId::new(3), &guild),
            Permissions::SEND_MESSAGES.bits()
        );
    }

    // ---- Message event mapping (the gateway's inbound half) ----

    fn user(id: u64, name: &str) -> User {
        let mut user = User::default();
        user.id = SerenityUserId::new(id);
        user.name = name.to_owned();
        user
    }

    /// A DM from a plain human account: no guild, no member data, no
    /// attachments, no mentions. Tests add the extras they need.
    fn plain_message(content: &str) -> Message {
        let mut message = Message::default();
        message.id = SerenityMessageId::new(1000);
        message.channel_id = SerenityChannelId::new(2000);
        message.author = user(42, "human");
        message.content = content.to_owned();
        message
    }

    fn no_names(_: MentionKind, _: u64) -> Option<String> {
        None
    }

    /// The handler's resolver wiring, mirrored for tests: payload mentions
    /// indexed by id, the gateway cache as fallback.
    fn payload_then_cache_resolver<'a>(
        message: &'a Message,
        guild: Option<&'a serenity::all::Guild>,
    ) -> impl Fn(MentionKind, u64) -> Option<String> + 'a {
        let mentioned: HashMap<u64, &User> =
            message.mentions.iter().map(|user| (user.id.get(), user)).collect();
        move |kind, id| resolve_mention_name(&mentioned, guild, kind, id)
    }

    /// The message payload of a successfully mapped event.
    fn message_payload(event: RequestContext) -> crate::kernel::models::MessagePayload {
        match event.payload {
            EventPayload::Message(payload) => payload,
            other => panic!("message payload expected, got {other:?}"),
        }
    }

    fn attachment(url: &str, filename: &str, content_type: Option<&str>) -> Attachment {
        serde_json::from_value(serde_json::json!({
            "id": "130000000000000002",
            "filename": filename,
            "size": 42,
            "url": url,
            "proxy_url": "https://media.discordapp.net/attachments/1/2/x",
            "content_type": content_type,
            "width": 640,
            "height": 480
        }))
        .expect("test attachment expected to deserialize")
    }

    /// The pipeline never sees machines: a bot author (including this
    /// bot's own replies) produces no event.
    #[test]
    fn bot_author_message_produces_no_event() {
        let mut message = plain_message("beep boop");
        message.author.bot = true;

        assert!(message_event(&message, 99, 0, None, &no_names).is_none());
    }

    /// Webhook-authored messages are skipped too, whatever the author
    /// field claims: `webhook_id` is the tell, not the bot flag.
    #[test]
    fn webhook_message_produces_no_event() {
        let mut message = plain_message("spoofed human");
        message.webhook_id = Some(WebhookId::new(77));

        assert!(message_event(&message, 99, 0, None, &no_names).is_none());
    }

    /// System notices (member joins, pins, boosts) ride MESSAGE_CREATE as
    /// human-authored, contentless messages - they are not conversation
    /// turns and must not be captured.
    #[test]
    fn system_message_produces_no_event() {
        let mut message = plain_message("");
        message.kind = MessageType::MemberJoin;

        assert!(message_event(&message, 99, 0, None, &no_names).is_none());
    }

    /// A human message lands on the taxonomy: kind, origin ids, content
    /// and the capture-time payload fields (guild name, permission bits).
    #[test]
    fn human_message_maps_onto_the_taxonomy() {
        let mut message = plain_message("hello world");
        message.guild_id = Some(SerenityGuildId::new(3000));

        let event = message_event(&message, 99, 0x400, Some("Forge".to_owned()), &no_names)
            .expect("human message expected to become an event");

        assert_eq!(event.kind, EventKind::MessageReceived);
        assert_eq!(event.origin.guild_id, Some(GuildId(3000)));
        assert_eq!(event.origin.channel_id, ChannelId(2000));
        assert_eq!(event.origin.user_id, UserId(42));
        assert_eq!(event.origin.message_id, Some(MessageId(1000)));
        assert_eq!(event.origin.reply_token, None);
        let payload = message_payload(event);
        assert_eq!(payload.content, "hello world");
        assert_eq!(payload.author_name.as_deref(), Some("human"));
        assert_eq!(payload.guild_name.as_deref(), Some("Forge"));
        assert_eq!(payload.author_permissions, 0x400);
        assert!(payload.attachments.is_empty());
        assert_eq!(payload.reply_to, None);
        assert!(!payload.mentions_bot);
    }

    /// `reply_to` comes from `message_reference` and survives the
    /// referenced message being uncached or deleted
    /// (`referenced_message` stays `None`). Real Discord replies carry
    /// their own message type (`InlineReply`, 19) - the shape below must
    /// stay truthful or the type filter would silently drop them again.
    #[test]
    fn reply_reference_is_captured_as_reply_to() {
        let mut message = plain_message("a reply");
        message.kind = MessageType::InlineReply;
        message.message_reference = Some(
            MessageReference::new(MessageReferenceKind::Default, SerenityChannelId::new(2000))
                .message_id(SerenityMessageId::new(555)),
        );

        let event = message_event(&message, 99, 0, None, &no_names).expect("event expected");

        assert_eq!(message_payload(event).reply_to, Some(MessageId(555)));
    }

    /// Regression guard for the reply blackout: `InlineReply` (19) is a
    /// conversation turn, not a system notice - only the notice types are
    /// dropped. Filtering on `Regular` alone produced "no event at all"
    /// for every user reply.
    #[test]
    fn reply_message_produces_an_event() {
        let mut message = plain_message("answering the bot");
        message.kind = MessageType::InlineReply;
        message.message_reference = Some(
            MessageReference::new(MessageReferenceKind::Default, SerenityChannelId::new(2000))
                .message_id(SerenityMessageId::new(555)),
        );

        let event =
            message_event(&message, 99, 0, None, &no_names).expect("reply must become an event");
        assert_eq!(event.kind, EventKind::MessageReceived);
        assert_eq!(message_payload(event).reply_to, Some(MessageId(555)));
    }

    /// Same guard for threads: the opening turn of a composer-created
    /// thread is `ThreadStarterMessage` (21), a real human turn whose
    /// reference points at the thread-created notice in the parent.
    #[test]
    fn thread_starter_message_produces_an_event() {
        let mut message = plain_message("kicking the thread off");
        message.kind = MessageType::ThreadStarterMessage;
        message.message_reference = Some(
            MessageReference::new(MessageReferenceKind::Default, SerenityChannelId::new(2000))
                .message_id(SerenityMessageId::new(555)),
        );

        let event = message_event(&message, 99, 0, None, &no_names)
            .expect("thread starter must become an event");
        assert_eq!(event.kind, EventKind::MessageReceived);
        assert_eq!(message_payload(event).reply_to, Some(MessageId(555)));
    }

    // ---- Message edit/deletion mapping ----

    /// A realistic MESSAGE_UPDATE payload: Discord repeats most fields and
    /// omits unchanged ones. Built through the wire shape on purpose.
    fn update_event(json: &str) -> MessageUpdateEvent {
        serde_json::from_str::<MessageUpdateEvent>(json).expect("update payload deserializes")
    }

    fn update_json(extra: &str) -> String {
        format!(
            r#"{{
                "id": "1000",
                "channel_id": "2000",
                "author": {{ "id": "42", "username": "human", "discriminator": "1" }},
                "content": "edited text",
                {extra}
                "type": 0
            }}"#
        )
    }

    #[test]
    fn message_update_maps_onto_the_edit_taxonomy() {
        let event = message_edited_event(
            &update_event(&update_json(r#""mentions": [], "#)),
            Some(SerenityGuildId::new(3000)),
            Some("Forge".to_owned()),
            99,
            &no_names,
        )
        .expect("human text edit expected to become an event");

        assert_eq!(event.kind, EventKind::MessageEdited);
        assert_eq!(event.origin.guild_id, Some(GuildId(3000)));
        assert_eq!(event.origin.channel_id, ChannelId(2000));
        assert_eq!(event.origin.user_id, UserId(42));
        assert_eq!(event.origin.message_id, Some(MessageId(1000)));
        let payload = message_payload(event);
        assert_eq!(payload.content, "edited text");
        assert_eq!(payload.author_name.as_deref(), Some("human"));
        assert_eq!(payload.guild_name.as_deref(), Some("Forge"));
        assert!(!payload.mentions_bot);
    }

    #[test]
    fn message_update_of_a_bot_or_webhook_produces_no_event() {
        // The bot's own streaming edits (bot author) must not enter the
        // pipeline.
        let bot = serde_json::from_str::<MessageUpdateEvent>(
            r#"{
                "id": "1000", "channel_id": "2000",
                "author": { "id": "42", "username": "robot", "bot": true },
                "content": "streaming edit", "type": 0
            }"#,
        )
        .expect("bot update deserializes");
        assert!(message_edited_event(&bot, None, None, 99, &no_names).is_none());

        let webhook = update_event(
            r#"{
                "id": "1000", "channel_id": "2000",
                "author": { "id": "42", "username": "hook" },
                "content": "hook edit", "webhook_id": "77", "type": 0
            }"#,
        );
        assert!(message_edited_event(&webhook, None, None, 99, &no_names).is_none());
    }

    #[test]
    fn message_update_without_text_is_not_an_edit() {
        // Embed unfurls, pin state and suppress-embed toggles ride
        // MESSAGE_UPDATE with no `content` field.
        let no_content = update_event(
            r#"{
                "id": "1000", "channel_id": "2000",
                "author": { "id": "42", "username": "human" },
                "pinned": true
            }"#,
        );
        assert!(message_edited_event(&no_content, None, None, 99, &no_names).is_none());

        // A missing author (rare partial payload) is likewise unmappable.
        let no_author = update_event(r#"{ "id": "1000", "channel_id": "2000", "content": "x" }"#);
        assert!(message_edited_event(&no_author, None, None, 99, &no_names).is_none());

        // System-ish types never arrive as human edits.
        let system = update_event(
            r#"{
                "id": "1000", "channel_id": "2000",
                "author": { "id": "42", "username": "human" },
                "content": "x", "type": 7
            }"#,
        );
        assert!(message_edited_event(&system, None, None, 99, &no_names).is_none());
    }

    #[test]
    fn message_delete_and_purge_map_onto_the_taxonomy() {
        let deleted = message_deleted_event(
            Some(SerenityGuildId::new(3000)),
            SerenityChannelId::new(2000),
            SerenityMessageId::new(1000),
            Some(42),
        );
        assert_eq!(deleted.kind, EventKind::MessageDeleted);
        assert_eq!(deleted.origin.guild_id, Some(GuildId(3000)));
        assert_eq!(deleted.origin.message_id, Some(MessageId(1000)));
        assert_eq!(deleted.origin.user_id, UserId(42));

        // Without the gateway cache, the author is unknown - the sentinel
        // keeps the origin total while the tracker fills the real name.
        let unknown = message_deleted_event(
            None,
            SerenityChannelId::new(2000),
            SerenityMessageId::new(1000),
            None,
        );
        assert_eq!(unknown.origin.user_id, UserId(0));
        assert_eq!(unknown.origin.guild_id, None);
        assert!(matches!(unknown.payload, EventPayload::Empty));
    }

    /// Attachments become platform-blind DTOs that preserve the CDN url
    /// (the pinned trusted host), file name, MIME type, size and
    /// dimensions.
    #[test]
    fn attachments_map_to_payload_dtos_preserving_the_metadata() {
        let mut message = plain_message("two files");
        message.attachments = vec![
            attachment("https://cdn.discordapp.com/attachments/1/2/notes.md", "notes.md", None),
            attachment(
                "https://cdn.discordapp.com/attachments/1/2/shot.png",
                "shot.png",
                Some("image/png"),
            ),
        ];

        let event = message_event(&message, 99, 0, None, &no_names).expect("event expected");
        let payload = message_payload(event);

        let first = payload.attachments.first().expect("first attachment expected");
        assert_eq!(first.url, "https://cdn.discordapp.com/attachments/1/2/notes.md");
        assert_eq!(first.file_name.as_deref(), Some("notes.md"));
        assert_eq!(first.content_type, None);
        assert_eq!(first.size_bytes, 42);
        assert_eq!(first.width, Some(640));
        assert_eq!(first.height, Some(480));
        let second = payload.attachments.get(1).expect("second attachment expected");
        assert_eq!(second.file_name.as_deref(), Some("shot.png"));
        assert_eq!(second.content_type.as_deref(), Some("image/png"));
    }

    /// `mentions_bot` is resolved against the bot identity the adapter was
    /// given: a message mentioning the bot triggers, mentioning only
    /// others does not.
    #[test]
    fn mentions_bot_matches_only_the_given_bot_identity() {
        let mut message = plain_message("hey <@99> and <@7>");
        message.mentions = vec![user(99, "the bot"), user(7, "someone")];

        let event = message_event(&message, 99, 0, None, &no_names).expect("event expected");
        assert!(message_payload(event).mentions_bot);

        let mut message = plain_message("hey <@7>");
        message.mentions = vec![user(7, "someone")];

        let event = message_event(&message, 99, 0, None, &no_names).expect("event expected");
        assert!(!message_payload(event).mentions_bot);
    }

    /// Capture-time author data prefers the payload member: channel nick
    /// over global display name over username; role ids pass through as
    /// opaque strings.
    #[test]
    fn member_capture_fields_prefer_the_payload_member() {
        let mut member = serenity::all::Member::default();
        member.nick = Some("Nicky".to_owned());
        member.roles = vec![SerenityRoleId::new(8)];
        let mut message = plain_message("guild hello");
        message.member = Some(Box::new(PartialMember::from(member)));
        message.author.global_name = Some("GlobalName".to_owned());
        message.guild_id = Some(SerenityGuildId::new(3000));

        let event = message_event(&message, 99, 0, None, &no_names).expect("event expected");
        let payload = message_payload(event);

        assert_eq!(payload.author_name.as_deref(), Some("Nicky"));
        assert_eq!(payload.author_roles, vec!["8".to_owned()]);
    }

    /// Mention names resolve from the payload's mention list first (the
    /// users Discord expanded), then the gateway cache: nick when set,
    /// else username. Ids in neither source stay unresolvable (`None`).
    #[test]
    fn mention_names_prefer_payload_then_cache() {
        let mut with_nick = serenity::all::Member::default();
        with_nick.nick = Some("CachedNick".to_owned());
        with_nick.user.name = "CachedName".to_owned();
        let mut without_nick = serenity::all::Member::default();
        without_nick.user.name = "CachedUsername".to_owned();
        let mut guild = serenity::all::Guild::default();
        guild.members.insert(SerenityUserId::new(7), with_nick);
        guild.members.insert(SerenityUserId::new(3), without_nick);

        let mut message = plain_message("hey <@5> <@7> <@3> <@123>");
        message.mentions = vec![user(5, "PayloadName"), user(7, "PayloadBeatsCache")];
        let resolve = payload_then_cache_resolver(&message, Some(&guild));

        // Payload first...
        assert_eq!(resolve(MentionKind::User, 5).as_deref(), Some("PayloadName"));
        // ...it beats the cache...
        assert_eq!(resolve(MentionKind::User, 7).as_deref(), Some("PayloadBeatsCache"));
        // ...which serves nick-then-username for everyone else.
        assert_eq!(resolve(MentionKind::User, 3).as_deref(), Some("CachedUsername"));
        assert_eq!(resolve(MentionKind::User, 123), None);
    }

    /// Roles resolve from the cache only (the payload's mention list is
    /// users), and through `message_event` the resolved names land in the
    /// normalized content.
    #[test]
    fn content_normalization_wires_the_resolver_with_payload_and_cache() {
        let mut role = serenity::all::Role::default();
        role.id = SerenityRoleId::new(7);
        role.name = "Mods".to_owned();
        let mut guild = serenity::all::Guild::default();
        guild.roles.insert(SerenityRoleId::new(7), role);

        let mut message = plain_message("hey <@5> and <@&7>");
        message.mentions = vec![user(5, "PayloadName")];
        let resolve = payload_then_cache_resolver(&message, Some(&guild));

        let event = message_event(&message, 99, 0, None, &resolve).expect("event expected");

        assert_eq!(message_payload(event).content, "hey [PayloadName]<@5> and [Mods]<@&7>");
    }

    /// Permission precedence: payload bits win even when the cache would
    /// compute something else; without payload bits, a missing cache entry
    /// (DM or uncached guild) and a cache that grants nothing both
    /// resolve to 0 = unknown.
    #[test]
    fn author_permissions_prefer_payload_then_cache() {
        let mut guild = serenity::all::Guild::default();
        guild.owner_id = SerenityUserId::new(2);

        // Payload bits beat the cache (user 2 is the owner: the cache
        // would grant everything).
        assert_eq!(
            permissions_from_payload_or_cache(Some(0x10), Some(&guild), SerenityUserId::new(2)),
            0x10
        );
        // Payload bits carry even without any cached guild (DM-shaped
        // interaction payload).
        assert_eq!(
            permissions_from_payload_or_cache(Some(0x400), None, SerenityUserId::new(1)),
            0x400
        );

        // No payload bits: a DM (no guild) is unknown.
        assert_eq!(permissions_from_payload_or_cache(None, None, SerenityUserId::new(1)), 0);
        // And so is a cache that grants the author nothing.
        assert_eq!(
            permissions_from_payload_or_cache(None, Some(&guild), SerenityUserId::new(3)),
            0
        );
    }
}
