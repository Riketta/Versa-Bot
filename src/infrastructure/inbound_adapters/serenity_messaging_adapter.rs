use std::collections::HashMap;
use std::sync::Arc;

use serenity::all::{
    ChannelId as SerenityChannelId, CommandDataOption, CommandDataOptionValue, CommandDataResolved,
    Context, CreateMessage, EditMessage, EventHandler, GuildId as SerenityGuildId, Http,
    Interaction, Member, Message, MessageReference, MessageReferenceKind, Ready,
    RoleId as SerenityRoleId, User, UserId as SerenityUserId,
};
use serenity::async_trait;
use tokio_util::sync::CancellationToken;

use crate::infrastructure::outbound_adapters::GatewayContext;
use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{
        AttachmentPayload, ChannelId, CommandPayload, Embed, EventKind, EventPayload, GuildId,
        MemberPayload, MessageId, Origin, OutboundError, OutboundMessage, Platform, RequestContext,
        UserId,
    },
    spi_ports::{ChatOutputFactoryPort, ChatOutputPort, ChatStreamPort, ChatTypingGuard},
};

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
        let author_permissions = message
            .member
            .as_ref()
            .and_then(|member| member.permissions)
            .map_or(0, |permissions| permissions.bits());
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
            platform: Platform::Discord,
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
        if let EventPayload::Message(payload) = &mut event.payload {
            payload.author_name = Some(author_name);
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
        // ignored, and a non-ephemeral defer makes the whole exchange public
        // - the first followup edits the original response and inherits its
        // ephemeral state (an existing message's ephemeral state cannot be
        // changed later). All command replies are admin-only, so the defer
        // carries EPHEMERAL (64) and the loading state already shows to the
        // invoker alone. (A future public-reply command would follow up a
        // second time with its own flags.)
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
            platform: Platform::Discord,
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
        platform: Platform::Discord,
        guild_id: Some(GuildId(guild_id.get())),
        channel_id: ChannelId(0),
        user_id: UserId(user.id.get()),
        message_id: None,
        reply_token: None,
    }
}

/// The acknowledgement sent for every slash-command interaction: a deferred
/// channel message whose loading state carries the EPHEMERAL flag (64). The
/// first followup then edits this response and inherits its ephemeral state,
/// so the whole exchange stays visible to the invoker alone. `flags` sits
/// inside `data` per the interaction-callback data shape - a top-level
/// `flags` field is silently ignored by Discord and would make the defer
/// (and with it every command reply) public.
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

/// The outbound inverse: `[Name]<@id>` -> `<@id>`, so tags the model
/// assembles from normalized history arrive as clean Discord mentions
/// (rendered output shows the resolved name anyway). The name segment must
/// be bracket-adjacent to the tag and free of nested brackets - text that
/// never used the normalized shape passes through byte-identical.
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
                _ => {}
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
}

#[async_trait]
impl ChatOutputPort for InteractionFollowupOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        if message.is_empty() {
            tracing::warn!("dropping empty interaction followup (no content, no embeds)");
            return Ok(());
        }
        // `reply_to` is ignored: a transactional reply is already anchored to
        // its interaction - a channel message reference adds nothing.
        // Normalized mention tags invert back to bare mentions on the way
        // out, like every other Discord send.
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

        self.http
            .create_followup_message(&self.token, &serde_json::Value::Object(body), Vec::new())
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
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
