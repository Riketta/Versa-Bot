use std::sync::{Arc, OnceLock};

use serenity::all::{
    ChannelId as SerenityChannelId, CommandDataOption, CommandDataOptionValue, Context,
    CreateMessage, EventHandler, GuildId as SerenityGuildId, Http, Interaction, Member, Message,
    Ready, User,
};
use serenity::async_trait;

use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{
        ChannelId, CommandPayload, Embed, EventKind, EventPayload, GuildId, MemberPayload,
        MessageId, Origin, OutboundError, OutboundMessage, Platform, RequestContext, UserId,
    },
    spi_ports::{ChatOutputFactoryPort, ChatOutputPort},
};

/// Kernel driving adapter: normalizes Discord gateway events onto the
/// chat-agnostic `RequestContext` taxonomy and pushes each through the
/// pipeline. Platform specifics (serenity types) never cross this boundary.
pub struct DiscordGatewayAdapter<H: RequestHandlerPort> {
    handler: H,
    /// Filled on `ready` so outbound adapters (presence) can drive the
    /// gateway; this adapter is the only writer.
    context: Arc<OnceLock<Context>>,
}

impl<H: RequestHandlerPort> DiscordGatewayAdapter<H> {
    pub fn new(handler: H, context: Arc<OnceLock<Context>>) -> Self {
        Self { handler, context }
    }
}

#[async_trait]
impl<H: RequestHandlerPort> EventHandler for DiscordGatewayAdapter<H> {
    async fn message(&self, _ctx: Context, message: Message) {
        // Bots and webhooks both feed noise into the pipeline.
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

        let origin = Origin {
            platform: Platform::Discord,
            guild_id: message.guild_id.map(|guild_id| GuildId(guild_id.get())),
            channel_id: ChannelId(message.channel_id.get()),
            user_id: UserId(message.author.id.get()),
            message_id: Some(MessageId(message.id.get())),
            reply_token: None,
        };

        let mut event = RequestContext::message_received(origin, message.content);
        if let EventPayload::Message(payload) = &mut event.payload {
            payload.author_roles = author_roles;
            payload.author_permissions = author_permissions;
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
        // the interaction token. Type 5 = DeferredChannelMessageWithSource;
        // flags 64 (EPHEMERAL) shows the "thinking" indicator to the invoker
        // only, so denied or failed commands no longer flash publicly. Later
        // followups control their own visibility - public replies still work.
        if let Err(err) = ctx
            .http
            .create_interaction_response(
                command.id,
                &command.token,
                &serde_json::json!({ "type": 5, "flags": 64 }),
                Vec::new(),
            )
            .await
        {
            tracing::error!(%err, "failed to defer interaction");
            return;
        }

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
                args: flatten_options(&command.data.options),
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
        let _ = self.context.set(ctx);
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

/// Flattens Discord's option tree into `name -> value` string pairs.
/// Subcommand boundaries are flattened away; resolved entities (users,
/// channels, roles) arrive as their IDs.
fn flatten_options(options: &[CommandDataOption]) -> Vec<(String, String)> {
    fn walk(options: &[CommandDataOption], args: &mut Vec<(String, String)>) {
        fn push_id(name: &str, id: impl std::fmt::Display, args: &mut Vec<(String, String)>) {
            args.push((name.to_owned(), id.to_string()));
        }

        for option in options {
            match &option.value {
                CommandDataOptionValue::SubCommand(inner)
                | CommandDataOptionValue::SubCommandGroup(inner) => walk(inner, args),
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
                CommandDataOptionValue::Attachment(_)
                | CommandDataOptionValue::Unknown(_)
                | CommandDataOptionValue::Autocomplete { .. }
                | _ => {}
            }
        }
    }

    let mut args = Vec::new();
    walk(options, &mut args);
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
    pub fn new(http: Http) -> Self {
        Self { http: Arc::new(http) }
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
        // meaning here and is ignored.
        let mut create = CreateMessage::new();
        if !message.content.is_empty() {
            create = create.content(message.content);
        }
        if !message.embeds.is_empty() {
            create = create.embeds(message.embeds.iter().map(discord_embed).collect());
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
        let mut body = serde_json::Map::new();
        if !message.content.is_empty() {
            body.insert("content".to_owned(), serde_json::json!(message.content));
        }
        if !message.embeds.is_empty() {
            let embeds: Vec<serde_json::Value> = message
                .embeds
                .iter()
                .map(|embed| {
                    serde_json::json!({
                        "title": embed.title,
                        "description": embed.description,
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
        .title(embed.title.clone())
        .description(embed.description.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let args = flatten_options(&options);

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
}
