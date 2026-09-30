use std::sync::Arc;

use serenity::all::{
    ChannelId as SerenityChannelId, CommandDataOption, CommandDataOptionValue, Context,
    CreateMessage, EventHandler, Http, Interaction, Message, Ready,
};
use serenity::async_trait;

use crate::kernel::{
    api_ports::RequestHandlerPort,
    models::{
        ChannelId, CommandPayload, EventKind, EventPayload, GuildId, MessageId, Origin,
        OutboundError, OutboundMessage, Platform, RequestContext, UserId,
    },
    spi_ports::{ChatOutputFactoryPort, ChatOutputPort},
};

/// Kernel driving adapter: normalizes Discord gateway events onto the
/// chat-agnostic `RequestContext` taxonomy and pushes each through the
/// pipeline. Platform specifics (serenity types) never cross this boundary.
pub struct DiscordGatewayAdapter<H: RequestHandlerPort> {
    handler: H,
}

impl<H: RequestHandlerPort> DiscordGatewayAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl<H: RequestHandlerPort> EventHandler for DiscordGatewayAdapter<H> {
    async fn message(&self, _ctx: Context, message: Message) {
        if message.author.bot {
            return;
        }

        // Best effort: role data is only present when Discord included the
        // member in the payload. Authorization treats missing roles as "no
        // roles", never as a hard failure.
        let author_roles: Vec<String> = message
            .member
            .as_ref()
            .map(|member| {
                member
                    .roles
                    .iter()
                    .map(|role| role.get().to_string())
                    .collect()
            })
            .unwrap_or_default();

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
        if let Err(err) = ctx.http.create_interaction_response(
            command.id,
            &command.token,
            &serde_json::json!({ "type": 5 }),
            Vec::new(),
        ).await {
            tracing::error!(%err, "failed to defer interaction");
            return;
        }

        let author_roles: Vec<String> = command
            .member
            .as_ref()
            .map(|member| {
                member
                    .roles
                    .iter()
                    .map(|role| role.get().to_string())
                    .collect()
            })
            .unwrap_or_default();

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
            }),
        };

        self.handler.handle(event).await;
    }

    async fn ready(&self, _ctx: Context, ready: Ready) {
        tracing::info!("connected as {}", ready.user.name);
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
        Self {
            http: Arc::new(http),
        }
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

        Arc::new(SerenityChatOutput {
            http: Arc::clone(&self.http),
            channel_id: SerenityChannelId::new(origin.channel_id.get()),
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
        self.http
            .send_message(
                self.channel_id,
                Vec::new(),
                &CreateMessage::new().content(message.content),
            )
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}

struct InteractionFollowupOutput {
    http: Arc<Http>,
    token: String,
}

#[async_trait]
impl ChatOutputPort for InteractionFollowupOutput {
    async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
        self.http
            .create_followup_message(
                &self.token,
                &serde_json::json!({ "content": message.content }),
                Vec::new(),
            )
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn option(name: &str, value: serde_json::Value) -> CommandDataOption {
        serde_json::from_value(serde_json::json!({ "name": name, "value": value }))
            .expect("test option expected to deserialize")
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
