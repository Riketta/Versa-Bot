//! `/set_guild_name`: the bot's guild-local display name - how members of
//! THIS server see it (Discord maps this to the member nickname). Nothing is
//! stored anywhere: the platform owns the value, the command only drives it
//! through [`NicknamePort`]. An omitted, empty or whitespace `name` resets
//! to the bot's real name.

use std::sync::Arc;

use async_trait::async_trait;

use crate::common::command_reply;
use crate::kernel::{
    models::{PluginError, RequestContext},
    plugin_ports::{
        AccessTier, ArgDescriptor, ArgKind, CommandArgs, CommandDescriptor, CommandHandler,
        CommandRegistryPort, PluginPort,
    },
    services::KernelServices,
    spi_ports::NicknamePort,
};

/// Discord's nickname length cap - enforced locally so the admin gets a
/// usage notice instead of a platform rejection.
const MAX_NICKNAME_CHARS: usize = 32;

pub struct NicknamePlugin {
    registry: Arc<dyn CommandRegistryPort>,
    nickname: Arc<dyn NicknamePort>,
}

impl NicknamePlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>, nickname: Arc<dyn NicknamePort>) -> Self {
        Self { registry, nickname }
    }
}

impl PluginPort for NicknamePlugin {
    fn name(&self) -> &'static str {
        "nickname"
    }

    fn init(&self) -> Result<(), PluginError> {
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "set_guild_name".to_owned(),
                description: "Change the bot's name in this server - how members see it \
                     (omit to reset)"
                    .to_owned(),
                arguments: vec![ArgDescriptor {
                    name: "name".to_owned(),
                    description: "New name, max 32 characters; omit to reset to the real name"
                        .to_owned(),
                    required: false,
                    kind: ArgKind::String,
                    choices: None,
                }],
                // No platform gate: the native nickname permission governs
                // who may rename MEMBERS, not who may rename the BOT - the
                // tier system answers that, with ephemeral denials.
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: true,
            },
            Arc::new(SetGuildNameHandler { nickname: Arc::clone(&self.nickname) }),
        );
        Ok(())
    }
}

/// Applies the name change: one REST call, no channel lock, no storage. The
/// platform is the single source of truth for the value.
struct SetGuildNameHandler {
    nickname: Arc<dyn NicknamePort>,
}

#[async_trait]
impl CommandHandler for SetGuildNameHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        // A guild-local name cannot exist outside a guild.
        let Some(guild) = event.origin.guild_id else {
            services
                .chat_output
                .send(command_reply("This command only works inside a server."))
                .await?;
            return Ok(());
        };
        // Discord's UI cannot submit an empty string, so the reset is
        // spelled two ways: the argument omitted, or whitespace only.
        let name = args.get("name").map(str::trim).filter(|name| !name.is_empty());
        if let Some(name) = name
            && name.chars().count() > MAX_NICKNAME_CHARS
        {
            services
                .chat_output
                .send(command_reply(format!(
                    "The name is too long (max {MAX_NICKNAME_CHARS} characters)."
                )))
                .await?;
            return Ok(());
        }

        match self.nickname.set(guild, name).await {
            Ok(()) => {
                let message = name.map_or_else(
                    || "Bot name reset - the real name shows again.".to_owned(),
                    |name| format!("Bot name in this server set to `{name}`."),
                );
                services.chat_output.send(command_reply(message)).await?;
            }
            Err(err) => {
                // Details (permission denials, rate limits) stay in the
                // logs; the member sees the classification only.
                tracing::warn!(%err, guild = guild.get(), "failed to set the bot nickname");
                services
                    .chat_output
                    .send(command_reply(
                        "Could not change the name - the platform rejected the change. The bot \
                         likely lacks the Manage Nicknames permission in this server.",
                    ))
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use parking_lot::Mutex;

    use super::*;
    use crate::kernel::{
        models::{
            ChannelId, CommandPayload, EventKind, EventPayload, GuildId, MessageId, Origin,
            OutboundError, UserId,
        },
        spi_ports::ChatOutputPort,
    };
    use crate::test_support::{
        CapturingCommandRegistry, RecordingChatOutput, RecordingChatOutputFactory,
    };

    /// Records the last request; optionally fails, for the rejection path.
    struct FakeNickname {
        last: Mutex<Option<(GuildId, Option<String>)>>,
        fail: bool,
    }

    impl FakeNickname {
        fn ok() -> Arc<Self> {
            Arc::new(Self { last: Mutex::new(None), fail: false })
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self { last: Mutex::new(None), fail: true })
        }

        fn last(&self) -> Option<(GuildId, Option<String>)> {
            self.last.lock().clone()
        }
    }

    #[async_trait]
    impl NicknamePort for FakeNickname {
        async fn set(&self, guild: GuildId, name: Option<&str>) -> Result<(), OutboundError> {
            if self.fail {
                return Err(OutboundError::Send("platform said no".to_owned()));
            }
            *self.last.lock() = Some((guild, name.map(ToOwned::to_owned)));
            Ok(())
        }
    }

    fn command_event(guild: Option<u64>) -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: Origin {
                guild_id: guild.map(GuildId),
                channel_id: ChannelId(6),
                user_id: UserId(3),
                message_id: Some(MessageId(4)),
                reply_token: None,
            },
            payload: EventPayload::Command(CommandPayload {
                name: "set_guild_name".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        }
    }

    /// The handler reads its `args` parameter, not the event payload - the
    /// adapter fills one from the other at dispatch time.
    fn command_args(name: Option<&str>) -> CommandArgs {
        CommandArgs(name.map(|name| vec![("name".to_owned(), name.to_owned())]).unwrap_or_default())
    }

    fn services() -> (Arc<RecordingChatOutput>, KernelServices) {
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(&output)).boxed(),
            guild_storage: None,
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        };
        (output, services)
    }

    /// The command registers guild-only, moderator-gated, with both
    /// descriptions inside Discord's 100-character caps.
    #[test]
    fn init_registers_the_command() {
        let registry = Arc::new(CapturingCommandRegistry::default());
        let plugin = NicknamePlugin::new(
            Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
            FakeNickname::ok(),
        );
        plugin.init().expect("init expected to succeed");

        assert_eq!(registry.names(), ["set_guild_name"]);
        let descriptor = registry.descriptor("set_guild_name").expect("descriptor expected");
        crate::test_support::assert_descriptions_fit_discord(&[descriptor.clone()]);
        assert_eq!(descriptor.plugin_id, "nickname");
        assert!(descriptor.guild_only);
        assert_eq!(descriptor.required_tier, Some(AccessTier::Moderator));
        assert!(descriptor.required_permission.is_none());
        assert_eq!(descriptor.arguments.len(), 1);
        let name_arg = descriptor.arguments.first().expect("name argument expected");
        assert!(!name_arg.required, "name is optional - omitting resets");
    }

    #[tokio::test]
    async fn set_and_reset_round_trip() {
        let nickname = FakeNickname::ok();
        let (output, services) = services();
        let handler =
            SetGuildNameHandler { nickname: Arc::clone(&nickname) as Arc<dyn NicknamePort> };

        handler
            .invoke(&command_event(Some(1)), &command_args(Some("Sage")), &services)
            .await
            .expect("set expected to succeed");
        assert_eq!(nickname.last(), Some((GuildId(1), Some("Sage".to_owned()))));
        assert!(output.messages().iter().any(|m| m.contains("Sage")));

        // Whitespace-only and omitted both reset.
        handler
            .invoke(&command_event(Some(1)), &command_args(Some("   ")), &services)
            .await
            .expect("reset expected to succeed");
        assert_eq!(nickname.last(), Some((GuildId(1), None)));

        handler
            .invoke(&command_event(Some(1)), &command_args(None), &services)
            .await
            .expect("reset expected to succeed");
        assert_eq!(nickname.last(), Some((GuildId(1), None)));
        assert!(
            output.messages().iter().any(|m| m.contains("reset")),
            "reset notice expected: {:?}",
            output.messages()
        );
    }

    #[tokio::test]
    async fn oversized_name_is_refused_without_touching_the_platform() {
        let nickname = FakeNickname::ok();
        let (output, services) = services();
        let handler =
            SetGuildNameHandler { nickname: Arc::clone(&nickname) as Arc<dyn NicknamePort> };

        let long = "x".repeat(MAX_NICKNAME_CHARS + 1);
        handler
            .invoke(&command_event(Some(1)), &command_args(Some(&long)), &services)
            .await
            .expect("handler expected to succeed");

        assert_eq!(nickname.last(), None, "the platform must not be called");
        assert!(output.messages().iter().any(|m| m.contains("too long")));
    }

    #[tokio::test]
    async fn non_guild_origin_is_refused() {
        let nickname = FakeNickname::ok();
        let (output, services) = services();
        let handler =
            SetGuildNameHandler { nickname: Arc::clone(&nickname) as Arc<dyn NicknamePort> };

        handler
            .invoke(&command_event(None), &command_args(Some("Sage")), &services)
            .await
            .expect("handler expected to succeed");

        assert_eq!(nickname.last(), None);
        assert!(output.messages().iter().any(|m| m.contains("inside a server")));
    }

    #[tokio::test]
    async fn platform_rejection_answers_without_error_detail() {
        let nickname = FakeNickname::failing();
        let (output, services) = services();
        let handler =
            SetGuildNameHandler { nickname: Arc::clone(&nickname) as Arc<dyn NicknamePort> };

        handler
            .invoke(&command_event(Some(1)), &command_args(Some("Sage")), &services)
            .await
            .expect("handler expected to succeed");

        let reply = output.messages().join("\n");
        assert!(reply.contains("rejected"), "{reply}");
        assert!(!reply.contains("platform said no"), "error detail leaked: {reply}");
    }
}
