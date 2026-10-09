use std::sync::Arc;

use async_trait::async_trait;
use futures_util::FutureExt;

use crate::common::{command_reply, panic_message};
use crate::kernel::{
    models::{Embed, EventPayload, OutboundMessage, RequestContext},
    plugin_ports::{
        AccessTier, CommandArgs, CommandDescriptor, CommandHandler, CommandRegistryPort,
        MiddlewarePluginPort, Next, PluginPort,
    },
    services::KernelServices,
};

/// Command dispatcher: routes native [`EventKind::CommandInvoked`] events to
/// handlers registered in the `CommandRegistryPort`. Meaning lives in the
/// owning plugins; this plugin only dispatches. An unknown command yields no
/// output - there is no "not found" default. A handler failure (an `Err` or
/// a panic - both are caught here, the pipeline never sees either) on a
/// transactional invocation sends a generic ephemeral failure notice; plain
/// events stay silent.
pub struct CommandPlugin {
    registry: Arc<dyn CommandRegistryPort>,
}

impl CommandPlugin {
    #[must_use]
    pub fn new(registry: Arc<dyn CommandRegistryPort>) -> Self {
        Self { registry }
    }
}

impl PluginPort for CommandPlugin {
    fn name(&self) -> &'static str {
        "command"
    }

    fn init(&self) -> Result<(), crate::kernel::models::PluginError> {
        // Demo command proving the full loop (register -> Discord sync ->
        // interaction -> dispatch -> respond). Real commands belong to the
        // feature plugins that own their meaning.
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "ping".to_owned(),
                description: "Check that the bot is alive - it replies with Pong".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::User),
                guild_only: false,
            },
            Arc::new(PingHandler),
        );
        // The guide renders the live registry - the dispatcher's own view,
        // so it always reflects what is actually registered.
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "help".to_owned(),
                description: "List every command with usage and who may run it".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::User),
                guild_only: false,
            },
            Arc::new(HelpHandler { registry: Arc::clone(&self.registry) }),
        );
        Ok(())
    }
}

#[async_trait]
impl MiddlewarePluginPort for CommandPlugin {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        let EventPayload::Command(command) = &event.payload else {
            return Next::Continue;
        };

        let Some(handler) = self.registry.lookup(&command.name) else {
            tracing::debug!(command = %command.name, "no handler registered - ignoring command");
            // A deferred interaction must never hang on "thinking": a stale
            // client can still invoke a command that was renamed/removed
            // between syncs (or a guild-only command can slip through in a
            // DM), so transactional origins get an ephemeral not-found
            // notice. Plain origins stay silent (the no-not-found default).
            if event.origin.reply_token.is_some() {
                let notice = OutboundMessage::embed(Embed {
                    title: "Unknown command".to_owned(),
                    description: format!(
                        "`/{}` is not registered - the command list may be out of date; \
                         reopen the command menu.",
                        command.name
                    ),
                })
                .ephemeral();
                if let Err(notice_err) = services.chat_output.send(notice).await {
                    tracing::warn!(%notice_err, "failed to deliver unknown-command notice");
                }
            }
            return Next::Continue;
        };

        let args = CommandArgs(command.args.clone());
        // Panic-isolated like every plugin hook: a broken handler must not
        // unwind into the pipeline, and a deferred interaction must not hang
        // on "thinking" because the failure never became an `Err`.
        let failure = match std::panic::AssertUnwindSafe(handler.invoke(event, &args, services))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(err.to_string()),
            Err(panic) => Some(format!("handler panicked: {}", panic_message(&panic))),
        };
        if let Some(detail) = failure {
            tracing::error!(command = %command.name, detail = %detail, "command handler failed");
            // Transactional invocations (slash commands) owe the platform an
            // answer: a generic ephemeral notice keeps the interaction from
            // hanging; details stay in the log, not in the channel. Plain
            // events stay silent, like denied plain messages.
            if event.origin.reply_token.is_some() {
                let notice = OutboundMessage::embed(Embed {
                    title: "⚠️ Command failed".to_owned(),
                    description: "The command could not be executed. Try again later.".to_owned(),
                })
                .ephemeral();
                if let Err(notice_err) = services.chat_output.send(notice).await {
                    tracing::warn!(%notice_err, "failed to deliver command failure notice");
                }
            }
        } else {
            // Audit trail: who ran what. User/guild/channel ride in the
            // kernel span; argument values render only while short (at
            // most 64 chars: settings keys, ids, and short admin-set free
            // text such as a prompt prefix - bounded operator telemetry,
            // never message-sized payloads); longer values appear as a
            // shape (their length) only.
            let summary = args
                .0
                .iter()
                .map(|(name, value)| {
                    let count = value.chars().count();
                    if count <= 64 {
                        format!("{name}={value:?}")
                    } else {
                        format!("{name}=<{count} chars>")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            tracing::info!(command = %command.name, summary = %summary, "command executed");
        }

        // Deliberate handling: like auth, a resolved command stops the chain.
        Next::Stop
    }
}

struct PingHandler;

#[async_trait]
impl CommandHandler for PingHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        services.chat_output.send(command_reply("Pong!")).await?;
        Ok(())
    }
}

/// The `/help` guide: renders the live command registry as an ephemeral
/// embed, grouped by access tier, each command as a usage line. Pages are
/// packed under the platform's embed budget - the guide degrades (splits),
/// it never fails.
struct HelpHandler {
    registry: Arc<dyn CommandRegistryPort>,
}

#[async_trait]
impl CommandHandler for HelpHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let limit = services.platform_info.embed_limit().unwrap_or(DEFAULT_EMBED_LIMIT);
        let pages = help_pages(self.registry.descriptors(), limit);
        let total = pages.len();
        for (index, description) in pages.into_iter().enumerate() {
            let title = if total > 1 {
                format!("🧭 Command guide ({}/{total})", index + 1)
            } else {
                "🧭 Command guide".to_owned()
            };
            services
                .chat_output
                .send(OutboundMessage::embed(Embed { title, description }).ephemeral())
                .await?;
        }
        Ok(())
    }
}

/// Embed budget where the platform declares none (Discord's own cap).
const DEFAULT_EMBED_LIMIT: usize = 4096;

/// UTF-16 length - the unit the strictest platforms count limits in, so
/// budgeting in it stays safe under both a chars and a units cap.
fn utf16_len(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

/// The guide's sections in privilege order, one per tier that has commands.
/// `Guest` and undeclared tiers render as `Everyone`; `Banned` commands are
/// listed nowhere - they run for nobody. Names sort within sections.
fn help_sections(descriptors: &[CommandDescriptor]) -> Vec<(&'static str, Vec<String>)> {
    const ORDER: [(&str, Option<AccessTier>); 5] = [
        ("Everyone", None),
        ("User", Some(AccessTier::User)),
        ("Moderator", Some(AccessTier::Moderator)),
        ("Admin", Some(AccessTier::Admin)),
        ("Bot owner", Some(AccessTier::Owner)),
    ];
    let mut sorted: Vec<&CommandDescriptor> = descriptors.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    ORDER
        .iter()
        .filter_map(|(heading, tier)| {
            let lines: Vec<String> = sorted
                .iter()
                .filter(|descriptor| match tier {
                    None => matches!(descriptor.required_tier, None | Some(AccessTier::Guest)),
                    Some(expected) => descriptor.required_tier == Some(*expected),
                })
                .map(|descriptor| help_line(descriptor))
                .collect();
            (!lines.is_empty()).then_some((*heading, lines))
        })
        .collect()
}

/// One guide line: usage with required args as `<name>`, optional ones as
/// `[name]`, then the command's own mini-doc description.
fn help_line(descriptor: &CommandDescriptor) -> String {
    let usage: String = descriptor
        .arguments
        .iter()
        .map(
            |arg| {
                if arg.required { format!(" <{}>", arg.name) } else { format!(" [{}]", arg.name) }
            },
        )
        .collect();
    format!("`/{}{usage}` - {}", descriptor.name, descriptor.description)
}

fn render_section(heading: &str, lines: &[String]) -> String {
    format!("**{heading}**\n{}", lines.join("\n"))
}

/// Packs the guide into embed-sized pages: sections stay whole while they
/// fit; a section larger than the budget spills per line; a single line
/// larger than the budget is hard-truncated. Every command is delivered (or
/// visibly degraded) - the guide never drops one.
fn help_pages(descriptors: Vec<CommandDescriptor>, limit: usize) -> Vec<String> {
    let limit = limit.max(1);
    let mut pages: Vec<String> = Vec::new();
    let mut current = String::new();
    for (heading, lines) in help_sections(&descriptors) {
        let section = render_section(&heading, &lines);
        if !current.is_empty() {
            let candidate = format!("{current}\n\n{section}");
            if utf16_len(&candidate) <= limit {
                current = candidate;
                continue;
            }
            pages.push(std::mem::take(&mut current));
        }
        current = section;
        // The section sits alone on the page; if it alone exceeds the
        // budget, spill whole lines onto continuation pages.
        while utf16_len(&current) > limit {
            let (head, rest) = spill_over_limit(&current, limit);
            if head.is_empty() {
                // One character larger than the whole budget: emit it
                // alone - nothing smaller exists to cut to.
                let alone: String = current.chars().take(1).collect();
                current = current.chars().skip(1).collect();
                pages.push(alone);
            } else {
                pages.push(head);
                current = rest;
            }
        }
    }
    if !current.is_empty() {
        pages.push(current);
    }
    pages
}

/// Splits `text` at the last line break inside the budget, counting UTF-16
/// units; when no break fits, cuts at the largest fitting char boundary.
/// The head may be empty when a single character exceeds the whole budget -
/// the caller emits it anyway, so no character is ever lost.
fn spill_over_limit(text: &str, limit: usize) -> (String, String) {
    let mut units = 0usize;
    let mut chars = 0usize;
    let mut line_break: Option<usize> = None;
    for ch in text.chars() {
        if units + ch.len_utf16() > limit {
            let head_chars = line_break.unwrap_or(chars);
            let tail: String = text.chars().skip(head_chars).collect();
            return (text.chars().take(head_chars).collect(), tail);
        }
        units += ch.len_utf16();
        chars += 1;
        if ch == '\n' {
            line_break = Some(chars);
        }
    }
    (text.to_owned(), String::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryCommandRegistry;
    use crate::kernel::{
        models::{ChannelId, EventKind, GuildId, MessageId, Origin, UserId},
        plugin_ports::{ArgDescriptor, ArgKind},
        spi_ports::{PlatformInfoPort, StoragePort},
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
    use std::sync::Arc;

    /// The walking-skeleton commands carry the same description discipline
    /// as every other plugin: self-sufficient docs within Discord's
    /// 100-char cap.
    #[test]
    fn init_registers_ping_and_help_within_discord_limits() {
        let registry = Arc::new(InMemoryCommandRegistry::new());
        let plugin = CommandPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>);
        plugin.init().expect("init expected to succeed");

        let descriptors = registry.descriptors();
        crate::test_support::assert_descriptions_fit_discord(&descriptors);
        let ping = registry.descriptor("ping").expect("ping descriptor expected");
        assert_eq!(ping.required_tier, Some(AccessTier::User));
        let help = registry.descriptor("help").expect("help descriptor expected");
        assert_eq!(help.required_tier, Some(AccessTier::User));
    }

    struct StaticHandler {
        reply: &'static str,
    }

    #[async_trait]
    impl CommandHandler for StaticHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            _args: &CommandArgs,
            services: &KernelServices,
        ) -> anyhow::Result<()> {
            services.chat_output.send(OutboundMessage::text(self.reply)).await?;
            Ok(())
        }
    }

    fn origin() -> Origin {
        Origin {
            guild_id: Some(GuildId(1)),
            channel_id: ChannelId(2),
            user_id: UserId(3),
            message_id: Some(MessageId(4)),
            reply_token: None,
        }
    }

    fn command_event(name: &str, args: &[(&str, &str)]) -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: origin(),
            payload: EventPayload::Command(crate::kernel::models::CommandPayload {
                name: name.to_owned(),
                args: args.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        }
    }

    fn registry_with(command: &str, reply: &'static str) -> Arc<InMemoryCommandRegistry> {
        let registry = InMemoryCommandRegistry::new();
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: command.to_owned(),
                description: "test".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: None,
                guild_only: false,
            },
            Arc::new(StaticHandler { reply }),
        );
        Arc::new(registry)
    }

    /// Declares one test command with the given tier and arguments; the
    /// handler is never invoked.
    fn declare(
        registry: &InMemoryCommandRegistry,
        name: &str,
        tier: Option<AccessTier>,
        arguments: Vec<ArgDescriptor>,
    ) {
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: name.to_owned(),
                description: format!("{name} description"),
                arguments,
                required_permission: None,
                required_tier: tier,
                guild_only: false,
            },
            Arc::new(StaticHandler { reply: "unused" }),
        );
    }

    fn arg(name: &str, required: bool) -> ArgDescriptor {
        ArgDescriptor {
            name: name.to_owned(),
            description: "arg".to_owned(),
            required,
            kind: ArgKind::String,
            choices: None,
        }
    }

    fn test_services(output: &Arc<RecordingChatOutput>) -> KernelServices {
        let storage = InMemoryStorage::new();
        KernelServices {
            chat_output: Arc::clone(output) as Arc<dyn crate::kernel::spi_ports::ChatOutputPort>,
            chat_output_factory: RecordingChatOutputFactory::new(Arc::clone(output)).boxed(),
            guild_storage: Some(storage.guild_scoped("test", GuildId(1))),
            plugin_storage: crate::test_support::test_plugin_storage(),
            platform_info: crate::test_support::test_platform_info(),
        }
    }

    #[tokio::test]
    async fn registered_command_is_dispatched_with_args() {
        let registry = registry_with("greet", "hello!");
        let plugin = CommandPlugin::new(registry);
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("greet", &[("target", "world")]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        assert_eq!(output.messages(), ["hello!"]);
    }

    /// An unknown command on a transactional origin must not hang the
    /// deferred interaction on "thinking": an ephemeral not-found notice
    /// goes out (stale clients can still invoke renamed/removed commands).
    /// Plain origins stay silent - the no-not-found default.
    #[tokio::test]
    async fn unknown_command_answers_the_interaction_ephemerally() {
        let plugin = CommandPlugin::new(registry_with("greet", "hello!"));
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("vanished", &[]);
        if let crate::kernel::models::EventPayload::Command(payload) = &mut event.payload {
            event.origin.reply_token = Some("interaction-token".to_owned());
            payload.name = "vanished".to_owned();
        }

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Continue));
        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        assert!(sent.first().is_some_and(|message| message.ephemeral));
        assert!(output.messages().first().is_some_and(|text| text.contains("vanished")));
    }

    #[tokio::test]
    async fn unknown_plain_command_stays_silent() {
        let plugin = CommandPlugin::new(registry_with("greet", "hello!"));
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("vanished", &[]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn handler_receives_arguments_by_name() {
        let registry = InMemoryCommandRegistry::new();
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: "echo".to_owned(),
                description: "test".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: None,
                guild_only: false,
            },
            Arc::new(EchoTextHandler),
        );
        let plugin = CommandPlugin::new(Arc::new(registry));
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("echo", &[("text", "privit")]);

        let _ = plugin.pre(&mut event, &services).await;

        assert_eq!(output.messages(), ["privit"]);
    }

    #[tokio::test]
    async fn unknown_command_continues_without_output() {
        let plugin = CommandPlugin::new(Arc::new(InMemoryCommandRegistry::new()));
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("unknown", &[]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Continue));
        assert!(output.messages().is_empty());
    }

    #[tokio::test]
    async fn non_command_events_are_ignored() {
        let registry = registry_with("greet", "hello!");
        let plugin = CommandPlugin::new(registry);
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = RequestContext::message_received(origin(), "hello");

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Continue));
        assert!(output.messages().is_empty());
    }

    struct EchoTextHandler;

    #[async_trait]
    impl CommandHandler for EchoTextHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            args: &CommandArgs,
            services: &KernelServices,
        ) -> anyhow::Result<()> {
            let text = args.get("text").unwrap_or("nothing");
            services.chat_output.send(OutboundMessage::text(text)).await?;
            Ok(())
        }
    }

    struct FailingHandler;

    #[async_trait]
    impl CommandHandler for FailingHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            _args: &CommandArgs,
            _services: &KernelServices,
        ) -> anyhow::Result<()> {
            Err(anyhow::anyhow!("boom"))
        }
    }

    struct PanickingHandler;

    #[async_trait]
    impl CommandHandler for PanickingHandler {
        async fn invoke(
            &self,
            _event: &RequestContext,
            _args: &CommandArgs,
            _services: &KernelServices,
        ) -> anyhow::Result<()> {
            panic!("handler boom");
        }
    }

    fn registry_with_handler(
        command: &str,
        handler: Arc<dyn CommandHandler>,
    ) -> Arc<InMemoryCommandRegistry> {
        let registry = InMemoryCommandRegistry::new();
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: command.to_owned(),
                description: "test".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: None,
                guild_only: false,
            },
            handler,
        );
        Arc::new(registry)
    }

    fn failing_registry() -> Arc<InMemoryCommandRegistry> {
        let registry = InMemoryCommandRegistry::new();
        registry.register(
            CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: "fail".to_owned(),
                description: "test".to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: None,
                guild_only: false,
            },
            Arc::new(FailingHandler),
        );
        Arc::new(registry)
    }

    /// A handler failure on a transactional invocation answers the invoker
    /// with exactly one generic ephemeral notice; details stay in the log.
    #[tokio::test]
    async fn failing_handler_answers_ephemerally_with_reply_token() {
        let plugin = CommandPlugin::new(failing_registry());
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("fail", &[]);
        event.origin.reply_token = Some("token".to_owned());

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        let sent = output.sent();
        assert_eq!(sent.len(), 1, "exactly one failure notice expected");
        let notice = sent.first().expect("notice expected");
        assert!(notice.ephemeral, "failure notice must be visible to the invoker only");
        let text = output.messages().into_iter().next().expect("notice expected");
        assert!(text.contains("⚠️ Command failed"));
        assert!(text.contains("Try again later."));
    }

    /// Plain events have no transactional answer: a handler failure stays
    /// silent (a public rejection would be a spam vector).
    #[tokio::test]
    async fn failing_handler_without_reply_token_stays_silent() {
        let plugin = CommandPlugin::new(failing_registry());
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("fail", &[]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// A PANICKING handler is failure like any other: the dispatcher catches
    /// it, the transactional invocation gets the same ephemeral answer, and
    /// no panic unwinds into the pipeline.
    #[tokio::test]
    async fn panicking_handler_answers_ephemerally_with_reply_token() {
        let registry = registry_with_handler("boom", Arc::new(PanickingHandler));
        let plugin = CommandPlugin::new(registry);
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("boom", &[]);
        event.origin.reply_token = Some("token".to_owned());

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        let sent = output.sent();
        assert_eq!(sent.len(), 1, "exactly one failure notice expected");
        assert!(sent.first().expect("notice expected").ephemeral);
        assert!(
            output.messages().into_iter().next().is_some_and(|t| t.contains("⚠️ Command failed"))
        );
    }

    #[tokio::test]
    async fn panicking_handler_without_reply_token_stays_silent() {
        let registry = registry_with_handler("boom", Arc::new(PanickingHandler));
        let plugin = CommandPlugin::new(registry);
        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let mut event = command_event("boom", &[]);

        let next = plugin.pre(&mut event, &services).await;

        assert!(matches!(next, Next::Stop));
        assert!(output.messages().is_empty());
    }

    /// `/help` renders the live registry: every command with usage, grouped
    /// by tier in privilege order, delivered as an ephemeral embed. Banned
    /// commands are listed nowhere.
    #[tokio::test]
    async fn help_lists_every_command_grouped_by_tier() {
        let registry = InMemoryCommandRegistry::new();
        declare(&registry, "zeta", None, Vec::new());
        declare(&registry, "banned_thing", Some(AccessTier::Banned), Vec::new());
        declare(
            &registry,
            "mod_thing",
            Some(AccessTier::Moderator),
            vec![arg("channel", true), arg("quiet", false)],
        );
        declare(&registry, "admin_thing", Some(AccessTier::Admin), Vec::new());
        declare(&registry, "owner_thing", Some(AccessTier::Owner), Vec::new());
        let registry = Arc::new(registry);
        let plugin = CommandPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>);
        plugin.init().expect("init expected to succeed");

        let output = RecordingChatOutput::new();
        let services = test_services(&output);
        let handler = registry.lookup("help").expect("help handler expected");
        handler
            .invoke(&command_event("help", &[]), &CommandArgs(Vec::new()), &services)
            .await
            .expect("invoke expected to succeed");

        let sent = output.sent();
        assert_eq!(sent.len(), 1, "one embed expected under the default budget");
        assert!(sent.first().expect("embed expected").ephemeral);
        let rendered = output.messages().into_iter().next().expect("guide expected");

        assert!(rendered.contains("**Everyone**"), "{rendered}");
        assert!(rendered.contains("`/zeta` - zeta description"), "{rendered}");
        assert!(rendered.contains("**User**"), "{rendered}");
        assert!(rendered.contains("`/help` - "), "{rendered}");
        assert!(rendered.contains("`/ping` - "), "{rendered}");
        assert!(rendered.contains("**Moderator**"), "{rendered}");
        assert!(
            rendered.contains("`/mod_thing <channel> [quiet]` - mod_thing description"),
            "{rendered}"
        );
        assert!(rendered.contains("**Admin**"), "{rendered}");
        assert!(rendered.contains("**Bot owner**"), "{rendered}");
        assert!(!rendered.contains("banned_thing"), "banned commands are listed nowhere");

        // Sections appear in privilege order.
        let position = |marker: &str| rendered.find(marker).expect("section expected");
        assert!(position("**Everyone**") < position("**User**"));
        assert!(position("**User**") < position("**Moderator**"));
        assert!(position("**Moderator**") < position("**Admin**"));
        assert!(position("**Admin**") < position("**Bot owner**"));
    }

    /// Pages respect the embed budget and no command is dropped, whatever
    /// the registry size.
    #[test]
    fn help_pages_fit_the_budget_and_keep_every_command() {
        let mut descriptors = Vec::new();
        for index in 0..40 {
            descriptors.push(CommandDescriptor {
                plugin_id: "test".to_owned(),
                name: format!("command_{index:02}"),
                description: format!("description {index}"),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::Moderator),
                guild_only: false,
            });
        }

        let pages = help_pages(descriptors, 200);

        assert!(pages.len() > 1, "expected multiple pages");
        let mut joined = String::new();
        for page in &pages {
            assert!(utf16_len(page) <= 200, "page over budget: {page}");
            joined.push_str(page);
            joined.push('\n');
        }
        for index in 0..40 {
            assert!(joined.contains(&format!("command_{index:02}")), "command dropped");
        }
    }

    /// The handler packs pages by the platform's own embed budget, not a
    /// built-in constant.
    struct TinyEmbedPlatform;

    impl PlatformInfoPort for TinyEmbedPlatform {
        fn slug(&self) -> &'static str {
            "test"
        }

        fn display_name(&self) -> &'static str {
            "Test"
        }

        fn message_limit(&self) -> Option<usize> {
            Some(2000)
        }

        fn embed_limit(&self) -> Option<usize> {
            Some(200)
        }
    }

    #[tokio::test]
    async fn help_splits_pages_by_the_platform_embed_budget() {
        let registry = InMemoryCommandRegistry::new();
        for index in 0..30 {
            declare(
                &registry,
                &format!("command_{index:02}"),
                Some(AccessTier::Moderator),
                Vec::new(),
            );
        }
        let registry = Arc::new(registry);
        let plugin = CommandPlugin::new(Arc::clone(&registry) as Arc<dyn CommandRegistryPort>);
        plugin.init().expect("init expected to succeed");

        let output = RecordingChatOutput::new();
        let mut services = test_services(&output);
        services.platform_info = Arc::new(TinyEmbedPlatform);
        let handler = registry.lookup("help").expect("help handler expected");
        handler
            .invoke(&command_event("help", &[]), &CommandArgs(Vec::new()), &services)
            .await
            .expect("invoke expected to succeed");

        assert!(output.sent().len() > 1, "multiple pages expected");
        for message in output.messages() {
            // Title-plus-description projection stays comfortably within the
            // budget even with the title counted.
            assert!(message.chars().count() <= 250, "page over budget: {message}");
        }
    }
}
