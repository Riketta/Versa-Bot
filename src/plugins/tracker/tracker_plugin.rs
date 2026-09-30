use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::kernel::{
    models::{
        ChannelId, Event, EventKind, EventPayload, OutboundMessage, PluginError, RequestContext,
    },
    plugin_ports::{
        CommandArgs, CommandDescriptor, CommandHandler, CommandRegistryPort, EventBusPort,
        MiddlewarePluginPort, Next, Permission, PluginPort,
    },
    services::KernelServices,
};

use super::events::{UserJoinedGuild, UserLeftGuild};

/// Guild storage namespace owned by this plugin (the plugin's slug).
const NAMESPACE: &str = "tracker";
/// Guild storage key holding the serialized [`TrackerConfig`].
const CONFIG_KEY: &str = "config";

/// Per-guild tracker settings, stored as a JSON document in the plugin's
/// guild storage namespace. Platform identifiers are stored as strings -
/// snowflakes exceed JSON number precision.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrackerConfig {
    /// Channel receiving join/leave audit messages. Absent = tracking is
    /// off for the guild.
    #[serde(default)]
    pub audit_channel_id: Option<String>,
}

/// User activity tracker: observes member lifecycle events in the pipeline
/// and turns each into two things:
///
/// - an audit message in the guild's configured channel (best effort - a
///   failed or unconfigured destination never blocks the pipeline), and
/// - a plugin-owned domain event on the bus ([`UserJoinedGuild`] /
///   [`UserLeftGuild`]), so bus-only plugins can react without joining the
///   pipeline themselves.
///
/// Passive middleware: always `Continue` - tracking observes, never gates.
/// Policy contrasts with auth: a malformed config here fails *open* (skip
/// the log line) because observability must not break the event flow.
///
/// The audit channel is assigned per guild via `/assign_tracker` (run in the
/// channel to assign) and cleared via `/unassign_tracker`; both are declared
/// by this plugin in `init()` - meaning lives here, not in the dispatcher.
pub struct UserActivityTrackerPlugin<B: EventBusPort> {
    bus: B,
    registry: Arc<dyn CommandRegistryPort>,
}

impl<B: EventBusPort> UserActivityTrackerPlugin<B> {
    #[must_use]
    pub fn new(bus: B, registry: Arc<dyn CommandRegistryPort>) -> Self {
        Self { bus, registry }
    }
}

impl<B: EventBusPort> PluginPort for UserActivityTrackerPlugin<B> {
    fn name(&self) -> &'static str {
        "tracker"
    }

    fn init(&self) -> Result<(), PluginError> {
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "assign_tracker".to_owned(),
                aliases: None,
                description: "Log member joins/leaves in this channel".to_owned(),
                arguments: Vec::new(),
                // Platform-interpreted: the Discord adapter publishes this as
                // `default_member_permissions` (Manage Server).
                required_permission: Some(Permission { name: "manage_guild".to_owned() }),
            },
            Arc::new(AssignTrackerHandler),
        );
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "unassign_tracker".to_owned(),
                aliases: None,
                description: "Stop logging member joins/leaves in this guild".to_owned(),
                arguments: Vec::new(),
                required_permission: Some(Permission { name: "manage_guild".to_owned() }),
            },
            Arc::new(UnassignTrackerHandler),
        );
        Ok(())
    }
}

#[async_trait]
impl<B: EventBusPort> MiddlewarePluginPort for UserActivityTrackerPlugin<B> {
    async fn pre(&self, event: &mut RequestContext, services: &KernelServices) -> Next {
        let (joined, username) = match (&event.kind, &event.payload) {
            (EventKind::MemberJoined, EventPayload::Member(member)) => {
                (true, member.username.clone())
            }
            (EventKind::MemberLeft, EventPayload::Member(member)) => {
                (false, member.username.clone())
            }
            _ => return Next::Continue,
        };

        // Direct messages: nothing to track.
        let Some(guild_id) = event.origin.guild_id else {
            return Next::Continue;
        };
        let Some(storage) = &services.guild_storage else {
            return Next::Continue;
        };

        // Unconfigured = tracking off for this guild.
        let Some(raw) = storage.get(NAMESPACE, CONFIG_KEY).await.ok().flatten() else {
            return Next::Continue;
        };
        let Ok(config) = serde_json::from_value::<TrackerConfig>(raw) else {
            tracing::warn!(namespace = NAMESPACE, "tracker config is malformed - skipping");
            return Next::Continue;
        };
        let channel_id = match config.audit_channel_id.as_deref().map(str::parse::<u64>) {
            Some(Ok(channel_id)) => channel_id,
            Some(Err(_)) | None => {
                tracing::warn!(namespace = NAMESPACE, "tracker audit channel missing or invalid");
                return Next::Continue;
            }
        };

        let display =
            username.clone().unwrap_or_else(|| format!("user {}", event.origin.user_id.get()));
        let announcement = if joined {
            format!("📥 **{display}** joined the guild")
        } else {
            format!("📤 **{display}** left the guild")
        };

        // Best effort: deliver to the configured audit channel; a broken
        // destination is logged, never propagated - the pipeline keeps going.
        let output =
            services.chat_output_factory.channel_output(&event.origin, ChannelId(channel_id));
        if let Err(err) = output.send(OutboundMessage::text(announcement)).await {
            tracing::warn!(%err, "failed to deliver member audit message");
        }

        // The domain fact is published regardless of audit delivery above:
        // bus subscribers react to what happened, not to whether the log wrote.
        let derived: Arc<dyn Event> = if joined {
            Arc::new(UserJoinedGuild {
                platform: event.origin.platform,
                guild_id,
                user_id: event.origin.user_id,
                username,
            })
        } else {
            Arc::new(UserLeftGuild {
                platform: event.origin.platform,
                guild_id,
                user_id: event.origin.user_id,
                username,
            })
        };
        self.bus.publish(derived);

        Next::Continue
    }
}

/// `/assign_tracker`: the channel the command is run in becomes the guild's
/// audit channel (last write wins - one channel per guild). Requires a guild
/// origin; the reply lands back in that channel via the event-scoped port.
struct AssignTrackerHandler;

#[async_trait]
impl CommandHandler for AssignTrackerHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = &services.guild_storage else {
            services
                .chat_output
                .send(OutboundMessage::text("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        storage
            .set(
                NAMESPACE,
                CONFIG_KEY,
                serde_json::json!({
                    "audit_channel_id": event.origin.channel_id.get().to_string()
                }),
            )
            .await?;

        services
            .chat_output
            .send(OutboundMessage::text(
                "Tracker assigned: member joins and leaves will be logged in this channel.",
            ))
            .await?;
        Ok(())
    }
}

/// `/unassign_tracker`: clears the guild's tracker config (idempotent).
struct UnassignTrackerHandler;

#[async_trait]
impl CommandHandler for UnassignTrackerHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = &services.guild_storage else {
            services
                .chat_output
                .send(OutboundMessage::text("This command only works inside a server."))
                .await?;
            return Ok(());
        };

        storage.delete(NAMESPACE, CONFIG_KEY).await?;

        services
            .chat_output
            .send(OutboundMessage::text(
                "Tracker unassigned: member joins and leaves are no longer logged.",
            ))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{
        models::{
            ChannelId as ChannelIdModel, CommandPayload, GuildId, MemberPayload, MessageId, Origin,
            Platform, UserId,
        },
        plugin_ports::EventHandler,
        spi_ports::{ChatOutputFactoryPort, ChatOutputPort, GUILD_SETTINGS, StoragePort},
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput};
    use crate::{
        infrastructure::plugin_adapters::{InMemoryCommandRegistry, InMemoryEventBus},
        kernel::models::OutboundError,
    };
    use parking_lot::Mutex;
    use std::sync::Arc;

    // --- recording factory that remembers the destination channel ---

    #[derive(Default)]
    struct Sent {
        messages: Mutex<Vec<(u64, String)>>,
    }

    struct ChannelRecorder {
        sent: Arc<Sent>,
        channel: u64,
    }

    #[async_trait]
    impl ChatOutputPort for ChannelRecorder {
        async fn send(&self, message: OutboundMessage) -> Result<(), OutboundError> {
            self.sent.messages.lock().push((self.channel, message.content));
            Ok(())
        }
    }

    struct ChannelRecordingFactory {
        sent: Arc<Sent>,
    }

    impl ChatOutputFactoryPort for ChannelRecordingFactory {
        fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort> {
            self.channel_output(origin, origin.channel_id)
        }

        fn channel_output(
            &self,
            _origin: &Origin,
            channel_id: ChannelIdModel,
        ) -> Arc<dyn ChatOutputPort> {
            Arc::new(ChannelRecorder { sent: Arc::clone(&self.sent), channel: channel_id.get() })
        }
    }

    // --- bus recorder ---

    struct Recorder<E: Event + Clone + Send + Sync> {
        events: Mutex<Vec<E>>,
    }

    impl<E: Event + Clone + Send + Sync> EventHandler<E> for Recorder<E> {
        fn handle(&self, event: &E) {
            self.events.lock().push(event.clone());
        }
    }

    // --- fixtures ---

    fn origin(guild: Option<u64>) -> Origin {
        Origin {
            platform: Platform::Discord,
            guild_id: guild.map(GuildId),
            channel_id: ChannelIdModel(2),
            user_id: UserId(3),
            message_id: Some(MessageId(4)),
            reply_token: None,
        }
    }

    fn member_event(kind: EventKind, guild: Option<u64>, username: &str) -> RequestContext {
        RequestContext {
            kind,
            origin: origin(guild),
            payload: EventPayload::Member(MemberPayload { username: Some(username.to_owned()) }),
        }
    }

    fn configured_storage(channel: &str) -> Arc<InMemoryStorage> {
        let storage = InMemoryStorage::new();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "audit_channel_id": channel }),
        );
        Arc::new(storage)
    }

    struct Fixture {
        sent: Arc<Sent>,
        output: Arc<RecordingChatOutput>,
        joined: Arc<Recorder<UserJoinedGuild>>,
        left: Arc<Recorder<UserLeftGuild>>,
    }

    fn fixture(
        storage: Option<Arc<InMemoryStorage>>,
    ) -> (UserActivityTrackerPlugin<InMemoryEventBus>, KernelServices, Fixture) {
        let bus = InMemoryEventBus::new();
        let joined = Arc::new(Recorder::<UserJoinedGuild> { events: Mutex::new(Vec::new()) });
        let left = Arc::new(Recorder::<UserLeftGuild> { events: Mutex::new(Vec::new()) });
        bus.subscribe(Arc::clone(&joined) as Arc<dyn EventHandler<UserJoinedGuild>>);
        bus.subscribe(Arc::clone(&left) as Arc<dyn EventHandler<UserLeftGuild>>);

        let sent = Arc::new(Sent::default());
        let output = RecordingChatOutput::new();
        let services = KernelServices {
            chat_output: Arc::clone(&output) as Arc<dyn ChatOutputPort>,
            chat_output_factory: Arc::new(ChannelRecordingFactory { sent: Arc::clone(&sent) }),
            guild_storage: storage
                .map(|storage| storage.guild_scoped(Platform::Discord, GuildId(1))),
        };

        let plugin = UserActivityTrackerPlugin::new(bus, Arc::new(InMemoryCommandRegistry::new()));
        (plugin, services, Fixture { sent, output, joined, left })
    }

    // --- tests ---

    #[tokio::test]
    async fn join_is_logged_to_configured_channel_and_published() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "📥 **someone** joined the guild".to_owned())]
        );
        let published = fixture.joined.events.lock().clone();
        let first = published.first().expect("join event expected to be published");
        assert_eq!(first.guild_id, GuildId(1));
        assert_eq!(first.user_id, UserId(3));
        assert_eq!(first.username.as_deref(), Some("someone"));
        assert_eq!(published.len(), 1);
        assert!(fixture.left.events.lock().is_empty());
    }

    #[tokio::test]
    async fn leave_is_logged_and_published() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = member_event(EventKind::MemberLeft, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "📤 **someone** left the guild".to_owned())]
        );
        assert_eq!(fixture.left.events.lock().len(), 1);
        assert!(fixture.joined.events.lock().is_empty());
    }

    #[tokio::test]
    async fn unconfigured_guild_is_silent() {
        let (plugin, services, fixture) = fixture(Some(Arc::new(InMemoryStorage::new())));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));

        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.joined.events.lock().is_empty());
    }

    #[tokio::test]
    async fn malformed_config_fails_open() {
        let storage = InMemoryStorage::new();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!("not an object"),
        );
        let (plugin, services, fixture) = fixture(Some(Arc::new(storage)));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");

        // Unlike auth (fail closed), tracking just skips its log line.
        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.joined.events.lock().is_empty());
    }

    #[tokio::test]
    async fn direct_messages_are_ignored() {
        let (plugin, services, fixture) = fixture(None);
        let mut event = member_event(EventKind::MemberJoined, None, "someone");

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.joined.events.lock().is_empty());
    }

    #[tokio::test]
    async fn non_member_events_are_ignored() {
        let (plugin, services, fixture) = fixture(Some(configured_storage("777")));
        let mut event = RequestContext {
            kind: EventKind::CommandInvoked,
            origin: origin(Some(1)),
            payload: EventPayload::Command(CommandPayload {
                name: "ping".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
            }),
        };

        assert!(matches!(plugin.pre(&mut event, &services).await, Next::Continue));
        assert!(fixture.sent.messages.lock().is_empty());
        assert!(fixture.joined.events.lock().is_empty());
    }

    #[tokio::test]
    async fn missing_username_falls_back_to_user_id() {
        let storage = InMemoryStorage::new();
        storage.seed(
            Platform::Discord,
            GuildId(1),
            NAMESPACE,
            CONFIG_KEY,
            serde_json::json!({ "audit_channel_id": "777" }),
        );
        let (plugin, services, fixture) = fixture(Some(Arc::new(storage)));
        let mut event = RequestContext {
            kind: EventKind::MemberJoined,
            origin: origin(Some(1)),
            payload: EventPayload::Member(MemberPayload { username: None }),
        };

        let _ = plugin.pre(&mut event, &services).await;

        assert_eq!(
            fixture.sent.messages.lock().clone(),
            vec![(777, "📥 **user 3** joined the guild".to_owned())]
        );
    }

    /// `/assign_tracker` run in a channel stores that channel and confirms.
    #[tokio::test]
    async fn assign_handler_stores_current_channel_and_replies() {
        let storage = Arc::new(InMemoryStorage::new());
        let (_plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");
        event.origin.channel_id = ChannelIdModel(777);

        AssignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("assign expected to succeed");

        let stored = storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed");
        assert_eq!(stored, Some(serde_json::json!({ "audit_channel_id": "777" })));
        assert_eq!(fixture.output.messages().len(), 1);
    }

    /// Last write wins: assigning another channel replaces the previous one.
    #[tokio::test]
    async fn assign_handler_replaces_previous_channel() {
        let storage = configured_storage("777");
        let (_plugin, services, _fixture) = fixture(Some(Arc::clone(&storage)));
        let mut event = member_event(EventKind::MemberJoined, Some(1), "someone");
        event.origin.channel_id = ChannelIdModel(888);

        AssignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("assign expected to succeed");

        let stored = storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed");
        assert_eq!(stored, Some(serde_json::json!({ "audit_channel_id": "888" })));
    }

    /// `/unassign_tracker` clears the config; tracking goes silent afterwards.
    #[tokio::test]
    async fn unassign_handler_clears_config() {
        let storage = configured_storage("777");
        let (plugin, services, fixture) = fixture(Some(Arc::clone(&storage)));
        let event = member_event(EventKind::MemberJoined, Some(1), "someone");

        UnassignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("unassign expected to succeed");

        let stored = storage
            .guild_scoped(Platform::Discord, GuildId(1))
            .get(NAMESPACE, CONFIG_KEY)
            .await
            .expect("storage get expected to succeed");
        assert_eq!(stored, None);
        assert_eq!(fixture.output.messages().len(), 1);

        // Tracking is off now: a join produces no audit message and no event.
        let mut join = member_event(EventKind::MemberJoined, Some(1), "someone");
        let _ = plugin.pre(&mut join, &services).await;
        assert!(fixture.sent.messages.lock().is_empty());
    }

    #[tokio::test]
    async fn commands_in_direct_messages_reply_guild_only() {
        let (_plugin, services, fixture) = fixture(None);
        let event = member_event(EventKind::MemberJoined, None, "someone");

        AssignTrackerHandler
            .invoke(&event, &CommandArgs::default(), &services)
            .await
            .expect("assign expected to succeed");

        assert_eq!(fixture.output.messages(), ["This command only works inside a server."]);
    }

    /// The plugin declares its own commands during `init()`; the dispatcher
    /// and the Discord registrar consume the descriptors meaning-free.
    #[test]
    fn init_registers_tracker_commands() {
        let registry = Arc::new(InMemoryCommandRegistry::new());
        let plugin = UserActivityTrackerPlugin::new(InMemoryEventBus::new(), registry.clone());

        plugin.init().expect("init expected to succeed");

        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 2);
        assert!(descriptors.iter().all(|d| d.plugin_id == "tracker"));
        let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"assign_tracker"));
        assert!(names.contains(&"unassign_tracker"));
        assert!(
            descriptors
                .iter()
                .all(|d| d.required_permission.as_ref().is_some_and(|p| p.name == "manage_guild"))
        );
    }

    /// Guards the reserved namespace constant; the tracker must never
    /// collide with guild settings.
    #[test]
    fn tracker_namespace_is_not_the_reserved_guild_namespace() {
        assert_ne!(NAMESPACE, GUILD_SETTINGS);
    }
}
