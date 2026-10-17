//! Audit trail for plugin-owned domain events. The first event-bus
//! consumer: a `PluginPort`-only subscriber, deliberately not in the
//! middleware pipeline - it reacts to derived facts published by other
//! plugins (the pipeline<->bus bridge), not to raw inbound events. It
//! references the event types of those plugins - the type is the bus
//! contract; there are no direct calls between the plugins.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::kernel::{
    models::{ChannelId, GuildId, MessageId, PluginError, UserId},
    plugin_ports::{EventBusPort, EventBusSubscription, EventHandler, PluginPort},
};
use crate::plugins::tracker::{
    MessageDeleted, MessageEdited, MessagePurged, UserJoinedGuild, UserLeftGuild,
};

/// Logs membership changes and message edit/delete/purge facts across all
/// guilds as structured `audit` tracing events - stdout plus Sentry/GlitchTip
/// through the tracing layer, with origin fields attached, so the trail is
/// queryable without any per-guild configuration. Discord-side per-guild
/// logging stays the tracker's job; this plugin observes the domain facts
/// themselves.
///
/// Privacy note: message TEXT is never logged here - only ids and text
/// lengths. Message content belongs to the tracker's per-guild audit channel
/// (an operator-configured surface), not to operator telemetry.
pub struct AuditLogPlugin<B: EventBusPort> {
    bus: B,
    /// Live bus subscriptions; non-empty means `init` already ran. The
    /// kernel stops a plugin exactly once, but re-init stays harmless.
    subscriptions: Mutex<Vec<EventBusSubscription>>,
}

impl<B: EventBusPort> AuditLogPlugin<B> {
    #[must_use]
    pub fn new(bus: B) -> Self {
        Self { bus, subscriptions: Mutex::new(Vec::new()) }
    }
}

impl<B: EventBusPort> PluginPort for AuditLogPlugin<B> {
    fn name(&self) -> &'static str {
        "audit_log"
    }

    fn init(&self) -> Result<(), PluginError> {
        let mut subscriptions = self.subscriptions.lock();
        // Subscription is the plugin's setup: no events flow before the
        // kernel boots, so init-time subscription cannot miss any. A repeated
        // init must not re-subscribe - that would duplicate the handling.
        if subscriptions.is_empty() {
            subscriptions.push(
                self.bus
                    .subscribe(Arc::new(TrackerAudit) as Arc<dyn EventHandler<UserJoinedGuild>>),
            );
            subscriptions.push(
                self.bus.subscribe(Arc::new(TrackerAudit) as Arc<dyn EventHandler<UserLeftGuild>>),
            );
            subscriptions.push(
                self.bus.subscribe(Arc::new(TrackerAudit) as Arc<dyn EventHandler<MessageEdited>>),
            );
            subscriptions.push(
                self.bus.subscribe(Arc::new(TrackerAudit) as Arc<dyn EventHandler<MessageDeleted>>),
            );
            subscriptions.push(
                self.bus.subscribe(Arc::new(TrackerAudit) as Arc<dyn EventHandler<MessagePurged>>),
            );
        }
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        let mut subscriptions = self.subscriptions.lock();
        for subscription in subscriptions.iter() {
            subscription.unsubscribe();
        }
        subscriptions.clear();
        Ok(())
    }
}

/// Reacts to tracker-owned domain events.
struct TrackerAudit;

impl EventHandler<UserJoinedGuild> for TrackerAudit {
    fn handle(&self, event: &UserJoinedGuild) {
        log_membership("joined the guild", event);
    }
}

impl EventHandler<UserLeftGuild> for TrackerAudit {
    fn handle(&self, event: &UserLeftGuild) {
        log_membership("left the guild", event);
    }
}

impl EventHandler<MessageEdited> for TrackerAudit {
    fn handle(&self, event: &MessageEdited) {
        tracing::info!(
            target: "audit",
            guild_id = event.guild_id.get(),
            channel_id = event.channel_id.get(),
            message_id = event.message_id.get(),
            user_id = event.user_id.get(),
            old_captured = event.old_content.is_some(),
            old_len = event.old_content.as_ref().map(|text| text.chars().count()),
            new_len = event.new_content.chars().count(),
            "message edited"
        );
    }
}

impl EventHandler<MessageDeleted> for TrackerAudit {
    fn handle(&self, event: &MessageDeleted) {
        tracing::info!(
            target: "audit",
            guild_id = event.guild_id.get(),
            channel_id = event.channel_id.get(),
            message_id = event.message_id.get(),
            user_id = event.user_id.get(),
            captured = event.content.is_some(),
            len = event.content.as_ref().map(|text| text.chars().count()),
            "message deleted"
        );
    }
}

impl EventHandler<MessagePurged> for TrackerAudit {
    fn handle(&self, event: &MessagePurged) {
        tracing::info!(
            target: "audit",
            guild_id = event.guild_id.get(),
            channel_id = event.channel_id.get(),
            count = event.count,
            "messages purged"
        );
    }
}

/// Accessors shared by the two membership event types.
trait MembershipEvent {
    fn guild_id(&self) -> GuildId;
    fn user_id(&self) -> UserId;
    fn username(&self) -> Option<&str>;
}

impl MembershipEvent for UserJoinedGuild {
    fn guild_id(&self) -> GuildId {
        self.guild_id
    }

    fn user_id(&self) -> UserId {
        self.user_id
    }

    fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }
}

impl MembershipEvent for UserLeftGuild {
    fn guild_id(&self) -> GuildId {
        self.guild_id
    }

    fn user_id(&self) -> UserId {
        self.user_id
    }

    fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }
}

fn log_membership(what: &str, event: &impl MembershipEvent) {
    tracing::info!(
        target: "audit",
        guild_id = event.guild_id().get(),
        user_id = event.user_id().get(),
        username = event.username().unwrap_or("unknown"),
        "user {what}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::plugin_adapters::InMemoryEventBus;
    use parking_lot::Mutex;
    use std::io;

    /// In-memory writer capturing formatted log lines for assertions.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn joined_event() -> UserJoinedGuild {
        UserJoinedGuild {
            guild_id: GuildId(1),
            user_id: UserId(3),
            username: Some("someone".to_owned()),
        }
    }

    fn left_event() -> UserLeftGuild {
        UserLeftGuild { guild_id: GuildId(1), user_id: UserId(3), username: None }
    }

    fn edited_event() -> MessageEdited {
        MessageEdited {
            guild_id: GuildId(1),
            channel_id: ChannelId(2),
            message_id: MessageId(4),
            user_id: UserId(3),
            author_name: Some("someone".to_owned()),
            old_content: Some("before".to_owned()),
            new_content: "after".to_owned(),
        }
    }

    fn deleted_event() -> MessageDeleted {
        MessageDeleted {
            guild_id: GuildId(1),
            channel_id: ChannelId(2),
            message_id: MessageId(4),
            user_id: UserId(3),
            author_name: Some("someone".to_owned()),
            content: Some("gone".to_owned()),
        }
    }

    fn purged_event() -> MessagePurged {
        MessagePurged { guild_id: GuildId(1), channel_id: ChannelId(2), count: 7 }
    }

    /// `init()` must subscribe all tracker event types: publishing on the
    /// same bus afterwards reaches the audit handler.
    #[test]
    fn init_subscribes_the_tracker_events() {
        let bus = InMemoryEventBus::new();
        let plugin = AuditLogPlugin::new(bus.clone());
        let capture = Capture::default();
        let writer = capture.clone();

        let subscriber =
            tracing_subscriber::fmt().with_ansi(false).with_writer(move || writer.clone()).finish();

        tracing::subscriber::with_default(subscriber, || {
            plugin.init().expect("init expected to succeed");
            bus.publish(joined_event());
            bus.publish(left_event());
            bus.publish(edited_event());
            bus.publish(deleted_event());
            bus.publish(purged_event());
        });

        let log = String::from_utf8(capture.0.lock().clone()).expect("log expected to be utf8");
        assert!(log.contains("user joined the guild"), "log: {log}");
        assert!(log.contains("user left the guild"), "log: {log}");
        assert!(log.contains("message edited"), "log: {log}");
        assert!(log.contains("message deleted"), "log: {log}");
        assert!(log.contains("messages purged"), "log: {log}");
        assert!(log.contains("count=7"), "log: {log}");
        // fmt quotes string field values.
        assert!(log.contains("guild_id=1"), "log: {log}");
        assert!(log.contains("username=\"someone\""), "log: {log}");
        assert!(log.contains("username=\"unknown\""), "log: {log}");
        // Message events log ids and text LENGTHS, never the text itself
        // (message-sized payloads stay out of operator telemetry).
        assert!(log.contains("old_len=6"), "log: {log}");
        assert!(log.contains("new_len=5"), "log: {log}");
        assert!(!log.contains("before"), "text must not be logged: {log}");
        assert!(!log.contains("after"), "text must not be logged: {log}");
        assert!(!log.contains("gone"), "text must not be logged: {log}");
    }

    /// Captures formatted log lines produced while `f` runs.
    fn captured<F: FnOnce()>(f: F) -> String {
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber =
            tracing_subscriber::fmt().with_ansi(false).with_writer(move || writer.clone()).finish();

        tracing::subscriber::with_default(subscriber, f);
        String::from_utf8(capture.0.lock().clone()).expect("log expected to be utf8")
    }

    /// A repeated init must not re-subscribe: after two `init()` calls, one
    /// published event produces exactly ONE audit handling.
    #[test]
    fn double_init_does_not_duplicate_handling() {
        let bus = InMemoryEventBus::new();
        let plugin = AuditLogPlugin::new(bus.clone());

        plugin.init().expect("first init expected to succeed");
        plugin.init().expect("second init expected to succeed");

        let log = captured(|| bus.publish(joined_event()));
        assert_eq!(
            log.matches("user joined the guild").count(),
            1,
            "double init must not duplicate the handling; log: {log}"
        );
    }

    /// `stop()` unsubscribes everything `init()` subscribed; a later init
    /// subscribes again - the plugin is reusable across kernel restarts.
    #[test]
    fn stop_unsubscribes_and_reinit_resubscribes() {
        let bus = InMemoryEventBus::new();
        let plugin = AuditLogPlugin::new(bus.clone());

        plugin.init().expect("init expected to succeed");
        plugin.stop().expect("stop expected to succeed");

        let log = captured(|| bus.publish(joined_event()));
        assert_eq!(
            log.matches("user joined the guild").count(),
            0,
            "a stopped plugin must not handle events; log: {log}"
        );

        plugin.init().expect("re-init expected to succeed");
        let log = captured(|| bus.publish(joined_event()));
        assert_eq!(
            log.matches("user joined the guild").count(),
            1,
            "a re-initialized plugin must handle events again; log: {log}"
        );
    }
}
