//! Audit trail for plugin-owned domain events. The first event-bus
//! consumer: a `PluginPort`-only subscriber, deliberately not in the
//! middleware pipeline - it reacts to derived facts published by other
//! plugins (the pipeline<->bus bridge), not to raw inbound events. It
//! references the event types of those plugins - the type is the bus
//! contract; there are no direct calls between the plugins.

use std::sync::Arc;

use parking_lot::Mutex;

use crate::kernel::{
    models::{GuildId, Platform, PluginError, UserId},
    plugin_ports::{EventBusPort, EventBusSubscription, EventHandler, PluginPort},
};
use crate::plugins::tracker::{UserJoinedGuild, UserLeftGuild};

/// Logs membership changes across all guilds as structured `audit` tracing
/// events - stdout plus Sentry/GlitchTip through the tracing layer, with
/// origin fields attached, so the trail is queryable without any per-guild
/// configuration. Discord-side per-guild logging stays the tracker's job;
/// this plugin observes the domain facts themselves.
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
                    .subscribe(Arc::new(MembershipAudit) as Arc<dyn EventHandler<UserJoinedGuild>>),
            );
            subscriptions.push(
                self.bus
                    .subscribe(Arc::new(MembershipAudit) as Arc<dyn EventHandler<UserLeftGuild>>),
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

/// Reacts to tracker-owned membership events.
struct MembershipAudit;

impl EventHandler<UserJoinedGuild> for MembershipAudit {
    fn handle(&self, event: &UserJoinedGuild) {
        log_membership("joined the guild", event);
    }
}

impl EventHandler<UserLeftGuild> for MembershipAudit {
    fn handle(&self, event: &UserLeftGuild) {
        log_membership("left the guild", event);
    }
}

/// Accessors shared by the two membership event types.
trait MembershipEvent {
    fn platform(&self) -> Platform;
    fn guild_id(&self) -> GuildId;
    fn user_id(&self) -> UserId;
    fn username(&self) -> Option<&str>;
}

impl MembershipEvent for UserJoinedGuild {
    fn platform(&self) -> Platform {
        self.platform
    }

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
    fn platform(&self) -> Platform {
        self.platform
    }

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
        platform = event.platform().as_str(),
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
            platform: Platform::Discord,
            guild_id: GuildId(1),
            user_id: UserId(3),
            username: Some("someone".to_owned()),
        }
    }

    fn left_event() -> UserLeftGuild {
        UserLeftGuild {
            platform: Platform::Discord,
            guild_id: GuildId(1),
            user_id: UserId(3),
            username: None,
        }
    }

    /// `init()` must subscribe both membership event types: publishing on the
    /// same bus afterwards reaches the audit handler.
    #[test]
    fn init_subscribes_membership_audit() {
        let bus = InMemoryEventBus::new();
        let plugin = AuditLogPlugin::new(bus.clone());
        let capture = Capture::default();
        let writer = capture.clone();

        let subscriber =
            tracing_subscriber::fmt().with_ansi(false).with_writer(move || writer.clone()).finish();

        tracing::subscriber::with_default(subscriber, || {
            plugin.init().expect("init expected to succeed");
            bus.publish(Arc::new(joined_event()));
            bus.publish(Arc::new(left_event()));
        });

        let log = String::from_utf8(capture.0.lock().clone()).expect("log expected to be utf8");
        assert!(log.contains("user joined the guild"), "log: {log}");
        assert!(log.contains("user left the guild"), "log: {log}");
        // fmt quotes string field values.
        assert!(log.contains("platform=\"discord\""), "log: {log}");
        assert!(log.contains("guild_id=1"), "log: {log}");
        assert!(log.contains("username=\"someone\""), "log: {log}");
        assert!(log.contains("username=\"unknown\""), "log: {log}");
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

        let log = captured(|| bus.publish(Arc::new(joined_event())));
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

        let log = captured(|| bus.publish(Arc::new(joined_event())));
        assert_eq!(
            log.matches("user joined the guild").count(),
            0,
            "a stopped plugin must not handle events; log: {log}"
        );

        plugin.init().expect("re-init expected to succeed");
        let log = captured(|| bus.publish(Arc::new(joined_event())));
        assert_eq!(
            log.matches("user joined the guild").count(),
            1,
            "a re-initialized plugin must handle events again; log: {log}"
        );
    }
}
