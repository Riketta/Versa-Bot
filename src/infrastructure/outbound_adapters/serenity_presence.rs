use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use parking_lot::Mutex;
use serenity::all::{ActivityData, Context, OnlineStatus};

use crate::kernel::{
    models::{ActivityKind, OutboundError, Presence},
    spi_ports::PresencePort,
};

/// Shared gateway context handle. The driving adapter attaches the live
/// [`Context`] on `ready`; presence requests that arrived before that are
/// queued and applied exactly then - so the first rotation status shows up
/// as soon as the bot is online, not on the next scheduler tick.
#[derive(Default)]
pub struct GatewayContext {
    /// Crate-visible: the nickname adapter reads the same handle to make
    /// REST calls (rename), as presence makes gateway calls.
    pub(crate) context: OnceLock<Context>,
    pending: Mutex<Option<Presence>>,
}

impl GatewayContext {
    /// Captures the live gateway context and flushes a presence queued
    /// before `ready` (if any). Re-attaching (a new gateway session) keeps
    /// the original context.
    pub fn attach(&self, context: Context) {
        if self.context.set(context).is_err() {
            // A re-`ready` hands a second context. serenity 0.12 keeps the
            // same Arc'd shard/cache handles across sessions, so the
            // original stays correct - log so a future serenity change (a
            // materially distinct Context) becomes visible.
            tracing::debug!("gateway re-attached - keeping the existing gateway context");
            return;
        }
        if let Some(presence) = self.pending.lock().take()
            && let Some(context) = self.context.get()
        {
            Self::apply(context, &presence);
        }
    }

    fn apply(context: &Context, presence: &Presence) {
        let (activity, status) = presence_parts(presence);
        context.set_presence(activity, status);
    }
}

/// Maps the taxonomy presence onto Discord's wire shape. `None` activity
/// clears the activity and shows plain online.
fn presence_parts(presence: &Presence) -> (Option<ActivityData>, OnlineStatus) {
    let activity = presence.activity.as_ref().map(|activity| match activity.kind {
        ActivityKind::Playing => ActivityData::playing(activity.name.clone()),
        ActivityKind::Listening => ActivityData::listening(activity.name.clone()),
        ActivityKind::Watching => ActivityData::watching(activity.name.clone()),
        ActivityKind::Competing => ActivityData::competing(activity.name.clone()),
    });
    (activity, OnlineStatus::Online)
}

/// Discord [`PresencePort`]: pushes presence updates through the gateway
/// context the driving adapter attaches on `ready`. Requests made before
/// the gateway is up are queued and applied at attach time instead of
/// failing - a boot-time rotation tick must not lose the first status.
pub struct SerenityPresence {
    context: Arc<GatewayContext>,
}

impl SerenityPresence {
    /// Creates the adapter and the shared gateway context handle that the
    /// driving adapter fills in once connected.
    #[must_use]
    pub fn new() -> (Self, Arc<GatewayContext>) {
        let context = Arc::new(GatewayContext::default());
        (Self { context: Arc::clone(&context) }, context)
    }
}

impl Default for SerenityPresence {
    fn default() -> Self {
        Self::new().0
    }
}

#[async_trait]
impl PresencePort for SerenityPresence {
    async fn set(&self, presence: Presence) -> Result<(), OutboundError> {
        if let Some(context) = self.context.context.get() {
            GatewayContext::apply(context, &presence);
            return Ok(());
        }

        // The gateway is not up yet. Queue under the lock, re-checking:
        // `attach` may land between the first check and the lock, and its
        // flush would miss a presence stored after it - the re-check closes
        // that race from this side.
        let mut pending = self.context.pending.lock();
        if let Some(context) = self.context.context.get() {
            drop(pending);
            GatewayContext::apply(context, &presence);
            return Ok(());
        }
        *pending = Some(presence);
        tracing::debug!("presence queued until the gateway is ready");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Before `ready`, `set` queues instead of failing - the boot-time
    /// rotation tick must not lose the first status, and the caller must
    /// not see an error for an accepted request.
    #[tokio::test]
    async fn set_before_gateway_queues_and_succeeds() {
        let (presence, context) = SerenityPresence::new();

        presence.set(Presence::playing("first".to_owned())).await.expect("set expected to succeed");

        let queued = context.pending.lock().clone();
        let activity = queued.and_then(|presence| presence.activity);
        assert_eq!(
            activity.map(|activity| (activity.kind, activity.name)),
            Some((ActivityKind::Playing, "first".to_owned()))
        );
    }

    /// The taxonomy maps onto Discord's wire shapes: each activity kind
    /// keeps its name and maps to the matching Discord activity type, the
    /// status is always Online, and an activity-less presence clears the
    /// activity. (The `attach`/`apply` half needs a live gateway `Context`
    /// and is exercised in production only - the serenity boundary.)
    #[test]
    fn presence_parts_map_the_taxonomy_onto_discord_shapes() {
        let cases = [
            (ActivityKind::Playing, serenity::all::ActivityType::Playing),
            (ActivityKind::Listening, serenity::all::ActivityType::Listening),
            (ActivityKind::Watching, serenity::all::ActivityType::Watching),
            (ActivityKind::Competing, serenity::all::ActivityType::Competing),
        ];
        for (kind, expected) in cases {
            let presence = Presence {
                activity: Some(crate::kernel::models::Activity { kind, name: "versa".to_owned() }),
            };
            let (activity, status) = presence_parts(&presence);
            let activity = activity.expect("activity expected");
            assert_eq!(activity.kind, expected, "kind {kind:?}");
            assert_eq!(activity.name, "versa");
            assert_eq!(status, OnlineStatus::Online);
        }

        let (activity, status) = presence_parts(&Presence::default());
        assert!(activity.is_none(), "no activity expected");
        assert_eq!(status, OnlineStatus::Online);
    }

    /// A direct set (no gateway race) applies immediately - nothing queued.
    /// The context is absent here, so both checks miss and the queue holds
    /// the latest request: queueing keeps only the newest presence.
    #[tokio::test]
    async fn repeated_sets_before_gateway_keep_only_the_latest() {
        let (presence, context) = SerenityPresence::new();

        presence.set(Presence::playing("one".to_owned())).await.expect("set expected to succeed");
        presence.set(Presence::playing("two".to_owned())).await.expect("set expected to succeed");

        let queued = context.pending.lock().clone();
        assert_eq!(
            queued.and_then(|presence| presence.activity.map(|activity| activity.name)),
            Some("two".to_owned())
        );
    }

    /// The wire mapping: activity kinds map onto Discord builders, a missing
    /// activity clears to plain online.
    #[test]
    fn presence_maps_onto_activity_builders() {
        let (playing, status) = presence_parts(&Presence::playing("x".to_owned()));
        assert!(playing.is_some());
        assert_eq!(status, OnlineStatus::Online);

        let (none, _) = presence_parts(&Presence { activity: None });
        assert!(none.is_none());
    }
}
