use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serenity::all::{ActivityData, Context, OnlineStatus};

use crate::kernel::{
    models::{ActivityKind, OutboundError, Presence},
    spi_ports::PresencePort,
};

/// Discord [`PresencePort`]: pushes presence updates through the gateway
/// context captured by the driving adapter on `ready`. Until the gateway is
/// up, `set` fails with a send error - callers treat that as best effort.
pub struct SerenityPresence {
    context: Arc<OnceLock<Context>>,
}

impl SerenityPresence {
    /// Creates the adapter and the gateway context handle that the driving
    /// adapter fills in once connected.
    #[must_use]
    pub fn new() -> (Self, Arc<OnceLock<Context>>) {
        let context = Arc::new(OnceLock::new());
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
        let context = self
            .context
            .get()
            .ok_or_else(|| OutboundError::Send("gateway context not ready yet".to_owned()))?;

        let activity = presence.activity.map(|activity| match activity.kind {
            ActivityKind::Playing => ActivityData::playing(activity.name),
            ActivityKind::Listening => ActivityData::listening(activity.name),
            ActivityKind::Watching => ActivityData::watching(activity.name),
            ActivityKind::Competing => ActivityData::competing(activity.name),
        });

        context.set_presence(activity, OnlineStatus::Online);
        Ok(())
    }
}
