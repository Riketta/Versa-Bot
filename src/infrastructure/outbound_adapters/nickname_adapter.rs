use std::sync::Arc;

use async_trait::async_trait;
use serenity::all::GuildId as SerenityGuildId;

use crate::kernel::{
    models::{GuildId, OutboundError},
    spi_ports::NicknamePort,
};

use super::presence_adapter::GatewayContext;

/// Discord [`NicknamePort`]: renames the bot in one guild via the REST API,
/// through the gateway context the driving adapter attaches on `ready`.
/// Unlike presence - queued until connect, because a scheduler tick may
/// precede it - these requests are interactive command replies: when the
/// context is absent the call fails and the invoking member sees the notice.
pub struct SerenityNickname {
    context: Arc<GatewayContext>,
}

impl SerenityNickname {
    #[must_use]
    pub fn new(context: Arc<GatewayContext>) -> Self {
        Self { context }
    }
}

#[async_trait]
impl NicknamePort for SerenityNickname {
    async fn set(&self, guild: GuildId, name: Option<&str>) -> Result<(), OutboundError> {
        let context = self
            .context
            .context
            .get()
            .ok_or_else(|| OutboundError::Send("gateway context not ready yet".to_owned()))?;
        // The REST half needs a live gateway `Context` and is exercised in
        // production only - the serenity boundary (same as presence).
        context
            .http
            .edit_nickname(SerenityGuildId::new(guild.get()), name, None)
            .await
            .map_err(|err| OutboundError::Send(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Before `ready` the port fails instead of queueing: the caller is a
    /// command handler whose member is waiting for an answer, not a
    /// best-effort rotation tick.
    #[tokio::test]
    async fn missing_gateway_context_fails_rather_than_queueing() {
        let adapter = SerenityNickname::new(Arc::new(GatewayContext::default()));

        let err = adapter
            .set(GuildId(1), Some("Sage"))
            .await
            .expect_err("no gateway context expected to fail");
        assert!(err.to_string().contains("not ready"), "{err}");
    }
}
