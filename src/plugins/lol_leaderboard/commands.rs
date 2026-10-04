//! The `/lol_leaderboard` command: refresh-if-stale under the typing
//! indicator, then post the statistics as public channel messages. Unlike
//! configuration commands, the dump itself is channel-visible on purpose -
//! it is a stats artifact, not private state.

use std::sync::Arc;

use async_trait::async_trait;

use crate::common::command_reply;
use crate::kernel::{
    models::{OutboundMessage, RequestContext},
    plugin_ports::{CommandArgs, CommandHandler},
    services::KernelServices,
};

use super::{engine::LeaderboardEngine, format, stats};

pub struct LeaderboardHandler {
    engine: Arc<LeaderboardEngine>,
}

impl LeaderboardHandler {
    #[must_use]
    pub fn new(engine: Arc<LeaderboardEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl CommandHandler for LeaderboardHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        if !self.engine.is_configured() {
            services
                .chat_output
                .send(command_reply("The leaderboard source is not configured on this bot."))
                .await?;
            return Ok(());
        }

        // A cold parse can run tens of seconds (sequential, throttled
        // requests): hold the platform typing indicator for the whole time -
        // the adapter refreshes it on its own cadence until dropped.
        let _typing = services.chat_output_factory.start_typing(&event.origin);

        let snapshot = match self.engine.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::warn!(error = %err, "leaderboard source unavailable");
                services
                    .chat_output
                    .send(command_reply(
                        "The leaderboard source is currently unavailable - try again later.",
                    ))
                    .await?;
                return Ok(());
            }
        };

        let messages = format::render(&stats::build(&snapshot));
        for message in messages {
            // Plain public sends: the dump is a channel-visible artifact.
            services.chat_output.send(OutboundMessage::text(message)).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::{
        ChannelId, CommandPayload, EventKind, EventPayload, GuildId, Origin, Platform, UserId,
    };
    use crate::plugins::lol_leaderboard::ResolvedView;
    use crate::plugins::lol_leaderboard::engine::{EngineSettings, test_support::FakeSource};
    use crate::test_support::{RecordingChatOutput, RecordingChatOutputFactory};
    use std::time::Duration;

    fn engine_with(
        source: Arc<FakeSource>,
        regions: &[&str],
        ttl: Duration,
    ) -> Arc<LeaderboardEngine> {
        let regions: Vec<String> = regions.iter().map(|key| (*key).to_owned()).collect();
        let settings = EngineSettings::new(
            source.as_ref(),
            &regions,
            100,
            ttl,
            Duration::ZERO,
            ResolvedView::resolve(100, &[300], 300, 2),
        );
        Arc::new(LeaderboardEngine::new(source, settings))
    }

    fn services_for(
        output: &Arc<RecordingChatOutput>,
        factory: &Arc<RecordingChatOutputFactory>,
    ) -> KernelServices {
        KernelServices {
            chat_output: output.clone(),
            chat_output_factory: factory.clone(),
            guild_storage: None,
        }
    }

    fn command_event() -> RequestContext {
        RequestContext {
            kind: EventKind::CommandInvoked,
            origin: Origin {
                platform: Platform::Discord,
                guild_id: Some(GuildId(1)),
                channel_id: ChannelId(2),
                user_id: UserId(3),
                message_id: None,
                reply_token: Some("token".to_owned()),
            },
            payload: EventPayload::Command(CommandPayload {
                name: "lol_leaderboard".to_owned(),
                args: Vec::new(),
                author_roles: Vec::new(),
                author_permissions: 0,
            }),
        }
    }

    /// Warms the engine's cache so the command under test takes the fresh
    /// path deterministically.
    async fn warmed_engine(regions: &[&str]) -> (Arc<LeaderboardEngine>, Arc<FakeSource>) {
        let source = FakeSource::ungated();
        let engine = engine_with(source.clone(), regions, Duration::from_secs(3600));
        engine.snapshot().await.expect("warm-up snapshot");
        (engine, source)
    }

    #[tokio::test]
    async fn unconfigured_engine_answers_ephemerally_without_touching_the_source() {
        let source = FakeSource::new();
        let engine = engine_with(source.clone(), &[], Duration::from_secs(3600));
        let output = RecordingChatOutput::new();
        let factory = Arc::new(RecordingChatOutputFactory::new(output.clone()));
        let services = services_for(&output, &factory);
        let handler = LeaderboardHandler::new(engine);

        handler
            .invoke(&command_event(), &CommandArgs(Vec::new()), &services)
            .await
            .expect("invoke");

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        let first = sent.first().expect("one message");
        assert!(first.ephemeral);
        assert!(first.content.contains("not configured"));
        assert_eq!(source.leaderboard_calls(), 0);
    }

    #[tokio::test]
    async fn fresh_cache_dumps_publicly_under_the_typing_indicator() {
        let (engine, source) = warmed_engine(&["kr"]).await;
        let output = RecordingChatOutput::new();
        let factory = Arc::new(RecordingChatOutputFactory::new(output.clone()));
        let services = services_for(&output, &factory);
        let handler = LeaderboardHandler::new(engine);

        handler
            .invoke(&command_event(), &CommandArgs(Vec::new()), &services)
            .await
            .expect("invoke");

        // The dump is public; a short dump packs into one message.
        let sent = output.sent();
        assert!(!sent.is_empty());
        assert!(sent.iter().all(|message| !message.ephemeral));
        let first = sent.first().expect("at least one message");
        assert!(first.content.starts_with("# LoL Leaderboard Statistics"));
        assert!(sent.iter().all(|message| !message.is_empty()));
        // Typing was engaged for the answer.
        assert!(factory.typing_starts() >= 1);
        // Fresh cache: no further source calls.
        assert_eq!(source.leaderboard_calls(), 1);
    }

    #[tokio::test]
    async fn dead_source_answers_ephemerally_without_public_output() {
        let source = FakeSource::ungated();
        source.fail_region("kr");
        let engine = engine_with(source, &["kr"], Duration::from_secs(3600));
        let output = RecordingChatOutput::new();
        let factory = Arc::new(RecordingChatOutputFactory::new(output.clone()));
        let services = services_for(&output, &factory);
        let handler = LeaderboardHandler::new(engine);

        handler
            .invoke(&command_event(), &CommandArgs(Vec::new()), &services)
            .await
            .expect("invoke");

        let sent = output.sent();
        assert_eq!(sent.len(), 1);
        let first = sent.first().expect("one message");
        assert!(first.ephemeral);
        assert!(first.content.contains("unavailable"));
    }

    #[tokio::test]
    async fn partially_failed_refresh_still_dumps_and_names_the_failure() {
        let source = FakeSource::ungated();
        source.fail_region("na");
        let engine = engine_with(source, &["kr", "na"], Duration::from_secs(3600));
        let output = RecordingChatOutput::new();
        let factory = Arc::new(RecordingChatOutputFactory::new(output.clone()));
        let services = services_for(&output, &factory);
        let handler = LeaderboardHandler::new(engine);

        handler
            .invoke(&command_event(), &CommandArgs(Vec::new()), &services)
            .await
            .expect("invoke");

        let sent = output.sent();
        assert!(!sent.is_empty());
        assert!(sent.iter().all(|message| !message.ephemeral));
        let whole = sent.iter().map(|m| m.content.as_str()).collect::<Vec<_>>().join("\n");
        assert!(whole.contains("Could not refresh: na"));
        // kr served (10 players from the fake's default board), na absent.
        assert!(whole.contains("Parsed 10/200 players from 1 region"));
    }
}
