//! `LoL` leaderboard tracker: aggregated ranked-leaderboard statistics
//! (role distributions, most picked champions per role) from a pluggable
//! data source, dumped on demand by `/lol_leaderboard`.
//!
//! A `PluginPort`-only plugin (no middleware hook, no events): its primary
//! driver is the command, and its only state is a process-lifetime cache
//! of world data - there is deliberately no guild storage and no per-guild
//! setup. With `background_refresh` (the default) a scheduler job keeps
//! the cache warm off the user path and the command serves whatever is
//! cached, age-honest; without it the command re-parses stale regions
//! inline before replying. Own hexagon inside: the engine depends on
//! the [`port::LeaderboardSourcePort`] boundary,
//! [`DeepLolSource`] is the one adapter.
//!
//! Disabled by default: without a `[lol_leaderboard]` config section (or
//! with no usable regions) the command still registers and explains
//! itself. Startup-only settings (pacing, background mode, budget)
//! require a restart; the data window hot-reloads.

mod commands;
mod deeplol;
mod engine;
mod format;
mod port;
mod stats;

use std::sync::Arc;

use parking_lot::Mutex;

use crate::kernel::{
    models::PluginError,
    plugin_ports::{
        AccessTier, CommandDescriptor, CommandRegistryPort, JobHandle, PluginPort, SchedulerPort,
    },
};

pub use commands::LeaderboardHandler;
pub use deeplol::DeepLolSource;
pub use engine::{EngineSettings, LeaderboardEngine, ResolvedView, Snapshot};
pub use port::{LeaderboardPlayer, LeaderboardSourcePort, RegionLeaderboard, Role, SourceError};

/// The plugin facade: command registration (`init`) plus the optional
/// background-refresh job (`start`/`stop`).
pub struct LeaderboardPlugin {
    registry: Arc<dyn CommandRegistryPort>,
    scheduler: Arc<dyn SchedulerPort>,
    engine: Arc<LeaderboardEngine>,
    job: Mutex<Option<JobHandle>>,
}

impl LeaderboardPlugin {
    #[must_use]
    pub fn new(
        registry: Arc<dyn CommandRegistryPort>,
        scheduler: Arc<dyn SchedulerPort>,
        engine: Arc<LeaderboardEngine>,
    ) -> Self {
        Self { registry, scheduler, engine, job: Mutex::new(None) }
    }
}

impl PluginPort for LeaderboardPlugin {
    fn name(&self) -> &'static str {
        "lol_leaderboard"
    }

    fn init(&self) -> Result<(), PluginError> {
        self.registry.register(
            CommandDescriptor {
                plugin_id: self.name().to_owned(),
                name: "lol_leaderboard".to_owned(),
                description:
                    "LoL ranked leaderboard stats: role distribution and most picked champions"
                        .to_owned(),
                arguments: Vec::new(),
                required_permission: None,
                required_tier: Some(AccessTier::User),
                guild_only: false,
            },
            Arc::new(LeaderboardHandler::new(Arc::clone(&self.engine))),
        );
        Ok(())
    }

    fn start(&self) -> Result<(), PluginError> {
        let settings = self.engine.settings();
        if !settings.background_refresh {
            tracing::info!("leaderboard background refresh disabled - on-demand refresh only");
            return Ok(());
        }
        if !self.engine.is_configured() {
            tracing::info!("leaderboard has no usable regions - no background refresh job");
            return Ok(());
        }
        // The job ticks at the cache TTL and runs immediately on the first
        // tick: the boot warm-up happens off the user path, before the
        // first command can arrive. A slow cycle delays the next tick
        // (MissedTickBehavior::Delay) - deep configs never pile up.
        let job = self.scheduler.schedule(
            "lol_leaderboard_refresh",
            settings.cache_ttl,
            Arc::new(engine::RefreshJob { engine: Arc::clone(&self.engine) }),
        );
        *self.job.lock() = Some(job);
        tracing::info!(
            ttl_secs = settings.cache_ttl.as_secs(),
            "leaderboard background refresh scheduled"
        );
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        if let Some(job) = self.job.lock().take() {
            job.cancel();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::engine::test_support::FakeSource;
    use super::*;
    use crate::kernel::plugin_ports::{CommandRegistryPort, Job, SchedulerPort};
    use crate::test_support::{
        CapturingCommandRegistry, NoopScheduler, assert_descriptions_fit_discord,
    };
    use std::time::Duration;

    fn engine(regions: &[&str]) -> Arc<LeaderboardEngine> {
        let source = FakeSource::ungated();
        let keys: Vec<String> = regions.iter().map(|key| (*key).to_owned()).collect();
        let settings = EngineSettings::new(
            source.as_ref(),
            &keys,
            1000,
            Duration::from_secs(3600),
            Duration::ZERO,
            false,
            Duration::ZERO,
            ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        );
        Arc::new(LeaderboardEngine::new(source, settings))
    }

    #[test]
    fn registers_one_command_within_discord_limits() {
        let registry = Arc::new(CapturingCommandRegistry::default());
        let plugin = LeaderboardPlugin::new(
            Arc::clone(&registry) as Arc<dyn CommandRegistryPort>,
            Arc::new(NoopScheduler) as Arc<dyn SchedulerPort>,
            engine(&["kr"]),
        );
        plugin.init().expect("init");

        let descriptors = registry.descriptors();
        assert_eq!(descriptors.len(), 1);
        let descriptor = descriptors.first().expect("one descriptor");
        assert_eq!(descriptor.name, "lol_leaderboard");
        assert_eq!(descriptor.plugin_id, "lol_leaderboard");
        assert_eq!(descriptor.required_tier, Some(AccessTier::User));
        assert!(!descriptor.guild_only);
        assert!(descriptor.arguments.is_empty());
        assert!(registry.handler("lol_leaderboard").is_some());
        assert_descriptions_fit_discord(&descriptors);
    }

    #[test]
    fn engine_without_usable_regions_is_unconfigured() {
        assert!(!engine(&["mars"]).is_configured());
        assert!(engine(&["kr"]).is_configured());
    }

    /// Scheduler double recording what the plugin asked for; handles are
    /// dead, so cancellation mechanics stay the adapter's tested concern -
    /// this pins the plugin's scheduling call.
    #[derive(Default)]
    struct RecordingScheduler {
        jobs: std::sync::Mutex<Vec<(String, Duration)>>,
    }

    impl SchedulerPort for RecordingScheduler {
        fn schedule(&self, name: &str, interval: Duration, _job: Arc<dyn Job>) -> JobHandle {
            self.jobs.lock().expect("jobs lock").push((name.to_owned(), interval));
            JobHandle::new(Arc::new(|| {}))
        }
    }

    fn mode_engine(background: bool) -> Arc<LeaderboardEngine> {
        let source = FakeSource::ungated();
        let settings = EngineSettings::new(
            source.as_ref(),
            &["kr".to_owned()],
            1000,
            Duration::from_secs(3600),
            Duration::ZERO,
            background,
            Duration::from_secs(180),
            ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        );
        Arc::new(LeaderboardEngine::new(source, settings))
    }

    #[test]
    fn start_schedules_the_background_job_at_the_ttl_and_stop_is_idempotent() {
        let scheduler = Arc::new(RecordingScheduler::default());
        let plugin = LeaderboardPlugin::new(
            Arc::new(CapturingCommandRegistry::default()) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
            mode_engine(true),
        );
        plugin.init().expect("init");
        plugin.start().expect("start");

        assert_eq!(
            scheduler.jobs.lock().expect("jobs lock").as_slice(),
            vec![("lol_leaderboard_refresh".to_owned(), Duration::from_secs(3600))],
            "the job ticks at the cache TTL under the plugin's name"
        );

        plugin.stop().expect("stop");
        plugin.stop().expect("a second stop must be a no-op, not a panic");
        assert_eq!(scheduler.jobs.lock().expect("jobs lock").len(), 1, "stop never re-schedules");
    }

    #[test]
    fn background_off_schedules_nothing() {
        let scheduler = Arc::new(RecordingScheduler::default());
        let plugin = LeaderboardPlugin::new(
            Arc::new(CapturingCommandRegistry::default()) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
            mode_engine(false),
        );
        plugin.init().expect("init");
        plugin.start().expect("start");

        assert!(scheduler.jobs.lock().expect("jobs lock").is_empty());
    }
}
