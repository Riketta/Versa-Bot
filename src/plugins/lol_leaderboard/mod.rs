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
use std::time::Duration;

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
        // Scheduled even with no usable regions yet: a cycle with nothing
        // pending is a cheap no-op, and a job that exists from boot is what
        // makes hot-reloaded regions stay warm - without it the command
        // path (MissingOnly in background mode) would parse each region
        // once inline and then serve it frozen forever.
        self.schedule_job(settings.cache_ttl);
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        if let Some(job) = self.job.lock().take() {
            job.cancel();
        }
        Ok(())
    }
}

impl LeaderboardPlugin {
    /// Registers the background job at `interval`. The job ticks at the
    /// cache TTL and runs immediately on the first tick: the boot warm-up
    /// happens off the user path, before the first command can arrive. A
    /// slow cycle delays the next tick (MissedTickBehavior::Delay) - deep
    /// configs never pile up.
    fn schedule_job(&self, interval: Duration) {
        let job = self.scheduler.schedule(
            "lol_leaderboard_refresh",
            interval,
            Arc::new(engine::RefreshJob { engine: Arc::clone(&self.engine) }),
        );
        *self.job.lock() = Some(job);
        tracing::info!(ttl_secs = interval.as_secs(), "leaderboard background refresh scheduled");
    }

    /// Hot reload of `cache_ttl`: swap the job's tick cadence. No-op when
    /// background mode was off at boot (no job exists to reschedule) - the
    /// mode itself stays a boot decision.
    pub fn reschedule(&self, interval: Duration) {
        if self.job.lock().is_none() {
            return;
        }
        if let Some(job) = self.job.lock().take() {
            job.cancel();
        }
        self.schedule_job(interval);
        tracing::info!(ttl_secs = interval.as_secs(), "leaderboard refresh job rescheduled");
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

    /// The job exists from boot even with no usable regions: a cycle with
    /// nothing pending is a cheap no-op, and the job is what keeps
    /// hot-reloaded regions warm afterwards - without it the command path
    /// (MissingOnly in background mode) would parse each region once inline
    /// and then serve it frozen forever.
    #[test]
    fn start_schedules_the_job_even_without_usable_regions() {
        let scheduler = Arc::new(RecordingScheduler::default());
        let source = FakeSource::ungated();
        // "mars" is not served by the source: normalization drops it and
        // the engine boots unconfigured.
        let settings = EngineSettings::new(
            source.as_ref(),
            &["mars".to_owned()],
            1000,
            Duration::from_secs(3600),
            Duration::ZERO,
            true,
            Duration::ZERO,
            ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        );
        let plugin = LeaderboardPlugin::new(
            Arc::new(CapturingCommandRegistry::default()) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
            Arc::new(LeaderboardEngine::new(source, settings)),
        );
        plugin.init().expect("init");
        assert!(!plugin.engine.is_configured());
        plugin.start().expect("start");

        assert_eq!(
            scheduler.jobs.lock().expect("jobs lock").as_slice(),
            vec![("lol_leaderboard_refresh".to_owned(), Duration::from_secs(3600))],
            "the job is scheduled regardless of the boot-time region set"
        );
    }

    /// A TTL hot reload swaps the job's cadence; with no job (background
    /// mode off at boot) there is nothing to reschedule.
    #[test]
    fn reschedule_swaps_the_job_interval() {
        let scheduler = Arc::new(RecordingScheduler::default());
        let plugin = LeaderboardPlugin::new(
            Arc::new(CapturingCommandRegistry::default()) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
            mode_engine(true),
        );
        plugin.init().expect("init");
        plugin.start().expect("start");
        plugin.reschedule(Duration::from_secs(600));

        assert_eq!(
            scheduler.jobs.lock().expect("jobs lock").as_slice(),
            vec![
                ("lol_leaderboard_refresh".to_owned(), Duration::from_secs(3600)),
                ("lol_leaderboard_refresh".to_owned(), Duration::from_secs(600)),
            ],
            "rescheduling replaces the boot cadence"
        );

        // No job at boot: reschedule stays a no-op instead of minting one.
        let cold = LeaderboardPlugin::new(
            Arc::new(CapturingCommandRegistry::default()) as Arc<dyn CommandRegistryPort>,
            Arc::clone(&scheduler) as Arc<dyn SchedulerPort>,
            mode_engine(false),
        );
        cold.reschedule(Duration::from_secs(600));
        assert_eq!(scheduler.jobs.lock().expect("jobs lock").len(), 2);
    }
}
