use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rand::seq::SliceRandom;

use crate::kernel::{
    models::{PluginError, Presence},
    plugin_ports::{Job, JobHandle, PluginPort, SchedulerPort},
    spi_ports::PresencePort,
};

/// The plugin's hot-reloadable settings: rotation interval and status list.
/// `statuses` empty means the rotation is disabled.
#[derive(Debug, Clone, PartialEq)]
pub struct StatusSettings {
    pub interval: Duration,
    pub statuses: Vec<String>,
}

impl StatusSettings {
    /// Disabled state: empty list; the interval is a placeholder.
    #[must_use]
    pub fn disabled() -> Self {
        Self { interval: Duration::ZERO, statuses: Vec::new() }
    }
}

/// Rotates the bot's presence through a status list on a fixed interval
/// (`PluginPort`-only - it is not part of the inbound pipeline).
///
/// Settings come from the bot configuration (`[status]` section): presence
/// is a global concern, not a per-guild one. The plugin starts disabled
/// when the list is empty and re-applies on configuration changes
/// ([`Self::update`]): identical settings are a no-op, an empty list stops
/// the rotation, otherwise the job is rescheduled with the new interval and
/// list. `start` schedules only when no job is active yet, so an `update`
/// landing between construction and start cannot double-schedule. The job
/// runs on the kernel scheduler (first run immediate - a status set before
/// the gateway is up is queued by the presence adapter and lands exactly on
/// connect); `stop` cancels it. Statuses draw from a shuffled deck: every
/// status shows once per cycle, in fake-random order.
pub struct StatusRotatorPlugin {
    scheduler: Arc<dyn SchedulerPort>,
    presence: Arc<dyn PresencePort>,
    settings: Mutex<StatusSettings>,
    job: Mutex<Option<JobHandle>>,
    /// Set by `stop`, cleared by `start`: a stopped plugin owns no scheduled
    /// work. Without this, a config-watcher tick after shutdown would
    /// re-schedule a live rotation on a stopped plugin.
    stopped: AtomicBool,
}

impl StatusRotatorPlugin {
    #[must_use]
    pub fn new(
        scheduler: Arc<dyn SchedulerPort>,
        presence: Arc<dyn PresencePort>,
        settings: StatusSettings,
    ) -> Self {
        Self {
            scheduler,
            presence,
            settings: Mutex::new(settings),
            job: Mutex::new(None),
            stopped: AtomicBool::new(false),
        }
    }

    /// Applies new settings: identical ones are a no-op (unrelated config
    /// changes must not reset the rotation), otherwise the running job is
    /// cancelled and rescheduled - or stopped on an empty list.
    pub fn update(&self, settings: StatusSettings) {
        {
            let mut current = self.settings.lock();
            if *current == settings {
                return;
            }
            *current = settings.clone();
        }

        tracing::info!(
            statuses = settings.statuses.len(),
            interval_secs = settings.interval.as_secs(),
            "applying new status rotation settings"
        );
        // A stopped plugin stores the settings but schedules nothing - the
        // next `start` picks them up. Runtime config ticks have no business
        // resurrecting a cancelled rotation.
        if self.stopped.load(Ordering::Acquire) {
            tracing::debug!("status rotator is stopped - settings stored without scheduling");
            return;
        }
        // Settings-then-job lock order - the same order `start` uses, so a
        // config change racing plugin start can neither double-schedule nor
        // deadlock. The check-and-install runs under one held lock, and the
        // stopped flag is RE-CHECKED under it: a `stop` landing between the
        // cheap check above and this lock must win - otherwise update would
        // resurrect a rotation the stop just cancelled.
        let mut job = self.job.lock();
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        if let Some(handle) = job.take() {
            handle.cancel();
        }
        *job = self.build_rotation(&settings);
    }

    /// Schedules the rotation job for the given settings; `None` = disabled
    /// (no statuses configured). Caller installs the handle under the job
    /// lock.
    fn build_rotation(&self, settings: &StatusSettings) -> Option<JobHandle> {
        if settings.statuses.is_empty() {
            tracing::info!("status rotator has no statuses - disabled");
            return None;
        }

        let job = Arc::new(StatusJob::new(Arc::clone(&self.presence), settings.statuses.clone()));
        Some(self.scheduler.schedule(self.name(), settings.interval, job))
    }
}

impl PluginPort for StatusRotatorPlugin {
    fn name(&self) -> &'static str {
        "status_rotator"
    }

    fn start(&self) -> Result<(), PluginError> {
        self.stopped.store(false, Ordering::Release);
        // An `update` landing between construction and start has already
        // scheduled the rotation - starting must not schedule a second job.
        // Settings-then-job lock order (same as `update`) makes the
        // check-and-install atomic against config changes.
        let settings = self.settings.lock().clone();
        let mut job = self.job.lock();
        if job.is_none() {
            *job = self.build_rotation(&settings);
        }
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        self.stopped.store(true, Ordering::Release);
        if let Some(handle) = self.job.lock().take() {
            handle.cancel();
        }
        Ok(())
    }
}

/// Draws the configured statuses from a shuffled deck, one per tick: the
/// deck refills and reshuffles (Fisher-Yates over `rand`'s thread generator)
/// whenever it runs empty, so every status appears exactly once per cycle,
/// in fake-random order - full coverage without a predictable sequence.
struct StatusJob {
    presence: Arc<dyn PresencePort>,
    statuses: Vec<String>,
    deck: Mutex<Vec<usize>>,
}

impl StatusJob {
    fn new(presence: Arc<dyn PresencePort>, statuses: Vec<String>) -> Self {
        Self { presence, statuses, deck: Mutex::new(Vec::new()) }
    }

    fn draw(&self) -> Option<usize> {
        let mut deck = self.deck.lock();
        if deck.is_empty() {
            *deck = (0..self.statuses.len()).collect();
            deck.shuffle(&mut rand::rng());
        }
        deck.pop()
    }
}

#[async_trait]
impl Job for StatusJob {
    async fn run(&self) {
        let Some(index) = self.draw() else {
            return;
        };
        let Some(status) = self.statuses.get(index) else {
            return;
        };

        if let Err(err) = self.presence.set(Presence::playing(status.clone())).await {
            tracing::warn!(%err, status = %status, "failed to update presence");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::OutboundError;
    use parking_lot::Mutex as PLMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // --- fakes ---

    struct FakeScheduler {
        scheduled: PLMutex<Vec<(String, Duration)>>,
        job: PLMutex<Option<Arc<dyn Job>>>,
        cancel_count: Arc<AtomicUsize>,
    }

    impl FakeScheduler {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                scheduled: PLMutex::new(Vec::new()),
                job: PLMutex::new(None),
                cancel_count: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn scheduled(&self) -> Vec<(String, Duration)> {
            self.scheduled.lock().clone()
        }
    }

    impl SchedulerPort for FakeScheduler {
        fn schedule(&self, name: &str, interval: Duration, job: Arc<dyn Job>) -> JobHandle {
            self.scheduled.lock().push((name.to_owned(), interval));
            *self.job.lock() = Some(job);

            let cancel_count = Arc::clone(&self.cancel_count);
            JobHandle::new(Arc::new(move || {
                cancel_count.fetch_add(1, Ordering::SeqCst);
            }))
        }
    }

    struct FakePresence {
        statuses: PLMutex<Vec<String>>,
        fail: bool,
    }

    impl FakePresence {
        fn new() -> Arc<Self> {
            Arc::new(Self { statuses: PLMutex::new(Vec::new()), fail: false })
        }

        fn failing() -> Arc<Self> {
            Arc::new(Self { statuses: PLMutex::new(Vec::new()), fail: true })
        }

        fn statuses(&self) -> Vec<String> {
            self.statuses.lock().clone()
        }
    }

    #[async_trait]
    impl PresencePort for FakePresence {
        async fn set(&self, presence: Presence) -> Result<(), OutboundError> {
            if self.fail {
                return Err(OutboundError::Send("gateway not ready".to_owned()));
            }
            if let Some(activity) = presence.activity {
                self.statuses.lock().push(activity.name);
            }
            Ok(())
        }
    }

    // --- fixtures ---

    fn settings(interval_secs: u64, statuses: &[&str]) -> StatusSettings {
        StatusSettings {
            interval: Duration::from_secs(interval_secs),
            statuses: statuses.iter().map(|status| (*status).to_owned()).collect(),
        }
    }

    fn plugin(
        scheduler: &Arc<FakeScheduler>,
        presence: &Arc<FakePresence>,
        statuses: Vec<&str>,
    ) -> StatusRotatorPlugin {
        StatusRotatorPlugin::new(
            Arc::clone(scheduler) as Arc<dyn SchedulerPort>,
            Arc::clone(presence) as Arc<dyn PresencePort>,
            settings(30, &statuses),
        )
    }

    // --- tests ---

    #[tokio::test]
    async fn start_schedules_job_with_configured_interval() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a", "b"]);

        plugin.start().expect("start expected to succeed");

        assert_eq!(
            scheduler.scheduled(),
            vec![("status_rotator".to_owned(), Duration::from_secs(30))]
        );
    }

    /// Deck semantics: two full cycles over three statuses draw each status
    /// exactly twice, and the first cycle covers every status once (a
    /// permutation, not a subsequence) - full coverage, shuffled order.
    #[tokio::test]
    async fn job_draws_all_statuses_once_per_deck_cycle() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a", "b", "c"]);
        plugin.start().expect("start expected to succeed");

        let job = scheduler.job.lock().clone().expect("job expected");
        for _ in 0..6 {
            job.run().await;
        }

        let drawn = presence.statuses();
        assert_eq!(drawn.len(), 6);
        for status in ["a", "b", "c"] {
            assert_eq!(
                drawn.iter().filter(|drawn| drawn == &status).count(),
                2,
                "status {status} must appear exactly once per cycle"
            );
        }
        let first_cycle = drawn.get(..3).expect("two full cycles drawn");
        let distinct: std::collections::HashSet<_> = first_cycle.iter().collect();
        assert_eq!(distinct.len(), 3, "a deck cycle is a permutation of the list");
    }

    #[tokio::test]
    async fn stop_cancels_scheduled_job_once() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a"]);

        plugin.start().expect("start expected to succeed");
        plugin.stop().expect("stop expected to succeed");
        plugin.stop().expect("second stop expected to succeed");

        assert_eq!(
            scheduler.cancel_count.load(Ordering::SeqCst),
            1,
            "cancelling twice must not double-cancel"
        );
    }

    #[tokio::test]
    async fn empty_status_list_never_schedules() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec![]);

        plugin.start().expect("start expected to succeed");

        assert!(scheduler.scheduled().is_empty());
        assert!(presence.statuses().is_empty());
    }

    /// Presence failures (gateway not ready yet) are logged and contained -
    /// the job survives and keeps the rotation going.
    #[tokio::test]
    async fn presence_failure_is_contained() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::failing();
        let plugin = plugin(&scheduler, &presence, vec!["a"]);
        plugin.start().expect("start expected to succeed");

        let job = scheduler.job.lock().clone().expect("job expected");
        job.run().await;
        job.run().await;

        assert!(presence.statuses().is_empty());
    }

    /// Hot reload: identical settings must be a no-op - unrelated config
    /// changes must not reset the running rotation.
    #[tokio::test]
    async fn update_to_same_settings_is_noop() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a"]);
        plugin.start().expect("start expected to succeed");

        plugin.update(settings(30, &["a"]));

        assert_eq!(scheduler.scheduled().len(), 1);
        assert_eq!(scheduler.cancel_count.load(Ordering::SeqCst), 0);
    }

    /// Hot reload: new interval and list cancel the old job and reschedule.
    #[tokio::test]
    async fn update_reschedules_with_new_interval_and_statuses() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a"]);
        plugin.start().expect("start expected to succeed");

        plugin.update(settings(60, &["x"]));

        assert_eq!(
            scheduler.scheduled(),
            vec![
                ("status_rotator".to_owned(), Duration::from_secs(30)),
                ("status_rotator".to_owned(), Duration::from_secs(60)),
            ]
        );
        assert_eq!(scheduler.cancel_count.load(Ordering::SeqCst), 1);

        let job = scheduler.job.lock().clone().expect("job expected");
        job.run().await;
        assert_eq!(presence.statuses(), ["x"]);
    }

    /// Hot reload: removing `[status]` (empty list) stops the rotation;
    /// re-adding it later re-enables it.
    #[tokio::test]
    async fn update_to_empty_stops_and_readd_reenables() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a"]);
        plugin.start().expect("start expected to succeed");

        plugin.update(StatusSettings::disabled());
        assert_eq!(scheduler.cancel_count.load(Ordering::SeqCst), 1);
        let scheduled_after_disable = scheduler.scheduled().len();

        plugin.update(settings(30, &["a"]));
        assert_eq!(scheduler.scheduled().len(), scheduled_after_disable + 1);
    }

    /// A config change landing between construction and `start` has already
    /// scheduled the rotation - starting must not schedule a second job.
    #[tokio::test]
    async fn update_before_start_does_not_double_schedule() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec![]);

        plugin.update(settings(45, &["x"]));
        plugin.start().expect("start expected to succeed");

        assert_eq!(
            scheduler.scheduled(),
            vec![("status_rotator".to_owned(), Duration::from_secs(45))]
        );
    }

    /// A stopped plugin owns no scheduled work: a post-shutdown config tick
    /// stores the new settings but must not resurrect the rotation.
    #[tokio::test]
    async fn update_after_stop_does_not_reschedule() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a"]);
        plugin.start().expect("start expected to succeed");
        plugin.stop().expect("stop expected to succeed");
        let scheduled_at_stop = scheduler.scheduled().len();

        plugin.update(settings(60, &["x"]));

        assert_eq!(
            scheduler.scheduled().len(),
            scheduled_at_stop,
            "a stopped plugin must not schedule new work"
        );
        assert_eq!(
            scheduler.cancel_count.load(Ordering::SeqCst),
            1,
            "nothing new was scheduled, so nothing new was cancelled"
        );

        // A fresh lifecycle (kernel reboot in tests) resumes from the
        // stored settings.
        plugin.start().expect("restart expected to succeed");
        assert_eq!(scheduler.scheduled().len(), scheduled_at_stop + 1);
    }
}
