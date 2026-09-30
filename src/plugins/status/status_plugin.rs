use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

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
/// list. The job runs on the kernel scheduler (first run immediate); `stop`
/// cancels it.
pub struct StatusRotatorPlugin {
    scheduler: Arc<dyn SchedulerPort>,
    presence: Arc<dyn PresencePort>,
    settings: Mutex<StatusSettings>,
    job: Mutex<Option<JobHandle>>,
}

impl StatusRotatorPlugin {
    #[must_use]
    pub fn new(
        scheduler: Arc<dyn SchedulerPort>,
        presence: Arc<dyn PresencePort>,
        settings: StatusSettings,
    ) -> Self {
        Self { scheduler, presence, settings: Mutex::new(settings), job: Mutex::new(None) }
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
        if let Some(handle) = self.job.lock().take() {
            handle.cancel();
        }
        self.schedule_rotation(settings);
    }

    fn schedule_rotation(&self, settings: StatusSettings) {
        if settings.statuses.is_empty() {
            tracing::info!("status rotator has no statuses - disabled");
            return;
        }

        let job = Arc::new(StatusJob {
            presence: Arc::clone(&self.presence),
            statuses: settings.statuses,
            index: AtomicUsize::new(0),
        });
        let handle = self.scheduler.schedule(self.name(), settings.interval, job);
        *self.job.lock() = Some(handle);
    }
}

impl PluginPort for StatusRotatorPlugin {
    fn name(&self) -> &'static str {
        "status_rotator"
    }

    fn start(&self) -> Result<(), PluginError> {
        let settings = self.settings.lock().clone();
        self.schedule_rotation(settings);
        Ok(())
    }

    fn stop(&self) -> Result<(), PluginError> {
        if let Some(handle) = self.job.lock().take() {
            handle.cancel();
        }
        Ok(())
    }
}

/// Cycles through the configured statuses, one per tick.
struct StatusJob {
    presence: Arc<dyn PresencePort>,
    statuses: Vec<String>,
    index: AtomicUsize,
}

#[async_trait]
impl Job for StatusJob {
    async fn run(&self) {
        let count = self.statuses.len();
        if count == 0 {
            return;
        }
        let index = self.index.fetch_add(1, Ordering::Relaxed) % count;
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

    #[tokio::test]
    async fn job_rotates_through_statuses_in_order() {
        let scheduler = FakeScheduler::new();
        let presence = FakePresence::new();
        let plugin = plugin(&scheduler, &presence, vec!["a", "b"]);
        plugin.start().expect("start expected to succeed");

        let job = scheduler.job.lock().clone().expect("job expected");
        job.run().await;
        job.run().await;
        job.run().await;

        assert_eq!(presence.statuses(), ["a", "b", "a"]);
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
}
