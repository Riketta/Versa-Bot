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

/// Rotates the bot's presence through a configured status list on a fixed
/// interval (`PluginPort`-only - it is not part of the inbound pipeline).
///
/// Both the list and the interval come from the bot configuration
/// (`[status]` section): presence is a global concern, not a per-guild one.
/// An empty list disables the plugin - `start` schedules nothing. The job
/// runs on the kernel scheduler (first run immediate); `stop` cancels it.
pub struct StatusRotatorPlugin {
    scheduler: Arc<dyn SchedulerPort>,
    presence: Arc<dyn PresencePort>,
    interval: Duration,
    statuses: Vec<String>,
    job: Mutex<Option<JobHandle>>,
}

impl StatusRotatorPlugin {
    #[must_use]
    pub fn new(
        scheduler: Arc<dyn SchedulerPort>,
        presence: Arc<dyn PresencePort>,
        interval: Duration,
        statuses: Vec<String>,
    ) -> Self {
        Self { scheduler, presence, interval, statuses, job: Mutex::new(None) }
    }
}

impl PluginPort for StatusRotatorPlugin {
    fn name(&self) -> &'static str {
        "status_rotator"
    }

    fn start(&self) -> Result<(), PluginError> {
        if self.statuses.is_empty() {
            tracing::info!("status rotator has no statuses - disabled");
            return Ok(());
        }

        let job = Arc::new(StatusJob {
            presence: Arc::clone(&self.presence),
            statuses: self.statuses.clone(),
            index: AtomicUsize::new(0),
        });
        let handle = self.scheduler.schedule(self.name(), self.interval, job);
        *self.job.lock() = Some(handle);
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

    fn plugin(
        scheduler: &Arc<FakeScheduler>,
        presence: &Arc<FakePresence>,
        statuses: Vec<&str>,
    ) -> StatusRotatorPlugin {
        StatusRotatorPlugin::new(
            Arc::clone(scheduler) as Arc<dyn SchedulerPort>,
            Arc::clone(presence) as Arc<dyn PresencePort>,
            Duration::from_secs(30),
            statuses.into_iter().map(ToOwned::to_owned).collect(),
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
}
