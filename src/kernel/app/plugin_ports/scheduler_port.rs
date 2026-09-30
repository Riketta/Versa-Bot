//! Kernel-owned scheduling service: runs recurring jobs for plugins on the
//! runtime adapter's background tasks. Injected into plugins at
//! construction (same pattern as the event bus); the kernel and plugins
//! never see the concrete runtime.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

/// A unit of recurring work. Runs on the scheduler's runtime; a panicking
/// `run` is contained by the scheduler (logged, job keeps being scheduled) -
/// the same failure policy as the pipeline and the bus.
#[async_trait]
pub trait Job: Send + Sync + 'static {
    async fn run(&self);
}

/// Handle to a scheduled job: cancelling stops future runs; a run in flight
/// completes. Cloning shares the same underlying job.
#[derive(Clone)]
pub struct JobHandle {
    cancel: Arc<dyn Fn() + Send + Sync>,
}

impl JobHandle {
    pub(crate) fn new(cancel: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self { cancel }
    }

    pub fn cancel(&self) {
        (self.cancel)();
    }
}

/// Kernel-owned service port: schedules recurring jobs. The first run is
/// immediate, then every `interval` - a plugin that wants a delay before
/// the first run encodes it itself.
pub trait SchedulerPort: Send + Sync + 'static {
    /// Schedules `job` under `name` (used for log attribution). Cancel via
    /// the returned handle - plugins do so in `stop()`.
    fn schedule(&self, name: &str, interval: Duration, job: Arc<dyn Job>) -> JobHandle;
}
