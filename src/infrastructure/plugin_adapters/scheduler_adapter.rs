use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;

use crate::common::panic_message;
use crate::kernel::plugin_ports::{Job, JobHandle, SchedulerPort};

/// Tokio-backed [`SchedulerPort`]: one background task per job, ticking on a
/// fixed interval (first run immediate). Panicking jobs are logged and keep
/// being scheduled - the same failure policy as the pipeline and the bus.
#[derive(Clone, Default)]
pub struct TokioScheduler;

impl TokioScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl SchedulerPort for TokioScheduler {
    fn schedule(&self, name: &str, interval: Duration, job: Arc<dyn Job>) -> JobHandle {
        // A zero period would panic `tokio::time::interval` inside the spawned
        // task - refuse to spawn and hand back an already-cancelled handle.
        if interval.is_zero() {
            tracing::error!(job = %name, "zero interval rejected - job never scheduled");
            let token = CancellationToken::new();
            token.cancel();
            return JobHandle::new(Arc::new(move || token.cancel()));
        }

        let token = CancellationToken::new();
        let shutdown = token.clone();
        let job_name = name.to_owned();

        tokio::spawn(async move {
            // tokio intervals tick immediately on the first call.
            let mut ticker = tokio::time::interval(interval);
            // A delayed poll job must not burst-catch-up after a slow run.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                if shutdown.is_cancelled() {
                    break;
                }

                let run = std::panic::AssertUnwindSafe(job.run()).catch_unwind().await;
                if let Err(panic) = run {
                    tracing::error!(
                        job = %job_name,
                        panic = panic_message(&panic),
                        "scheduled job panicked"
                    );
                }

                if shutdown.is_cancelled() {
                    break;
                }
            }
        });

        JobHandle::new(Arc::new(move || token.cancel()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;

    struct CountingJob(Arc<AtomicUsize>);

    #[async_trait]
    impl Job for CountingJob {
        async fn run(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A zero interval cannot drive a ticker (`tokio::time::interval` would
    /// panic inside the spawned task): the adapter must return a dead handle
    /// without spawning - the job never runs, cancelling stays a no-op.
    #[tokio::test]
    async fn zero_interval_yields_dead_handle_without_running() {
        let scheduler = TokioScheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let handle = scheduler.schedule(
            "dead_job",
            Duration::ZERO,
            Arc::new(CountingJob(Arc::clone(&counter))),
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(counter.load(Ordering::SeqCst), 0, "a dead job must never run");
        handle.cancel(); // cancelling a dead handle must not panic
    }
}
