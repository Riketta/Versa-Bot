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
        let token = CancellationToken::new();
        let shutdown = token.clone();
        let job_name = name.to_owned();

        tokio::spawn(async move {
            // tokio intervals tick immediately on the first call.
            let mut ticker = tokio::time::interval(interval);
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
