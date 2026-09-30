use std::sync::Arc;

use parking_lot::RwLock;

use crate::common::panic_message;
use crate::kernel::spi_ports::{ConfigChangeHandler, ConfigPort};

/// Polling [`ConfigPort`]: periodically rebuilds the configuration via the
/// provided closure and notifies subscribers when the new snapshot differs
/// from the previous one. A failed reload (e.g. a half-written file) keeps
/// the last good configuration. Subscriber handlers run inline and are
/// panic-isolated - one broken subscriber cannot block the others.
pub struct PollingConfigWatcher<C: PartialEq + Send + Sync + 'static> {
    reload: Box<dyn Fn() -> anyhow::Result<C> + Send + Sync>,
    state: RwLock<Option<Arc<C>>>,
    subscribers: RwLock<Vec<Arc<dyn ConfigChangeHandler<C>>>>,
}

impl<C: PartialEq + Send + Sync + 'static> PollingConfigWatcher<C> {
    #[must_use]
    pub fn new(reload: impl Fn() -> anyhow::Result<C> + Send + Sync + 'static) -> Self {
        Self {
            reload: Box::new(reload),
            state: RwLock::new(None),
            subscribers: RwLock::new(Vec::new()),
        }
    }

    /// Seeds the boot-time snapshot without notifying anyone.
    pub fn seed(&self, config: C) {
        *self.state.write() = Some(Arc::new(config));
    }

    /// One reload-and-notify pass. Runs on the scheduler; public for tests.
    pub fn poll(&self) {
        let snapshot = match (self.reload)() {
            Ok(config) => Arc::new(config),
            Err(err) => {
                tracing::warn!(%err, "config reload failed - keeping last good configuration");
                return;
            }
        };

        {
            let state = self.state.read();
            if let Some(previous) = state.as_ref() {
                if **previous == *snapshot {
                    return;
                }
            }
        }

        tracing::info!("configuration changed");
        *self.state.write() = Some(Arc::clone(&snapshot));

        for handler in self.subscribers.read().iter() {
            // Panic isolation: same contract as the event bus - a broken
            // subscriber must not block the others.
            let delivery = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler.on_change(Arc::clone(&snapshot));
            }));
            if let Err(panic) = delivery {
                tracing::error!(panic = panic_message(&panic), "config change handler panicked");
            }
        }
    }
}

impl<C: PartialEq + Send + Sync + 'static> ConfigPort<C> for PollingConfigWatcher<C> {
    fn subscribe(&self, handler: Arc<dyn ConfigChangeHandler<C>>) {
        self.subscribers.write().push(handler);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, PartialEq)]
    struct TestConfig {
        value: u32,
    }

    struct Recorder(Arc<Mutex<Vec<u32>>>);

    impl ConfigChangeHandler<TestConfig> for Recorder {
        fn on_change(&self, config: Arc<TestConfig>) {
            self.0.lock().push(config.value);
        }
    }

    struct PanickingHandler;

    impl ConfigChangeHandler<TestConfig> for PanickingHandler {
        fn on_change(&self, _config: Arc<TestConfig>) {
            panic!("handler exploded");
        }
    }

    struct Fixture {
        current: Arc<Mutex<u32>>,
        reload_should_fail: Arc<AtomicUsize>,
        watcher: PollingConfigWatcher<TestConfig>,
        received: Arc<Mutex<Vec<u32>>>,
    }

    fn fixture() -> Fixture {
        let current = Arc::new(Mutex::new(1));
        let reload_should_fail = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));

        let current_reload = Arc::clone(&current);
        let fail_reload = Arc::clone(&reload_should_fail);
        let watcher = PollingConfigWatcher::new(move || {
            if fail_reload.load(Ordering::SeqCst) > 0 {
                return Err(anyhow::anyhow!("simulated reload failure"));
            }
            Ok(TestConfig { value: *current_reload.lock() })
        });
        watcher.seed(TestConfig { value: 1 });
        watcher.subscribe(Arc::new(Recorder(Arc::clone(&received))));

        Fixture { current, reload_should_fail, watcher, received }
    }

    #[test]
    fn poll_without_change_does_not_notify() {
        let fixture = fixture();

        fixture.watcher.poll(); // value still 1 - same as seeded

        assert!(fixture.received.lock().is_empty());
    }

    #[test]
    fn poll_notifies_exactly_once_per_change() {
        let fixture = fixture();

        *fixture.current.lock() = 2;
        fixture.watcher.poll();
        fixture.watcher.poll(); // no further change

        *fixture.current.lock() = 3;
        fixture.watcher.poll();

        assert_eq!(*fixture.received.lock(), vec![2, 3]);
    }

    #[test]
    fn failed_reload_keeps_last_good_snapshot() {
        let fixture = fixture();
        *fixture.current.lock() = 2;
        fixture.reload_should_fail.store(1, Ordering::SeqCst);

        fixture.watcher.poll(); // fails - nothing changes

        assert!(fixture.received.lock().is_empty());

        fixture.reload_should_fail.store(0, Ordering::SeqCst);
        fixture.watcher.poll(); // reloads 2 - now it differs

        assert_eq!(*fixture.received.lock(), vec![2]);
    }

    #[test]
    fn panicking_handler_does_not_block_others() {
        let fixture = fixture();
        *fixture.current.lock() = 2;
        fixture
            .watcher
            .subscribe(Arc::new(PanickingHandler) as Arc<dyn ConfigChangeHandler<TestConfig>>);

        fixture.watcher.poll(); // reloads 2 - both handlers run, one panics

        assert_eq!(*fixture.received.lock(), vec![2]);
    }
}
