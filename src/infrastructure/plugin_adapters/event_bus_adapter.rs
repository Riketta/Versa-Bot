use std::{any::TypeId, collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::common::panic_message;
use crate::kernel::{
    models::Event,
    plugin_ports::{EventBusPort, EventHandler},
};

type ErasedHandler = Arc<dyn Fn(&dyn Event) + Send + Sync>;

/// The single active `EventBusPort` adapter: in-memory, topic-keyed by event
/// `TypeId`. One instance per kernel, `Clone`d into every plugin at
/// construction. Runtime contract: handlers run inline on the publishing
/// task, in subscription order - keep them fast and non-blocking. A
/// panicking handler is caught, logged, and skipped: a broken subscriber
/// cannot crash the publisher, the pipeline, or other subscribers. Delivery
/// isolation, cross-task ordering, and backpressure (external broker) are
/// deliberately deferred until a real consumer needs them.
#[derive(Clone, Default)]
pub struct InMemoryEventBus {
    subscribers: Arc<RwLock<HashMap<TypeId, Vec<ErasedHandler>>>>,
}

impl InMemoryEventBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventBusPort for InMemoryEventBus {
    fn publish(&self, event: Arc<dyn Event>) {
        let handlers = {
            let subscribers = self.subscribers.read();
            subscribers.get(&event.as_any().type_id()).cloned().unwrap_or_default()
        };

        for handler in handlers {
            // Panic isolation: handlers get only read access to the event,
            // so recovering from a unwind cannot leave the bus or the event
            // in a corrupted state.
            let delivery =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event.as_ref())));
            if let Err(panic) = delivery {
                tracing::error!(
                    event = event.name(),
                    panic = panic_message(&panic),
                    "event bus handler panicked"
                );
            }
        }
    }

    fn subscribe<E: Event + 'static>(&self, handler: Arc<dyn EventHandler<E>>) {
        let erased: ErasedHandler = Arc::new(move |event: &dyn Event| {
            if let Some(typed) = event.as_any().downcast_ref::<E>() {
                handler.handle(typed);
            }
        });

        self.subscribers.write().entry(TypeId::of::<E>()).or_default().push(erased);
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct PingPublished;

    impl Event for PingPublished {
        fn name(&self) -> &'static str {
            "ping_published"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct UnheardEvent;

    impl Event for UnheardEvent {
        fn name(&self) -> &'static str {
            "unheard_event"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct CountingHandler(Arc<AtomicUsize>);

    impl EventHandler<PingPublished> for CountingHandler {
        fn handle(&self, _event: &PingPublished) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PanickingHandler;

    impl EventHandler<PingPublished> for PanickingHandler {
        fn handle(&self, _event: &PingPublished) {
            panic!("subscriber exploded");
        }
    }

    /// Contract: a broken subscriber must not crash the publisher or other
    /// subscribers - the panic is caught, logged, and skipped.
    #[test]
    fn panicking_handler_is_isolated() {
        let bus = InMemoryEventBus::new();
        bus.subscribe(Arc::new(PanickingHandler));
        let counter = Arc::new(AtomicUsize::new(0));
        bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));

        bus.publish(Arc::new(PingPublished));
        bus.publish(Arc::new(PingPublished));

        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "healthy subscriber must be unaffected by the panicking one"
        );
    }

    #[test]
    fn routes_events_to_subscribers_by_type() {
        let bus = InMemoryEventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));

        bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));

        bus.publish(Arc::new(PingPublished));
        bus.publish(Arc::new(UnheardEvent));
        bus.publish(Arc::new(PingPublished));

        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "only the subscribed event type reaches the handler"
        );
    }
}
