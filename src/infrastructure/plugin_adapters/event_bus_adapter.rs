use std::{
    any::TypeId,
    collections::HashMap,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::RwLock;

use crate::common::panic_message;
use crate::kernel::{
    models::Event,
    plugin_ports::{EventBusPort, EventBusSubscription, EventHandler},
};

type ErasedHandler = Arc<dyn Fn(&dyn Event) + Send + Sync>;

/// One subscription slot: the handler plus the unique id its cancellation
/// handle owns. Clearing matches on the id, not the position - after an
/// entry's removal (all slots gone) a stale handle's index would otherwise
/// point into a newer subscription's vector.
struct Slot {
    id: u64,
    handler: ErasedHandler,
}

/// Ids are process-lifetime unique, so a stale handle can never collide
/// with a live slot, whatever the subscribe/unsubscribe interleaving.
static NEXT_SUBSCRIPTION_ID: AtomicU64 = AtomicU64::new(1);

/// Subscription bookkeeping, shared by every bus clone so a cancellation
/// handle can clear exactly its own slot: per event type, `None` marks an
/// unsubscribed handler.
#[derive(Default)]
struct Inner {
    subscribers: RwLock<HashMap<TypeId, Vec<Option<Slot>>>>,
}

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
    inner: Arc<Inner>,
}

impl InMemoryEventBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventBusPort for InMemoryEventBus {
    fn publish(&self, event: Arc<dyn Event>) {
        // Snapshot the live handlers, skipping unsubscribed (`None`) slots.
        let handlers: Vec<ErasedHandler> = {
            let subscribers = self.inner.subscribers.read();
            subscribers
                .get(&event.as_any().type_id())
                .map(|slots| slots.iter().flatten().map(|slot| Arc::clone(&slot.handler)).collect())
                .unwrap_or_default()
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

    fn subscribe<E: Event + 'static>(
        &self,
        handler: Arc<dyn EventHandler<E>>,
    ) -> EventBusSubscription {
        let erased: ErasedHandler = Arc::new(move |event: &dyn Event| {
            if let Some(typed) = event.as_any().downcast_ref::<E>() {
                handler.handle(typed);
            }
        });

        let type_id = TypeId::of::<E>();
        let id = NEXT_SUBSCRIPTION_ID.fetch_add(1, Ordering::Relaxed);
        {
            let mut subscribers = self.inner.subscribers.write();
            let slots = subscribers.entry(type_id).or_default();
            slots.push(Some(Slot { id, handler: erased }));
        }
        let inner = Arc::clone(&self.inner);

        // The handle owns the shared bookkeeping and clears exactly its own
        // slot, matched by subscription id - not by position: after the
        // entry's removal below, a stale handle's index would point into a
        // newer subscription's vector. A second unsubscribe finds nothing
        // with its id and is a true no-op.
        EventBusSubscription::new(Arc::new(move || {
            let mut subscribers = inner.subscribers.write();
            let mut remove_type = false;
            if let Some(slots) = subscribers.get_mut(&type_id) {
                if let Some(slot) =
                    slots.iter_mut().find(|slot| slot.as_ref().is_some_and(|live| live.id == id))
                {
                    *slot = None;
                }
                // Compact when the last subscriber for this event type is
                // gone - slot vectors would otherwise grow without bound
                // under subscribe/unsubscribe cycling. Removal is safe: it
                // happens only when no live id for the type remains.
                remove_type = slots.iter().all(Option::is_none);
            }
            if remove_type {
                subscribers.remove(&type_id);
            }
        }))
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

    /// Contract: unsubscribe is idempotent per the port doc - and a stale
    /// handle whose subscription was already removed must never cancel a
    /// newer subscriber that reused the same slot position.
    #[test]
    fn stale_handle_cannot_cancel_a_newer_subscription() {
        let bus = InMemoryEventBus::new();
        let stale_counter = Arc::new(AtomicUsize::new(0));
        let stale = bus.subscribe(Arc::new(CountingHandler(Arc::clone(&stale_counter))));
        stale.unsubscribe();

        let live_counter = Arc::new(AtomicUsize::new(0));
        let _live = bus.subscribe(Arc::new(CountingHandler(Arc::clone(&live_counter))));

        // The stale handle's position points at the live subscription's
        // slot; the second call must be a no-op, not a cancellation.
        stale.unsubscribe();

        bus.publish(Arc::new(PingPublished));

        assert_eq!(stale_counter.load(Ordering::SeqCst), 0);
        assert_eq!(
            live_counter.load(Ordering::SeqCst),
            1,
            "the stale handle must not cancel the newer subscription"
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

    /// Contract: an unsubscribed handler stops receiving events, while other
    /// subscribers of the same event type keep receiving them.
    #[test]
    fn unsubscribe_stops_delivery() {
        let bus = InMemoryEventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let detach = bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));
        bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));

        bus.publish(Arc::new(PingPublished));
        detach.unsubscribe();
        bus.publish(Arc::new(PingPublished));

        assert_eq!(
            counter.load(Ordering::SeqCst),
            3,
            "unsubscribed handler must stop receiving; the other must keep counting"
        );
    }

    /// Contract: unsubscribing twice is a no-op - the second call neither
    /// panics nor affects the remaining subscribers.
    #[test]
    fn double_unsubscribe_is_noop() {
        let bus = InMemoryEventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let detach = bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));
        bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));

        detach.unsubscribe();
        detach.unsubscribe();

        bus.publish(Arc::new(PingPublished));

        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "the surviving subscriber must be unaffected by the double unsubscribe"
        );
    }
}
