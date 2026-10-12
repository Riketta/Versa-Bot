use std::{
    any::TypeId,
    collections::HashMap,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::RwLock;

use crate::common::panic_message;
use crate::kernel::{
    models::{BusEvent, Event},
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

/// Catalog entry for an event type ever registered on this bus: qualified
/// name and the Rust type path for diagnostics. Append-only and
/// process-lifetime (bounded by distinct event types, like per-channel
/// plugin state) - name uniqueness is checked against it.
struct RegisteredEvent {
    name: &'static str,
    type_path: &'static str,
}

/// Subscription bookkeeping, shared by every bus clone so a cancellation
/// handle can clear exactly its own slot: per event type, `None` marks an
/// unsubscribed handler. The catalog never shrinks - it only records types.
#[derive(Default)]
struct Inner {
    subscribers: RwLock<HashMap<TypeId, Vec<Option<Slot>>>>,
    catalog: RwLock<HashMap<TypeId, RegisteredEvent>>,
}

/// The single active `EventBusPort` adapter: in-memory, topic-keyed by event
/// `TypeId`. One instance per kernel, `Clone`d into every plugin at
/// construction. Runtime contract: handlers run inline on the publishing
/// task, in subscription order - keep them fast and non-blocking. A
/// panicking handler is caught, logged, and skipped: a broken subscriber
/// cannot crash the publisher, the pipeline, or other subscribers. Delivery
/// isolation, cross-task ordering, and backpressure (external broker) are
/// deliberately deferred until a real consumer needs them.
///
/// Every published or subscribed type joins the bus catalog (logged at
/// `info`, matching the command registration trail); duplicate event names
/// fail fast there.
#[derive(Clone, Default)]
pub struct InMemoryEventBus {
    inner: Arc<Inner>,
}

impl InMemoryEventBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the event type to the process-lifetime catalog, fail-fast on a
    /// name collision. Delivery itself is `TypeId`-keyed and could never
    /// misroute between same-named types - uniqueness keeps the event
    /// registry and its audit trail unambiguous (and defends a future
    /// name-keyed broker). A collision aborts the boot when it surfaces at
    /// subscription time (plugin `init()`); on first publish it fails the
    /// publishing event (the pipeline logs and drops it - fail closed).
    /// Re-registering an already catalogued type is a no-op (stop/`init`
    /// cycling, repeated publishes).
    fn register_event<E: BusEvent>(catalog: &RwLock<HashMap<TypeId, RegisteredEvent>>) {
        let type_id = TypeId::of::<E>();
        let mut catalog = catalog.write();
        if catalog.contains_key(&type_id) {
            return;
        }
        if let Some(clash) = catalog.values().find(|known| known.name == E::EVENT_NAME) {
            panic!(
                "bus event name `{}` is claimed by both `{}` and `{}` - event names must be unique",
                E::EVENT_NAME,
                clash.type_path,
                ::core::any::type_name::<E>()
            );
        }
        catalog.insert(
            type_id,
            RegisteredEvent { name: E::EVENT_NAME, type_path: ::core::any::type_name::<E>() },
        );
        tracing::info!(
            plugin = E::EVENT_SLUG,
            event = E::EVENT_NAME,
            type = ::core::any::type_name::<E>(),
            "bus event type registered"
        );
    }
}

impl EventBusPort for InMemoryEventBus {
    fn publish<E: BusEvent>(&self, event: E) {
        // First publish of a type joins the catalog too - a read-lock probe
        // keeps the common (already-registered) path cheap.
        if !self.inner.catalog.read().contains_key(&TypeId::of::<E>()) {
            Self::register_event::<E>(&self.inner.catalog);
        }

        // Snapshot the live handlers, skipping unsubscribed (`None`) slots.
        let handlers: Vec<ErasedHandler> = {
            let subscribers = self.inner.subscribers.read();
            subscribers
                .get(&TypeId::of::<E>())
                .map(|slots| slots.iter().flatten().map(|slot| Arc::clone(&slot.handler)).collect())
                .unwrap_or_default()
        };

        // No Arc: the event outlives the inline dispatch on this task's
        // stack, handlers get only read access (`&E` unsizes to `&dyn
        // Event`), so a unwind recovery cannot corrupt the bus or the event.
        for handler in handlers {
            let delivery =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&event)));
            if let Err(panic) = delivery {
                tracing::error!(
                    event = E::EVENT_NAME,
                    panic = panic_message(&panic),
                    "event bus handler panicked"
                );
            }
        }
    }

    fn subscribe<E: BusEvent + 'static>(
        &self,
        handler: Arc<dyn EventHandler<E>>,
    ) -> EventBusSubscription {
        let erased: ErasedHandler = Arc::new(move |event: &dyn Event| {
            if let Some(typed) = event.as_any().downcast_ref::<E>() {
                handler.handle(typed);
            }
        });

        let type_id = TypeId::of::<E>();
        Self::register_event::<E>(&self.inner.catalog);
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

    use parking_lot::Mutex;

    use super::*;

    struct PingPublished;

    impl BusEvent for PingPublished {
        const EVENT_SLUG: &'static str = "test";
        const EVENT_NAME: &'static str = "test.ping_published";
    }

    impl Event for PingPublished {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct UnheardEvent;

    impl BusEvent for UnheardEvent {
        const EVENT_SLUG: &'static str = "test";
        const EVENT_NAME: &'static str = "test.unheard_event";
    }

    impl Event for UnheardEvent {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    /// Second type claiming `test.ping_published` - the name collides with
    /// [`PingPublished`] although the types differ.
    struct NameCollisionEvent;

    impl BusEvent for NameCollisionEvent {
        const EVENT_SLUG: &'static str = "test";
        const EVENT_NAME: &'static str = "test.ping_published";
    }

    impl Event for NameCollisionEvent {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct NoopHandler;

    impl EventHandler<NameCollisionEvent> for NoopHandler {
        fn handle(&self, _event: &NameCollisionEvent) {}
    }

    /// Appends its label to a shared log - the fixture that makes WHEN a
    /// handler ran observable, unlike position-blind counters.
    struct OrderedHandler {
        label: &'static str,
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl EventHandler<PingPublished> for OrderedHandler {
        fn handle(&self, _event: &PingPublished) {
            self.log.lock().push(self.label);
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

        bus.publish(PingPublished);
        bus.publish(PingPublished);

        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "healthy subscriber must be unaffected by the panicking one"
        );
    }

    /// Contract: handlers run inline on the publishing task, in subscription
    /// order - a publish delivers both handlers, first subscriber first.
    #[test]
    fn handlers_run_in_subscription_order() {
        let bus = InMemoryEventBus::new();
        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

        bus.subscribe(Arc::new(OrderedHandler { label: "first", log: Arc::clone(&log) }));
        bus.subscribe(Arc::new(OrderedHandler { label: "second", log: Arc::clone(&log) }));

        bus.publish(PingPublished);

        assert_eq!(*log.lock(), vec!["first", "second"], "handlers must run in subscription order");
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

        bus.publish(PingPublished);

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

        bus.publish(PingPublished);
        bus.publish(UnheardEvent);
        bus.publish(PingPublished);

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

        bus.publish(PingPublished);
        detach.unsubscribe();
        bus.publish(PingPublished);

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

        bus.publish(PingPublished);

        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "the surviving subscriber must be unaffected by the double unsubscribe"
        );
    }

    /// Contract: `<slug>.<event>` names are process-unique - a second event
    /// type claiming a taken name must fail loudly at registration time
    /// (which runs in plugin `init()` for subscribers, so this aborts the
    /// boot) instead of polluting the registry and its audit trail.
    #[test]
    fn duplicate_event_name_fails_fast() {
        let bus = InMemoryEventBus::new();
        bus.subscribe(Arc::new(CountingHandler(Arc::new(AtomicUsize::new(0)))));

        let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bus.subscribe(Arc::new(NoopHandler))
        })) else {
            panic!("a duplicate event name must panic");
        };
        let message = panic_message(&panic);
        assert!(
            message.contains("test.ping_published"),
            "panic must name the colliding event: {message}"
        );
        assert!(
            message.contains("NameCollisionEvent"),
            "panic must name the second type: {message}"
        );
    }

    /// Contract: `stop()`/`init()` cycling (bus-only plugins unsubscribe in
    /// `stop()`, resubscribe in `init()`) re-registers the same type without
    /// a name collision, and delivery resumes.
    #[test]
    fn resubscribing_a_type_does_not_collide() {
        let bus = InMemoryEventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let detach = bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));
        detach.unsubscribe();

        let _resubscribed = bus.subscribe(Arc::new(CountingHandler(Arc::clone(&counter))));

        bus.publish(PingPublished);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "delivery must resume after the resubscription"
        );
    }
}
