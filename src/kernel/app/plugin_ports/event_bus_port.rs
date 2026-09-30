use std::sync::Arc;

use crate::kernel::models::Event;

/// Handler subscribed to a specific event type on the bus.
pub trait EventHandler<E: Event + 'static>: Send + Sync {
    fn handle(&self, event: &E);
}

/// Cancellation handle for a bus subscription. Unsubscribed handlers stop
/// receiving events from the bus; dropping the handle does NOT unsubscribe -
/// call [`EventBusSubscription::unsubscribe`] explicitly (same style as
/// `JobHandle`: cloning shares the same subscription, dropping is inert).
#[derive(Clone)]
pub struct EventBusSubscription {
    cancel: Arc<dyn Fn() + Send + Sync>,
}

impl EventBusSubscription {
    pub(crate) fn new(cancel: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self { cancel }
    }

    /// Detaches the handler: it stops receiving events. Idempotent - a
    /// second call is a no-op.
    pub fn unsubscribe(&self) {
        (self.cancel)();
    }
}

/// Kernel-owned pub/sub bus for runtime plugin-to-plugin messaging. Plugins
/// publish/subscribe to events they own; the kernel routes but never defines
/// event meanings. It never carries raw inbound events - a middleware plugin
/// that processed an inbound event MAY publish a DERIVED domain event onto it.
///
/// Cardinality: a single active adapter, a single instance per kernel, shared
/// with plugins via injection (the adapter is `Clone`; each plugin receives
/// its own clone at construction).
///
/// Runtime contract: handlers run inline on the publishing task, in
/// subscription order - they must stay fast and non-blocking; implementations
/// must isolate panicking handlers instead of letting the unwind escape.
pub trait EventBusPort: Send + Sync + 'static {
    fn publish(&self, event: Arc<dyn Event>);

    fn subscribe<E: Event + 'static>(
        &self,
        handler: Arc<dyn EventHandler<E>>,
    ) -> EventBusSubscription;
}
