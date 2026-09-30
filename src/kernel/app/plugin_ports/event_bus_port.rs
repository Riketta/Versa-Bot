use std::sync::Arc;

use crate::kernel::models::Event;

/// Handler subscribed to a specific event type on the bus.
pub trait EventHandler<E: Event + 'static>: Send + Sync {
    fn handle(&self, event: &E);
}

/// Kernel-owned pub/sub bus for runtime plugin-to-plugin messaging. Plugins
/// publish/subscribe to events they own; the kernel routes but never defines
/// event meanings. It never carries raw inbound events - a middleware plugin
/// that processed an inbound event MAY publish a DERIVED domain event onto it.
///
/// Cardinality: a single active adapter, a single instance per kernel, shared
/// with plugins via injection (the adapter is `Clone`; each plugin receives
/// its own clone at construction).
pub trait EventBusPort: Send + Sync + 'static {
    fn publish(&self, event: Arc<dyn Event>);

    fn subscribe<E: Event + 'static>(&self, handler: Arc<dyn EventHandler<E>>);
}
