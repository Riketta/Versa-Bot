//! Kernel-owned configuration service: exposes configuration hot-reload to
//! plugins. Generic over the configuration type - the kernel never depends
//! on the concrete infrastructure config; the composition root wires the
//! concrete instance.

use std::sync::Arc;

/// Kernel-owned service port: subscribes to configuration changes.
pub trait ConfigPort<C: Send + Sync + 'static>: Send + Sync + 'static {
    /// Registers a handler invoked on every configuration change. Handlers
    /// run inline on the watcher (keep them fast and non-blocking) and are
    /// panic-isolated like bus subscribers. A change is delivered only when
    /// the new snapshot differs from the previous one.
    fn subscribe(&self, handler: Arc<dyn ConfigChangeHandler<C>>);
}

/// Reacts to a configuration change, receiving the new snapshot. The
/// subscriber decides which parts are relevant and how to apply them.
pub trait ConfigChangeHandler<C: Send + Sync + 'static>: Send + Sync {
    fn on_change(&self, config: Arc<C>);
}
