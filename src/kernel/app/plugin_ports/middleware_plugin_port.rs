use crate::kernel::{models::RequestContext, plugin_ports::PluginPort, services::KernelServices};
use async_trait::async_trait;

/// Payload-free pipeline control signal.
pub enum Next {
    /// Proceed to the next plugin's `pre`.
    Continue,
    /// Short-circuit: skip remaining `pre` hooks; `post` still runs for the
    /// plugins that ran.
    Stop,
    /// Hard stop: skip remaining `pre` hooks AND all `post` hooks.
    Abort,
}

/// Per-plugin pipeline step contract, NOT the pipeline itself - the pipeline
/// runner is `KernelService`'s `RequestHandlerPort` implementation.
///
/// `pre` is the forward hook (may short-circuit the chain);
/// `post` is the backward hook for observability/cleanup and receives the
/// event read-only - outputs are fire-and-forget, there is nothing to rewrite.
///
/// Every plugin implements `PluginPort` (identity + lifecycle); middleware
/// participation is this opt-in supertrait. The same `Arc` goes into the
/// kernel's `plugins` list and `middleware` list.
#[async_trait]
pub trait MiddlewarePluginPort: PluginPort + Send + Sync + 'static {
    async fn pre(&self, _event: &mut RequestContext, _services: &KernelServices) -> Next {
        Next::Continue
    }

    async fn post(&self, _event: &RequestContext, _services: &KernelServices) {}
}
