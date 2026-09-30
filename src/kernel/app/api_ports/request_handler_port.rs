use std::sync::Arc;

use async_trait::async_trait;

use crate::kernel::models::RequestContext;

/// Inbound driving port: the entry point driving adapters call.
/// Fire-and-forget - no response is returned. Plugins produce output via
/// event-scoped outbound ports; an event no plugin handles simply yields
/// no output.
#[async_trait]
pub trait RequestHandlerPort: Send + Sync + 'static {
    async fn handle(&self, event: RequestContext);
}

#[async_trait]
impl<H: RequestHandlerPort + ?Sized> RequestHandlerPort for Arc<H> {
    async fn handle(&self, event: RequestContext) {
        (**self).handle(event).await;
    }
}
