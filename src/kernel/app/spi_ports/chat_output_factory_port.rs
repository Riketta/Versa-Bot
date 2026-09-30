use std::sync::Arc;

use super::ChatOutputPort;
use crate::kernel::models::Origin;

/// Driven port the kernel calls per event to obtain outbound ports bound to
/// that event's origin. Implemented by the driving adapter (e.g. the Discord
/// gateway adapter), which owns the platform connection; the kernel stays
/// platform-agnostic.
pub trait ChatOutputFactoryPort: Send + Sync + 'static {
    fn chat_output(&self, origin: &Origin) -> Arc<dyn ChatOutputPort>;
}
