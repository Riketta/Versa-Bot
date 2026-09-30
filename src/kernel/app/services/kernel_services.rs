use std::sync::Arc;

use crate::kernel::app::spi_ports::ChatOutputPort;

/// Kernel-owned service context injected into pipeline hooks. Rebuilt per
/// event: `chat_output` is bound to the event's origin via
/// `ChatOutputFactoryPort`. Global services (storage, config, ...) join here
/// as `Arc` fields when their ports land.
#[derive(Clone)]
pub struct KernelServices {
    pub chat_output: Arc<dyn ChatOutputPort>,
}
