use std::sync::Arc;

use crate::kernel::app::spi_ports::{ChatOutputPort, GuildStorage};

/// Kernel-owned service context injected into pipeline hooks. Rebuilt per
/// event: `chat_output` is bound to the event's origin via
/// `ChatOutputFactoryPort`; `guild_storage` is bound to the event's origin
/// guild via `StoragePort` and is `None` for direct messages. Global services
/// (config, scheduler, ...) join here as `Arc` fields when their ports land.
#[derive(Clone)]
pub struct KernelServices {
    pub chat_output: Arc<dyn ChatOutputPort>,
    pub guild_storage: Option<Arc<dyn GuildStorage>>,
}
