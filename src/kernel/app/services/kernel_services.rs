use std::sync::Arc;

use crate::kernel::app::spi_ports::{
    ChatOutputFactoryPort, ChatOutputPort, GuildStorage, PlatformInfoPort,
};

/// Kernel-owned service context injected into pipeline hooks. Rebuilt per
/// event: `chat_output` is bound to the event's origin via
/// `ChatOutputFactoryPort`; `guild_storage` is bound to the event's origin
/// guild via `StoragePort` and is `None` for direct messages. The factory
/// itself rides along so a plugin can additionally obtain a port bound to a
/// configured channel of the same guild (`channel_output`), and the
/// deployment's platform identity rides along for prompt templates and
/// slug-keyed scoping. Global services (config, scheduler, ...) join here
/// as `Arc` fields when their ports land.
#[derive(Clone)]
pub struct KernelServices {
    pub chat_output: Arc<dyn ChatOutputPort>,
    pub chat_output_factory: Arc<dyn ChatOutputFactoryPort>,
    pub guild_storage: Option<Arc<dyn GuildStorage>>,
    pub platform_info: Arc<dyn PlatformInfoPort>,
}
