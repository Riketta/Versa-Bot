//! Plugin-owned derived event: published once per (guild, tracker kind) that
//! produced an announcement. Bus-only plugins may react to store activity
//! without joining any pipeline; the bus contract is the type import only.

use std::any::Any;

use crate::kernel::models::GuildId;

/// Which tracker produced the announcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreEventKind {
    Sales,
    NewSkins,
    MythicRotation,
    YourShop,
}

impl StoreEventKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sales => "sales",
            Self::NewSkins => "new_skins",
            Self::MythicRotation => "mythic_rotation",
            Self::YourShop => "yourshop",
        }
    }
}

/// Store updates were announced to a guild's assigned channel. Fields are
/// payload for future bus subscribers (the bus contract is the type import,
/// not field reads - see `audit_log` -> `tracker`); none exists yet, hence
/// the allow.
#[allow(dead_code)]
pub struct LolStoreAnnounced {
    pub guild_id: GuildId,
    pub kind: StoreEventKind,
}

impl crate::kernel::models::Event for LolStoreAnnounced {
    fn name(&self) -> &'static str {
        "lol_store.announced"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
