//! Plugin-owned derived event: published once per (guild, tracker kind) that
//! produced an announcement. Bus-only plugins may react to store activity
//! without joining any pipeline; the bus contract is the type import only.

use crate::{bus_event, kernel::models::GuildId};

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

crate::bus_event! {
    /// Store updates were announced to a guild's assigned channel. Fields
    /// are payload for future bus subscribers (the bus contract is the type
    /// import, not field reads - see `audit_log` -> `tracker`); none exists
    /// yet, hence the allow.
    #[allow(dead_code)]
    pub struct LolStoreAnnounced {
        pub guild_id: GuildId,
        pub kind: StoreEventKind,
    }
    => ("lol_store", "announced");
}
