use std::any::Any;

use crate::kernel::models::{Event, GuildId, UserId};

/// Domain event published by the tracker when a member joins a guild.
/// Plugin-owned (per the pipeline<->bus bridge): the kernel routes it, but
/// its meaning belongs to this plugin and its subscribers.
#[derive(Debug, Clone)]
pub struct UserJoinedGuild {
    pub guild_id: GuildId,
    pub user_id: UserId,
    pub username: Option<String>,
}

impl Event for UserJoinedGuild {
    fn name(&self) -> &'static str {
        "user_joined_guild"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Domain event published by the tracker when a member leaves a guild
/// (leave, kick, or ban - the platform does not distinguish).
#[derive(Debug, Clone)]
pub struct UserLeftGuild {
    pub guild_id: GuildId,
    pub user_id: UserId,
    pub username: Option<String>,
}

impl Event for UserLeftGuild {
    fn name(&self) -> &'static str {
        "user_left_guild"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
