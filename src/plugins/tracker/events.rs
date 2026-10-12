use crate::{
    bus_event,
    kernel::models::{GuildId, UserId},
};

crate::bus_event! {
    /// Domain event published by the tracker when a member joins a guild.
    /// Plugin-owned (per the pipeline<->bus bridge): the kernel routes it,
    /// but its meaning belongs to this plugin and its subscribers.
    #[derive(Debug, Clone)]
    pub struct UserJoinedGuild {
        pub guild_id: GuildId,
        pub user_id: UserId,
        pub username: Option<String>,
    }
    => ("tracker", "user_joined_guild");
}

crate::bus_event! {
    /// Domain event published by the tracker when a member leaves a guild
    /// (leave, kick, or ban - the platform does not distinguish).
    #[derive(Debug, Clone)]
    pub struct UserLeftGuild {
        pub guild_id: GuildId,
        pub user_id: UserId,
        pub username: Option<String>,
    }
    => ("tracker", "user_left_guild");
}

#[cfg(test)]
mod tests {
    use super::{UserJoinedGuild, UserLeftGuild};
    use crate::kernel::models::BusEvent;

    /// Pins the macro-derived identity: qualified `<slug>.<event>` names are
    /// the bus contract (uniqueness registry and audit trail). A `concat!`
    /// regression here must fail a test, not only surface in logs.
    #[test]
    fn event_names_are_plugin_scoped() {
        assert_eq!(UserJoinedGuild::EVENT_SLUG, "tracker");
        assert_eq!(UserJoinedGuild::EVENT_NAME, "tracker.user_joined_guild");
        assert_eq!(UserLeftGuild::EVENT_NAME, "tracker.user_left_guild");
    }
}
