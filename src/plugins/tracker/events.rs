use crate::{
    bus_event,
    kernel::models::{ChannelId, GuildId, MessageId, UserId},
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

crate::bus_event! {
    /// Domain event published by the tracker when a tracked human message's
    /// text content changed. `old_content` is the previously captured text -
    /// `None` when the original was never seen (bot restart, message sent
    /// before capture started). A no-op edit (content unchanged) publishes
    /// nothing.
    #[derive(Debug, Clone)]
    pub struct MessageEdited {
        pub guild_id: GuildId,
        pub channel_id: ChannelId,
        pub message_id: MessageId,
        pub user_id: UserId,
        pub author_name: Option<String>,
        pub old_content: Option<String>,
        pub new_content: String,
    }
    => ("tracker", "message_edited");
}

crate::bus_event! {
    /// Domain event published by the tracker when a tracked message is
    /// deleted. The text and author ride along only when the message was
    /// still in the tracker's capture cache; `user_id` is `UserId(0)` when
    /// the platform does not identify the author.
    #[derive(Debug, Clone)]
    pub struct MessageDeleted {
        pub guild_id: GuildId,
        pub channel_id: ChannelId,
        pub message_id: MessageId,
        pub user_id: UserId,
        pub author_name: Option<String>,
        pub content: Option<String>,
    }
    => ("tracker", "message_deleted");
}

crate::bus_event! {
    /// Domain event published by the tracker when many messages are removed
    /// at once (mass delete / purge). Only the count is known.
    #[derive(Debug, Clone)]
    pub struct MessagePurged {
        pub guild_id: GuildId,
        pub channel_id: ChannelId,
        pub count: usize,
    }
    => ("tracker", "message_purged");
}

#[cfg(test)]
mod tests {
    use super::{MessageDeleted, MessageEdited, MessagePurged, UserJoinedGuild, UserLeftGuild};
    use crate::kernel::models::BusEvent;

    /// Pins the macro-derived identity: qualified `<slug>.<event>` names are
    /// the bus contract (uniqueness registry and audit trail). A `concat!`
    /// regression here must fail a test, not only surface in logs.
    #[test]
    fn event_names_are_plugin_scoped() {
        assert_eq!(UserJoinedGuild::EVENT_SLUG, "tracker");
        assert_eq!(UserJoinedGuild::EVENT_NAME, "tracker.user_joined_guild");
        assert_eq!(UserLeftGuild::EVENT_NAME, "tracker.user_left_guild");
        assert_eq!(MessageEdited::EVENT_NAME, "tracker.message_edited");
        assert_eq!(MessageDeleted::EVENT_NAME, "tracker.message_deleted");
        assert_eq!(MessagePurged::EVENT_NAME, "tracker.message_purged");
    }
}
