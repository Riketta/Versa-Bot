//! Platform-agnostic ID newtypes. The kernel never depends on a specific
//! chat platform; driving adapters map native IDs onto these.

macro_rules! strong_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(pub u64);

        impl $name {
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

strong_id!(
    /// Chat platform guild (server) ID.
    GuildId
);
strong_id!(
    /// Chat channel ID.
    ChannelId
);
strong_id!(
    /// Chat user ID.
    UserId
);
strong_id!(
    /// Chat message ID.
    MessageId
);
