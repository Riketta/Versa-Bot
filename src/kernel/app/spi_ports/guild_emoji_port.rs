use async_trait::async_trait;
use std::sync::Arc;

/// One custom emoji of the origin's guild: a stable human key (`name`) and
/// the platform's exact wire-form reaction token (`token`). Kernel-neutral:
/// no platform vocabulary beyond what the adapter puts into the strings -
/// plugins filter on `name` and render `token` without parsing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactableEmoji {
    pub name: String,
    pub token: String,
}

/// Port bound to the origin event's guild: the custom emojis reactable
/// there, as exact platform wire forms. For plugins that offer the emoji
/// set to a model as reactable choices. Platforms without custom emojis
/// and origins outside any guild yield the undeliverable default. Cheap
/// for callers: adapters cache the guild listing.
#[async_trait]
pub trait GuildEmojiPort: Send + Sync {
    /// The guild's custom emojis. An empty result covers both "no custom
    /// emojis" and "listing failed" - a failure is the adapter's log
    /// concern and must never fail the caller's flow.
    async fn list(&self) -> Vec<ReactableEmoji>;
}

/// Terminal default for origins with no emoji listing (no guild, platform
/// without custom emojis): every `list` returns empty.
pub struct UndeliverableGuildEmojiPort;

#[async_trait]
impl GuildEmojiPort for UndeliverableGuildEmojiPort {
    async fn list(&self) -> Vec<ReactableEmoji> {
        Vec::new()
    }
}
