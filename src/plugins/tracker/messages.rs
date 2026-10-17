//! Capture of recent human messages and the audit notices for their edits
//! and deletions. The cache is the only honest source for the "before"
//! side: platform deletion events carry no text, and an edit notice that
//! cannot quote the original must say so instead of inventing one.

use std::collections::{HashMap, VecDeque};

use parking_lot::Mutex;

use crate::kernel::models::{ChannelId, GuildId, MessageId};

/// Upper bound on quoted message text per notice side. Clamped so a fully
/// loaded notice (two fenced blocks plus framing) still fits the platform's
/// message limit with headroom.
pub const MAX_NOTICE_CLIP: usize = 900;

/// Default per-side text cap for edit/deletion notices (the `[tracker]`
/// `message_notice_limit` config value). The plugin clamps into
/// [`MAX_NOTICE_CLIP`] regardless of what is configured.
pub const DEFAULT_NOTICE_LIMIT: usize = 500;

/// Bounded FIFO of recently seen human messages, keyed per guild/channel/
/// message. Process-lifetime and capped by count ([`CACHE_CAP`]), so memory
/// scales with capture volume, not uptime - same class as the plugin's
/// other in-memory state.
pub struct MessageCache {
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    entries: HashMap<MessageKey, CachedMessage>,
    order: VecDeque<MessageKey>,
}

/// Cache coordinates of one captured message.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageKey {
    pub guild: GuildId,
    pub channel: ChannelId,
    pub message: MessageId,
}

impl MessageKey {
    #[must_use]
    pub fn new(guild: GuildId, channel: ChannelId, message: MessageId) -> Self {
        Self { guild, channel, message }
    }
}

/// What the tracker knows about a captured message: who wrote it and what
/// it said at last capture.
#[derive(Clone)]
pub struct CachedMessage {
    pub author_name: Option<String>,
    pub content: String,
}

/// Capture volume cap: the newest 4096 messages are the edit/deletion
/// horizon - older messages fall out and their notices degrade to the
/// "not seen" shape.
const CACHE_CAP: usize = 4096;

impl MessageCache {
    #[must_use]
    pub fn new() -> Self {
        Self { inner: Mutex::new(CacheInner::default()) }
    }

    /// Records a fresh message - the original text every later edit or
    /// deletion notice quotes. A duplicate key re-captures (and refreshes
    /// its recency).
    pub fn remember(&self, key: MessageKey, author_name: Option<String>, content: String) {
        let mut inner = self.inner.lock();
        inner.entries.insert(key, CachedMessage { author_name, content });
        inner.order.retain(|seen| *seen != key);
        inner.order.push_back(key);
        while inner.order.len() > CACHE_CAP {
            if let Some(evicted) = inner.order.pop_front() {
                inner.entries.remove(&evicted);
            }
        }
    }

    /// Replaces the stored text of an edited message and returns what it
    /// said before - the "was" side of the edit notice. `None` means the
    /// original predates capture; the new text is captured either way so a
    /// later edit diffs against this one.
    pub fn revise(
        &self,
        key: MessageKey,
        author_name: Option<String>,
        content: String,
    ) -> Option<String> {
        let mut inner = self.inner.lock();
        let previous = inner.entries.insert(key, CachedMessage { author_name, content });
        inner.order.retain(|seen| *seen != key);
        inner.order.push_back(key);
        previous.map(|message| message.content)
    }

    /// Removes a deleted message, returning what was captured for it.
    pub fn forget(&self, key: MessageKey) -> Option<CachedMessage> {
        let mut inner = self.inner.lock();
        inner.order.retain(|seen| *seen != key);
        inner.entries.remove(&key)
    }
}

impl Default for MessageCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-side text clip for notice quotes: whole characters, with an ellipsis
/// marking the cut.
#[must_use]
pub fn clipped(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let cut: String = text.chars().take(limit).collect();
    format!("{cut}…")
}

/// Fences text so whitespace and markdown stay visible in the notice. The
/// fence grows past any backtick run inside the text (never fewer than
/// three), so content containing code blocks cannot break out.
#[must_use]
fn fenced(text: &str) -> String {
    let mut longest = 2;
    let mut run = 0;
    for ch in text.chars() {
        if ch == '`' {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    let fence = "`".repeat(longest + 1);
    format!("{fence}\n{text}\n{fence}")
}

/// Discord-shaped jump link for a message (platform-FACT exception while a
/// single deployment serves one chat provider; other adapters swap this).
#[must_use]
fn jump_url(key: MessageKey) -> String {
    format!("https://discord.com/channels/{}/{}/{}", key.guild, key.channel, key.message)
}

/// The edit notice: who edited what where, quoting the previous text when
/// it was captured and saying so when it was not.
#[must_use]
pub fn edit_notice(
    limit: usize,
    author: &str,
    key: MessageKey,
    previous: Option<&str>,
    current: &str,
) -> String {
    let clip = limit.clamp(1, MAX_NOTICE_CLIP);
    let mut out =
        format!("✏️ **{author}** edited a message in <#{}>: {}\n", key.channel, jump_url(key));
    match previous {
        Some(previous) => {
            out.push_str("was:\n");
            out.push_str(&fenced(&clipped(previous, clip)));
            out.push_str("\nnow:\n");
            out.push_str(&fenced(&clipped(current, clip)));
        }
        None => {
            out.push_str("(original not seen - sent before capture or bot restart)\nnow:\n");
            out.push_str(&fenced(&clipped(current, clip)));
        }
    }
    out
}

/// The deletion notice for a message the tracker had captured.
#[must_use]
pub fn delete_notice(limit: usize, author: &str, key: MessageKey, content: &str) -> String {
    let clip = limit.clamp(1, MAX_NOTICE_CLIP);
    format!(
        "🗑 **{author}** deleted a message in <#{}>: {}\nwas:\n{}",
        key.channel,
        jump_url(key),
        fenced(&clipped(content, clip))
    )
}

/// The deletion notice when nothing was captured: the platform names only
/// which message vanished.
#[must_use]
pub fn delete_unknown_notice(key: MessageKey) -> String {
    format!(
        "🗑 a message was deleted in <#{}> (not seen - sent before capture or bot restart)",
        key.channel
    )
}

/// The purge notice: a mass deletion reports only a count.
#[must_use]
pub fn purge_notice(count: usize, channel: ChannelId) -> String {
    format!("🗑️ {count} messages were purged in <#{channel}>")
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_NOTICE_CLIP, MessageCache, MessageKey, clipped, delete_notice, delete_unknown_notice,
        edit_notice, fenced, jump_url, purge_notice,
    };
    use crate::kernel::models::{ChannelId, GuildId, MessageId};

    fn key() -> MessageKey {
        MessageKey::new(GuildId(1), ChannelId(20), MessageId(300))
    }

    #[test]
    fn clip_keeps_short_text_whole_and_marks_long_cuts() {
        assert_eq!(clipped("hello", 10), "hello");
        let long = "x".repeat(50);
        assert_eq!(clipped(&long, 10), format!("{}…", "x".repeat(10)));
    }

    #[test]
    fn fence_widens_past_embedded_backtick_runs() {
        assert!(fenced("plain").starts_with("```\n"));
        // Three backticks inside the text force a four-backtick fence, so
        // the content cannot terminate the block early.
        let text = "code ``` block";
        let fenced_text = fenced(text);
        assert!(fenced_text.starts_with("````\n"));
        assert!(fenced_text.ends_with("\n````"));
        assert_eq!(fenced_text.matches("````").count(), 2);
    }

    #[test]
    fn notices_name_channel_author_and_jump_link() {
        let edit = edit_notice(500, "Riketta", key(), Some("old"), "new");
        assert!(edit.contains("✏️ **Riketta** edited a message in <#20>"), "edit: {edit}");
        assert!(edit.contains(&jump_url(key())), "edit: {edit}");
        assert!(edit.contains("was:"), "edit: {edit}");
        assert!(edit.contains("now:"), "edit: {edit}");

        let unknown = edit_notice(500, "Riketta", key(), None, "new");
        assert!(
            unknown.contains("(original not seen - sent before capture or bot restart)"),
            "unknown: {unknown}"
        );

        let deletion = delete_notice(500, "Riketta", key(), "old text");
        assert!(
            deletion.contains("🗑 **Riketta** deleted a message in <#20>"),
            "delete: {deletion}"
        );
        assert!(deletion.contains("old text"), "delete: {deletion}");

        let unknown_deletion = delete_unknown_notice(key());
        assert!(
            unknown_deletion.contains("a message was deleted in <#20>"),
            "unknown delete: {unknown_deletion}"
        );

        let purge = purge_notice(7, ChannelId(20));
        assert_eq!(purge, "🗑️ 7 messages were purged in <#20>");
    }

    #[test]
    fn notice_clip_never_exceeds_the_platform_headroom_cap() {
        let long = "y".repeat(MAX_NOTICE_CLIP * 2);
        let edit = edit_notice(MAX_NOTICE_CLIP * 10, "a", key(), Some(&long), &long);
        // Both sides clamped: the whole notice stays under the 2000-char
        // platform limit even for an absurd configured limit.
        assert!(edit.chars().count() < 2000, "notice too long: {}", edit.chars().count());
    }

    #[test]
    fn jump_link_matches_the_discord_shape() {
        assert_eq!(jump_url(key()), "https://discord.com/channels/1/20/300");
    }

    #[test]
    fn cache_round_trips_remember_revise_and_forget() {
        let cache = MessageCache::new();
        let k = key();

        assert_eq!(cache.revise(k, Some("a".to_owned()), "one".to_owned()), None);
        cache.remember(k, Some("a".to_owned()), "one".to_owned());
        assert_eq!(cache.revise(k, Some("a".to_owned()), "two".to_owned()), Some("one".to_owned()));
        let removed = cache.forget(k).expect("captured message expected");
        assert_eq!(removed.content, "two");
        assert!(cache.forget(k).is_none());
    }

    #[test]
    fn cache_evicts_oldest_beyond_the_cap() {
        let cache = MessageCache::new();
        // Insert more than the cap; the oldest key must fall out. The cap
        // is a private const - rely on ordering: the first-inserted key is
        // the first evicted, so after many inserts only the newest survive.
        let first = MessageKey::new(GuildId(1), ChannelId(1), MessageId(1));
        cache.remember(first, None, "first".to_owned());
        for i in 0..5_000u64 {
            let k = MessageKey::new(GuildId(1), ChannelId(1), MessageId(10 + i));
            cache.remember(k, None, format!("m{i}"));
        }
        assert!(cache.forget(first).is_none(), "oldest entry must have been evicted");
        let last = MessageKey::new(GuildId(1), ChannelId(1), MessageId(10 + 4_999));
        assert_eq!(cache.forget(last).map(|m| m.content), Some("m4999".to_owned()));
    }
}
