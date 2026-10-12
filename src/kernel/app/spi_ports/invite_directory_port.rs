use async_trait::async_trait;

use crate::kernel::models::{ChannelId, Origin, UserId};

/// One invite of the origin guild's directory: the code, its creator when
/// the platform reports one, and the usage metadata the join-attribution
/// diff consumes (`uses` is the cumulative join count through the code -
/// the only join-attribution signal platforms like Discord expose).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryInvite {
    pub code: String,
    /// Channel the invite points at, when reported.
    pub channel_id: Option<ChannelId>,
    /// Invite creator, when reported.
    pub creator_id: Option<UserId>,
    /// Creator display name (best effort).
    pub creator_name: Option<String>,
    /// Cumulative times the code was used; `None` when the platform does
    /// not expose usage counts (join attribution then has no signal).
    pub uses: Option<u64>,
    /// Maximum use count (`0` = unlimited on platforms with the concept).
    pub max_uses: Option<u64>,
}

/// Driven port over the origin guild's invite directory, as the platform
/// sees it right now. One listing per call serves the tracker's join
/// attribution (uses-count diff against stored observations) - adapters
/// cache or rate-limit behind this contract as they see fit.
///
/// Empty results cover every degradation - platforms without invite
/// concepts, origins outside any guild, and permission-denied listings
/// (Discord's directory read needs Manage Server): attribution degrades,
/// never fails. Callers treat emptiness as "no signal", not "no invites".
#[async_trait]
pub trait InviteDirectoryPort: Send + Sync + 'static {
    /// The guild's currently live invites. Cheap to call: adapters own the
    /// caching/rate-limit policy behind it.
    async fn invites(&self, origin: &Origin) -> Vec<DirectoryInvite>;
}

/// Terminal default for deployments whose platform has no invite directory:
/// every `invites` returns empty.
pub struct EmptyInviteDirectoryPort;

#[async_trait]
impl InviteDirectoryPort for EmptyInviteDirectoryPort {
    async fn invites(&self, _origin: &Origin) -> Vec<DirectoryInvite> {
        Vec::new()
    }
}
