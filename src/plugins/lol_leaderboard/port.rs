//! The leaderboard data-source boundary - this plugin's own hexagon edge.
//! [`LeaderboardSourcePort`] is what the engine consumes; the `DeepLoL`
//! adapter ([`super::deeplol`]) is the one implementation. DTOs are
//! provider-neutral and hole-friendly: aggregate statistics must survive a
//! source that reports a player without a role or without champion data.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// The lane a leaderboard entry is associated with. Provider role strings
/// are mapped onto this enum at the source boundary - the stats and format
/// layers never see raw strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Top,
    Jungle,
    Mid,
    Bot,
    Support,
}

impl Role {
    /// Every role, in display order.
    pub const ALL: [Role; 5] = [Self::Top, Self::Jungle, Self::Mid, Self::Bot, Self::Support];

    /// Human-facing label (matches common leaderboard wording).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Top => "Top",
            Self::Jungle => "Jungle",
            Self::Mid => "Middle",
            Self::Bot => "Bot",
            Self::Support => "Supporter",
        }
    }

    /// Lenient parse of a provider lane string: case-insensitive, accepts
    /// common spellings. Unknown lanes yield `None`, which simply drops the
    /// player from the statistics - it must never be an error.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "top" => Some(Self::Top),
            "jungle" | "jng" | "jug" => Some(Self::Jungle),
            "middle" | "mid" => Some(Self::Mid),
            "bot" | "bottom" | "adc" => Some(Self::Bot),
            "support" | "supporter" | "utility" | "sup" => Some(Self::Support),
            _ => None,
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// One leaderboard entry, reduced to what the statistics need. `position`
/// is the dense 1-based rank within the region (renumbered after sorting -
/// provider ranks can be sparse when players are hidden).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderboardPlayer {
    pub position: u32,
    /// The player's primary role; `None` when the source does not tell.
    pub role: Option<Role>,
    /// Provider champion id of the player's most-played champion
    /// (e.g. `"12"`); `None` when the source has no champion data.
    pub champ_id: Option<String>,
}

/// A region's parsed leaderboard slice, already sorted best-first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionLeaderboard {
    /// Neutral region key as configured (e.g. `kr`).
    pub region: String,
    pub players: Vec<LeaderboardPlayer>,
}

/// Everything that can go wrong at the source boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    /// The source is unreachable, errored, or misconfigured (proxy).
    Request(String),
    /// The answer could not be understood (schema drift).
    Parse(String),
    /// The region key is not served by this source.
    UnknownRegion(String),
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(detail) => write!(f, "source request failed: {detail}"),
            Self::Parse(detail) => write!(f, "source answer not understood: {detail}"),
            Self::UnknownRegion(region) => write!(f, "unknown region: {region}"),
        }
    }
}

impl std::error::Error for SourceError {}

/// The data source behind `/lol_leaderboard`. One active adapter per
/// engine, injected at the composition root.
#[async_trait]
pub trait LeaderboardSourcePort: Send + Sync {
    /// Region keys this source can serve (neutral, lowercase).
    fn known_regions(&self) -> &'static [&'static str];

    /// The `depth` highest-ranked players of `region`. Implementations may
    /// return fewer players than requested (the source's board can be
    /// shorter); they must return them sorted best-first with dense
    /// positions.
    async fn leaderboard(&self, region: &str, depth: u32)
    -> Result<RegionLeaderboard, SourceError>;

    /// Champion id -> display name for the stats layer. Ids missing from
    /// the map degrade to a `Champion #id` placeholder downstream.
    async fn champion_names(&self) -> Result<HashMap<String, String>, SourceError>;
}
