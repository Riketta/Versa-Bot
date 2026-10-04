//! The one [`LeaderboardSourcePort`] adapter: `DeepLoL`'s public CDN API
//! (`https://b2c-api-cdn.deeplol.gg`, no auth). Endpoint field guide and
//! live-verified quirks live in this plugin's README.
//!
//! Pacing: requests are strictly sequential with the configured interval
//! between them - the source is someone else's website. Pagination clamps
//! to the board's own `total_page` declaration, so a `parse_depth` beyond
//! what exists simply parses what exists.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::port::{LeaderboardPlayer, LeaderboardSourcePort, RegionLeaderboard, Role, SourceError};

/// Root of the public CDN API.
pub const DEFAULT_BASE_URL: &str = "https://b2c-api-cdn.deeplol.gg";

/// Per-request HTTP timeout: generous enough for a cold CDN edge, small
/// enough that a hung page cannot stall a command for minutes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Neutral region key -> provider platform code. The provider uses Riot's
/// digitless codes (`KR`, not `KR1`) - the single most important gotcha of
/// this API.
pub fn provider_region(region: &str) -> Option<&'static str> {
    match region {
        "kr" => Some("KR"),
        "euw" => Some("EUW"),
        "eun" => Some("EUN"),
        "na" => Some("NA"),
        "jp" => Some("JP"),
        "br" => Some("BR"),
        "tr" => Some("TR"),
        "tw" => Some("TW"),
        "vn" => Some("VN"),
        "sea" => Some("SEA"),
        _ => None,
    }
}

/// Subset of `GET /summoner/summoner_rank` - only the fields consumed.
/// Everything is defaulted: schema drift degrades one field, never the
/// whole parse.
#[derive(Debug, Deserialize)]
struct RankPage {
    #[serde(rename = "summoner_rank_list", default)]
    players: Vec<RankEntry>,
    #[serde(rename = "total_page", default)]
    total_page: u32,
}

#[derive(Debug, Deserialize)]
struct RankEntry {
    #[serde(default)]
    rank: u32,
    #[serde(rename = "most_role", default)]
    most_role: String,
    #[serde(rename = "most_champion", default)]
    most_champion: Vec<String>,
}

/// Subset of `GET /common/champion-info`.
#[derive(Debug, Deserialize)]
struct ChampionInfo {
    #[serde(default)]
    champions: Vec<ChampionEntry>,
}

#[derive(Debug, Deserialize)]
struct ChampionEntry {
    #[serde(rename = "champion_id", default)]
    id: String,
    #[serde(rename = "champion_name_en", default)]
    name: String,
}

/// Maps raw entries onto neutral players: sort by provider rank, cut to
/// `depth`, renumber positions densely. Pure and unit-tested - the HTTP
/// layer only ferries JSON.
fn to_region(region: &str, mut entries: Vec<RankEntry>, depth: u32) -> RegionLeaderboard {
    entries.sort_by_key(|entry| entry.rank);
    let players = entries
        .into_iter()
        .take(depth.max(1) as usize)
        .enumerate()
        .map(|(index, entry)| LeaderboardPlayer {
            position: u32::try_from(index).expect("board length fits u32") + 1,
            role: Role::parse(&entry.most_role),
            champ_id: entry.most_champion.into_iter().next().filter(|id| !id.is_empty()),
        })
        .collect();
    RegionLeaderboard { region: region.to_owned(), players }
}

/// Maps the champion-info answer onto the id -> name map, skipping empty
/// entries.
fn to_champion_names(info: ChampionInfo) -> HashMap<String, String> {
    info.champions
        .into_iter()
        .filter(|entry| !entry.id.is_empty() && !entry.name.is_empty())
        .map(|entry| (entry.id, entry.name))
        .collect()
}

pub struct DeepLolSource {
    http: reqwest::Client,
    base_url: String,
    interval: Duration,
}

impl DeepLolSource {
    /// Builds the HTTP client. A proxy the client cannot parse fails here,
    /// at boot - not at first request.
    ///
    /// # Errors
    /// When the proxy URL is not parseable or the client cannot be built.
    pub fn new(proxy: Option<&str>, interval: Duration) -> Result<Self, SourceError> {
        Self::with_base_url(proxy, interval, DEFAULT_BASE_URL.to_owned())
    }

    /// Test/alternative-deployment seam: same client against another root.
    ///
    /// # Errors
    /// Same as [`Self::new`].
    pub fn with_base_url(
        proxy: Option<&str>,
        interval: Duration,
        base_url: String,
    ) -> Result<Self, SourceError> {
        let mut builder = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (compatible; versa-bot leaderboard)")
            .timeout(REQUEST_TIMEOUT);
        if let Some(proxy) = proxy {
            let proxy = reqwest::Proxy::all(proxy)
                .map_err(|err| SourceError::Request(format!("proxy: {err}")))?;
            builder = builder.proxy(proxy);
        }
        let http =
            builder.build().map_err(|err| SourceError::Request(format!("http client: {err}")))?;
        Ok(Self { http, base_url, interval })
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, SourceError> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|err| SourceError::Request(format!("{url}: {err}")))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| SourceError::Request(format!("{url}: reading body: {err}")))?;
        if !status.is_success() {
            return Err(SourceError::Request(format!("{url}: HTTP {status}: {body}")));
        }
        serde_json::from_str(&body).map_err(|err| SourceError::Parse(format!("{url}: {err}")))
    }
}

#[async_trait]
impl LeaderboardSourcePort for DeepLolSource {
    fn known_regions(&self) -> &'static [&'static str] {
        &["kr", "euw", "eun", "na", "jp", "br", "tr", "tw", "vn", "sea"]
    }

    async fn leaderboard(
        &self,
        region: &str,
        depth: u32,
    ) -> Result<RegionLeaderboard, SourceError> {
        let code =
            provider_region(region).ok_or_else(|| SourceError::UnknownRegion(region.to_owned()))?;
        let depth = depth.max(1);

        // Sequential pages until the depth is reached or the board ends at
        // its own declared `total_page` (a board reporting fewer pages than
        // the depth needs simply yields a shorter parse).
        let mut entries: Vec<RankEntry> = Vec::new();
        let mut page = 1;
        loop {
            let path = format!("/summoner/summoner_rank?platform_id={code}&lane=All&page={page}");
            let rank_page: RankPage = self.get_json(&path).await?;
            let total_page = rank_page.total_page.max(page);
            entries.extend(rank_page.players);
            if entries.len() >= depth as usize || page >= total_page {
                break;
            }
            page += 1;
            tokio::time::sleep(self.interval).await;
        }

        Ok(to_region(region, entries, depth))
    }

    async fn champion_names(&self) -> Result<HashMap<String, String>, SourceError> {
        let info: ChampionInfo = self.get_json("/common/champion-info").await?;
        Ok(to_champion_names(info))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(rank: u32, role: &str, champs: &[&str]) -> RankEntry {
        RankEntry {
            rank,
            most_role: role.to_owned(),
            most_champion: champs.iter().map(|id| (*id).to_owned()).collect(),
        }
    }

    #[test]
    fn region_parse_maps_known_keys_to_digitless_codes() {
        assert_eq!(provider_region("kr"), Some("KR"));
        assert_eq!(provider_region("euw"), Some("EUW"));
        assert_eq!(provider_region("sea"), Some("SEA"));
        assert_eq!(provider_region("kr1"), None);
        assert_eq!(provider_region("mars"), None);
    }

    #[test]
    fn role_parse_is_lenient_and_case_insensitive() {
        assert_eq!(Role::parse("Top"), Some(Role::Top));
        assert_eq!(Role::parse("SUPPORTER"), Some(Role::Support));
        assert_eq!(Role::parse(" middle "), Some(Role::Mid));
        assert_eq!(Role::parse("Fill"), None);
        assert_eq!(Role::parse(""), None);
    }

    #[test]
    fn to_region_sorts_cuts_and_renumbers() {
        // Provider ranks are sparse (rank 1 hidden) and unordered.
        let entries = vec![
            entry(3, "Middle", &["1"]),
            entry(2, "Supporter", &["12", "78"]),
            entry(9, "Top", &[]),
        ];
        let board = to_region("kr", entries, 10);
        assert_eq!(board.region, "kr");
        let positions: Vec<u32> = board.players.iter().map(|p| p.position).collect();
        assert_eq!(positions, vec![1, 2, 3]);
        let first = board.players.first().expect("3 players");
        let third = board.players.get(2).expect("3 players");
        assert_eq!(first.role, Some(Role::Support));
        assert_eq!(first.champ_id.as_deref(), Some("12"));
        assert_eq!(third.role, Some(Role::Top));
        assert_eq!(third.champ_id, None);
    }

    #[test]
    fn to_region_respects_depth_and_keeps_unknown_roles_as_none() {
        let entries: Vec<RankEntry> = (1..=10)
            .map(|rank| entry(rank, if rank == 1 { "Fill" } else { "Jungle" }, &["7"]))
            .collect();
        let board = to_region("euw", entries, 5);
        assert_eq!(board.players.len(), 5);
        // The unknown-role player stays on the board with no role - it
        // simply leaves the statistics downstream.
        let first = board.players.first().expect("5 players");
        assert_eq!(first.role, None);
        assert!(
            board.players.get(1..).expect("5 players").iter().all(|p| p.role == Some(Role::Jungle))
        );
    }

    #[test]
    fn champion_names_skip_blank_entries() {
        let info = ChampionInfo {
            champions: vec![
                ChampionEntry { id: "1".to_owned(), name: "Annie".to_owned() },
                ChampionEntry { id: String::new(), name: "Ghost".to_owned() },
                ChampionEntry { id: "2".to_owned(), name: String::new() },
            ],
        };
        let names = to_champion_names(info);
        assert_eq!(names.len(), 1);
        assert_eq!(names.get("1").map(String::as_str), Some("Annie"));
    }

    #[test]
    fn default_client_builds() {
        // reqwest parses proxies lazily (a bogus proxy fails at request
        // time, not here) - construction only fails on client-build errors,
        // which the default parameters never trigger.
        assert!(DeepLolSource::new(None, Duration::from_secs(1)).is_ok());
    }
}
