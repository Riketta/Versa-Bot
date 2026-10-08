//! The one [`LeaderboardSourcePort`] adapter: `DeepLoL`'s public CDN API
//! (`https://b2c-api-cdn.deeplol.gg`, no auth). Endpoint field guide and
//! live-verified quirks live in this plugin's README.
//!
//! Pacing: requests are strictly sequential with the configured interval
//! between them - the source is someone else's website. Pagination stops at
//! the depth, the board's own `total_page`, an empty page, or a computed
//! page cap - a degraded board cannot keep the walk (and the singleflight
//! lock it runs under) alive, and a misbehaving response cannot balloon
//! memory or logs.

use std::collections::HashMap;
use std::future::Future;
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

/// Page-walk bounds a hostile or degraded board cannot push around. An
/// honest page carries ~100 entries; assuming at least ten plus slack for
/// partial pages caps the walk at roughly the page count the depth needs.
const MIN_ASSUMED_PAGE_SIZE: usize = 10;
const PAGE_CAP_SLACK: u32 = 10;

/// Refuses to buffer absurd bodies: honest pages are tens of kilobytes, a
/// multi-megabyte answer is a misbehaving CDN, not data. `Content-Length`
/// is checked when present; a chunked body is capped after the read (the
/// spike is transient and the body never reaches logs or storage).
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Third-party error bodies are truncated before they reach an error
/// string - and from there the logs. Shapes, not contents.
const ERROR_EXCERPT_CHARS: usize = 200;

/// Page count the walk may visit for `depth` entries: what a page of the
/// assumed minimum size would need, plus slack. Pure and unit-tested.
fn max_pages(depth: usize) -> u32 {
    depth.div_ceil(MIN_ASSUMED_PAGE_SIZE) as u32 + PAGE_CAP_SLACK
}

/// Sequential page walk shared by the real adapter and tests: fetches page
/// 1.. until the depth is reached, the board ends at its declared
/// `total_page`, a page comes back empty, or the computed page cap is hit.
/// Pacing (the between-page sleep) is the caller's closure, not logic here.
fn paginate<F, Fut>(
    mut fetch: F,
    depth: u32,
) -> impl Future<Output = Result<Vec<RankEntry>, SourceError>>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<RankPage, SourceError>>,
{
    async move {
        let depth = depth.max(1) as usize;
        let page_cap = max_pages(depth);
        let mut entries: Vec<RankEntry> = Vec::new();
        let mut page = 1u32;
        loop {
            let rank_page = fetch(page).await?;
            let total_page = rank_page.total_page.max(page);
            let board_ended = rank_page.players.is_empty();
            entries.extend(rank_page.players);
            if entries.len() >= depth || page >= total_page || board_ended {
                break;
            }
            if page >= page_cap {
                tracing::debug!(page, "page walk stopped at the page cap - board may be truncated");
                break;
            }
            page += 1;
        }
        Ok(entries)
    }
}

/// First chunk of a response body for error strings - char-safe, bounded.
fn body_excerpt(body: &str) -> String {
    if body.chars().count() <= ERROR_EXCERPT_CHARS {
        return body.to_owned();
    }
    let excerpt: String = body.chars().take(ERROR_EXCERPT_CHARS).collect();
    format!("{excerpt}\u{2026} (+{} bytes not logged)", body.len() - excerpt.len())
}

/// Neutral region key -> provider platform code. The provider expects
/// Riot's platform ids: digit-suffixed (`EUW1`, `NA1`, ... `TW2`, `VN2`,
/// `SG2` - the post-Garena servers too), with `KR` the lone digitless id
/// in Riot's own scheme (`KR1` is an unknown key). A digitless guess such
/// as `EUW` is not rejected with 422 - it fails inside the handler with a
/// misleading `HTTP 500`, so a wrong mapping looks like an outage.
pub fn provider_region(region: &str) -> Option<&'static str> {
    match region {
        "kr" => Some("KR"),
        "euw" => Some("EUW1"),
        "eun" => Some("EUN1"),
        "na" => Some("NA1"),
        "jp" => Some("JP1"),
        "br" => Some("BR1"),
        "tr" => Some("TR1"),
        "tw" => Some("TW2"),
        "vn" => Some("VN2"),
        "sea" => Some("SG2"),
        _ => None,
    }
}

/// Subset of `GET /summoner/summoner_rank` - only the fields consumed.
/// Everything is defaulted: an added or removed field degrades to its
/// default; a wrong-typed field still fails the whole page parse (the
/// region errors for that cycle and keeps its cached data).
#[derive(Debug, Clone, Deserialize)]
struct RankPage {
    #[serde(rename = "summoner_rank_list", default)]
    players: Vec<RankEntry>,
    #[serde(rename = "total_page", default)]
    total_page: u32,
}

#[derive(Debug, Clone, Deserialize)]
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
        if response.content_length().is_some_and(|len| len > MAX_BODY_BYTES as u64) {
            return Err(SourceError::Request(format!(
                "{url}: HTTP {status}: body exceeds the {MAX_BODY_BYTES}-byte cap"
            )));
        }
        let body = response
            .text()
            .await
            .map_err(|err| SourceError::Request(format!("{url}: reading body: {err}")))?;
        if body.len() > MAX_BODY_BYTES {
            return Err(SourceError::Request(format!(
                "{url}: HTTP {status}: body of {} bytes exceeds the {MAX_BODY_BYTES}-byte cap",
                body.len()
            )));
        }
        if !status.is_success() {
            return Err(SourceError::Request(format!(
                "{url}: HTTP {status}: {}",
                body_excerpt(&body)
            )));
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

        let entries = paginate(
            |page| {
                let path =
                    format!("/summoner/summoner_rank?platform_id={code}&lane=All&page={page}");
                async move {
                    if page > 1 {
                        tokio::time::sleep(self.interval).await;
                    }
                    self.get_json(&path).await
                }
            },
            depth,
        )
        .await?;

        Ok(to_region(region, entries, depth))
    }

    async fn champion_names(&self) -> Result<HashMap<String, String>, SourceError> {
        let info: ChampionInfo = self.get_json("/common/champion-info").await?;
        Ok(to_champion_names(info))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::*;

    fn entry(rank: u32, role: &str, champs: &[&str]) -> RankEntry {
        RankEntry {
            rank,
            most_role: role.to_owned(),
            most_champion: champs.iter().map(|id| (*id).to_owned()).collect(),
        }
    }

    #[test]
    fn region_parse_maps_known_keys_to_riot_platform_ids() {
        assert_eq!(provider_region("kr"), Some("KR"));
        assert_eq!(provider_region("euw"), Some("EUW1"));
        assert_eq!(provider_region("na"), Some("NA1"));
        assert_eq!(provider_region("tw"), Some("TW2"));
        assert_eq!(provider_region("sea"), Some("SG2"));
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

    // ---- Pagination bounds (the walk runs under the singleflight lock,
    // ---- so every terminator here is a hang prevention) ----

    fn page_of(players: Vec<RankEntry>, total_page: u32) -> Result<RankPage, SourceError> {
        Ok(RankPage { players, total_page })
    }

    fn full_page(count: u32, total_page: u32) -> Result<RankPage, SourceError> {
        page_of((1..=count).map(|rank| entry(rank, "Top", &["1"])).collect(), total_page)
    }

    #[test]
    fn max_pages_covers_the_depth_at_the_assumed_page_size_plus_slack() {
        assert_eq!(max_pages(1), 11);
        assert_eq!(max_pages(100), 20);
        assert_eq!(max_pages(10_000), 1_010);
    }

    #[tokio::test]
    async fn pagination_stops_once_depth_is_reached() {
        let calls = Rc::new(Cell::new(0u32));
        let counter = Rc::clone(&calls);
        let entries = paginate(
            move |_page| {
                let counter = Rc::clone(&counter);
                async move {
                    counter.set(counter.get() + 1);
                    full_page(100, 5) // honest board: 5 pages of 100
                }
            },
            250,
        )
        .await
        .expect("pagination succeeds");
        assert_eq!(calls.get(), 3, "250 entries need exactly three 100-entry pages");
        assert_eq!(entries.len(), 300, "the final page is fetched whole and cut later");
    }

    #[tokio::test]
    async fn pagination_stops_on_an_empty_page_despite_a_huge_total_page() {
        let calls = Rc::new(Cell::new(0u32));
        let counter = Rc::clone(&calls);
        let entries = paginate(
            move |page| {
                let counter = Rc::clone(&counter);
                async move {
                    counter.set(counter.get() + 1);
                    if page == 1 { full_page(100, u32::MAX) } else { page_of(Vec::new(), u32::MAX) }
                }
            },
            1_000,
        )
        .await
        .expect("pagination succeeds");
        assert_eq!(calls.get(), 2, "an empty page must terminate the walk immediately");
        assert_eq!(entries.len(), 100);
    }

    #[tokio::test]
    async fn pagination_stops_at_the_page_cap_when_pages_stay_tiny() {
        let calls = Rc::new(Cell::new(0u32));
        let counter = Rc::clone(&calls);
        let entries = paginate(
            move |_page| {
                let counter = Rc::clone(&counter);
                async move {
                    counter.set(counter.get() + 1);
                    full_page(1, u32::MAX) // hostile: one entry per page, endless board
                }
            },
            100,
        )
        .await
        .expect("pagination succeeds");
        assert_eq!(calls.get(), u32::from(max_pages(100)), "the cap, not the depth, ends it");
        assert_eq!(entries.len(), max_pages(100) as usize, "one entry per visited page");
    }

    #[tokio::test]
    async fn pagination_respects_the_board_total_page() {
        let calls = Rc::new(Cell::new(0u32));
        let counter = Rc::clone(&calls);
        let entries = paginate(
            move |page| {
                let counter = Rc::clone(&counter);
                async move {
                    counter.set(counter.get() + 1);
                    full_page(100, 2) // short board: plenty offered, only 2 pages exist
                }
            },
            1_000,
        )
        .await
        .expect("pagination succeeds");
        assert_eq!(calls.get(), 2, "a board reporting fewer pages yields a shorter parse");
        assert_eq!(entries.len(), 200);
    }

    #[tokio::test]
    async fn pagination_propagates_fetch_errors() {
        let entries = paginate(
            |page| async move {
                if page == 1 {
                    full_page(100, 5)
                } else {
                    Err(SourceError::Request("boom".to_owned()))
                }
            },
            1_000,
        )
        .await;
        assert!(entries.is_err(), "a failed page fails the region");
    }

    // ---- Error-body truncation (third-party bodies must not reach logs) ----

    #[test]
    fn body_excerpt_passes_short_bodies_through() {
        assert_eq!(body_excerpt("Not Found"), "Not Found");
    }

    #[test]
    fn body_excerpt_truncates_long_bodies_char_safely() {
        let long = "x".repeat(5_000);
        let excerpt = body_excerpt(&long);
        assert!(excerpt.starts_with('x'));
        assert!(excerpt.contains("bytes not logged"));
        assert!(excerpt.chars().count() < 250, "bounded: 200 chars plus a fixed suffix");

        // Multibyte content must not panic or split a char.
        let cyrillic = "ж".repeat(1_000);
        let excerpt = body_excerpt(&cyrillic);
        assert!(excerpt.starts_with("жжж"));
        assert!(excerpt.contains("bytes not logged"));
    }
}
