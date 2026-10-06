//! The leaderboard engine: a process-lifetime cache over the data source
//! with singleflight refresh.
//!
//! The data is world data, not guild data - there is deliberately no
//! `GuildStorage` here. Nothing is persisted: after a restart the first
//! command pays the full parse, which an 18h-class TTL makes irrelevant.
//!
//! Refresh is on demand only: a fresh cache answers instantly, a stale
//! region triggers a re-parse inside the command's interaction window.
//! Concurrent invocations share one refresh (singleflight); a failing
//! region keeps its previous data and is reported in the snapshot instead
//! of failing the whole answer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::port::{LeaderboardSourcePort, RegionLeaderboard, SourceError};

/// Player-count windows resolved from config: buckets clamped to the parse
/// depth (sorted, deduplicated; empty -> a single full-depth row), the
/// champion pool capped likewise, and the per-role champion list length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedView {
    pub buckets: Vec<u32>,
    pub champ_pool: u32,
    pub champs_per_role: u32,
}

impl ResolvedView {
    #[must_use]
    pub fn resolve(
        parse_depth: u32,
        buckets: &[u32],
        champ_pool: u32,
        champs_per_role: u32,
    ) -> Self {
        let parse_depth = parse_depth.max(1);
        let mut resolved: Vec<u32> =
            buckets.iter().filter(|&&bucket| bucket > 0).map(|&b| b.min(parse_depth)).collect();
        resolved.sort_unstable();
        resolved.dedup();
        if resolved.is_empty() {
            resolved.push(parse_depth);
        }
        Self {
            buckets: resolved,
            champ_pool: match champ_pool {
                0 => parse_depth,
                pool => pool.min(parse_depth),
            },
            champs_per_role: champs_per_role.max(1),
        }
    }
}

/// Engine settings: the data window the engine serves. Regions, parse
/// depth, cache TTL and the display view hot-reload
/// ([`LeaderboardEngine::update_settings`]); the pacing interval is
/// boot-frozen with the source adapter's own page pacing.
#[derive(Debug, Clone)]
pub struct EngineSettings {
    pub regions: Vec<String>,
    pub parse_depth: u32,
    pub cache_ttl: Duration,
    pub request_interval: Duration,
    pub view: ResolvedView,
}

impl EngineSettings {
    /// Regions are normalized (trimmed, lowercased, deduplicated,
    /// order-preserving); unknown keys are dropped here so the engine only
    /// ever asks the source for regions it serves. The display windows
    /// arrive pre-resolved ([`ResolvedView::resolve`]).
    #[must_use]
    pub fn new(
        source: &dyn LeaderboardSourcePort,
        regions: &[String],
        parse_depth: u32,
        cache_ttl: Duration,
        request_interval: Duration,
        view: ResolvedView,
    ) -> Self {
        let known = source.known_regions();
        let mut normalized: Vec<String> = Vec::new();
        for region in regions {
            let key = region.trim().to_ascii_lowercase();
            if key.is_empty() {
                continue;
            }
            if !known.contains(&key.as_str()) {
                tracing::warn!(region = %region, "config region is not served by the leaderboard source - dropped");
                continue;
            }
            if !normalized.contains(&key) {
                normalized.push(key);
            }
        }
        Self { regions: normalized, parse_depth, cache_ttl, request_interval, view }
    }
}

/// One refresh's outcome, consumed by the stats layer.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Total players the full parse would cover (configured regions x
    /// depth) - the honest denominator for the coverage line.
    pub requested_players: usize,
    /// Served regions, in configured order (only those with data).
    pub regions: Vec<RegionLeaderboard>,
    pub champ_names: HashMap<String, String>,
    /// Regions whose refresh failed this cycle (their cached data, if any,
    /// is served and reflected in `stale`).
    pub failures: Vec<String>,
    /// True when any served region is beyond the TTL (failed refresh).
    pub stale: bool,
    /// Age of the oldest served region's data.
    pub age: Duration,
    pub view: ResolvedView,
}

struct CachedRegion {
    data: RegionLeaderboard,
    fetched_at: Instant,
}

#[derive(Default)]
struct CacheState {
    regions: HashMap<String, CachedRegion>,
    champ_names: HashMap<String, String>,
    names_fetched_at: Option<Instant>,
}

pub struct LeaderboardEngine {
    source: Arc<dyn LeaderboardSourcePort>,
    settings: Mutex<EngineSettings>,
    cache: Mutex<CacheState>,
    /// Singleflight: concurrent invocations share one refresh cycle - the
    /// second caller waits, then finds everything fresh.
    refresh: tokio::sync::Mutex<()>,
}

impl LeaderboardEngine {
    #[must_use]
    pub fn new(source: Arc<dyn LeaderboardSourcePort>, settings: EngineSettings) -> Self {
        Self {
            source,
            settings: Mutex::new(settings),
            cache: Mutex::new(CacheState::default()),
            refresh: tokio::sync::Mutex::new(()),
        }
    }

    /// False when no usable region survived config normalization: the
    /// command then explains itself instead of parsing nothing.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        !self.settings.lock().regions.is_empty()
    }

    /// Hot reload: swaps the data window - regions, parse depth, cache TTL,
    /// display view. The pacing interval is kept: the source adapter's own
    /// page pacing is built once at boot, and the two cadences must not
    /// fork. Identical values are a no-op. Cached region data survives the
    /// swap - fresh entries for re-added regions are reused, removed ones
    /// idle until the same name returns (bounded by served region names).
    pub fn update_settings(&self, settings: EngineSettings) {
        let mut current = self.settings.lock();
        if current.regions == settings.regions
            && current.parse_depth == settings.parse_depth
            && current.cache_ttl == settings.cache_ttl
            && current.view == settings.view
        {
            return;
        }
        current.regions = settings.regions;
        current.parse_depth = settings.parse_depth;
        current.cache_ttl = settings.cache_ttl;
        current.view = settings.view;
        drop(current);
        tracing::info!("leaderboard data window changed");
    }

    /// Returns a snapshot, refreshing stale regions first (under the
    /// singleflight lock). Errors only when there is no data at all - a
    /// partially failed cycle serves what it has.
    ///
    /// # Errors
    /// Only when every configured region lacks data (a total failure of
    /// this refresh cycle with an empty cache) - the source error of the
    /// last failed region is returned.
    pub async fn snapshot(&self) -> Result<Snapshot, SourceError> {
        let _single = self.refresh.lock().await;
        let now = Instant::now();
        // One clone per cycle, before any await - the cell may swap
        // mid-refresh, and the cycle must run on one coherent window.
        let settings = self.settings.lock().clone();
        let ttl = settings.cache_ttl;

        // Short cache-lock: decide what is stale. The fetches below run
        // outside the cache lock - but inside the singleflight lock, so a
        // long refresh delays other callers (bounded by the source caps).
        let stale_regions: Vec<String> = {
            let state = self.cache.lock();
            settings
                .regions
                .iter()
                .filter(|region| match state.regions.get(*region) {
                    Some(entry) => now.duration_since(entry.fetched_at) > ttl,
                    None => true,
                })
                .cloned()
                .collect()
        };
        let names_stale = {
            let state = self.cache.lock();
            match state.names_fetched_at {
                Some(at) => now.duration_since(at) > ttl,
                None => true,
            }
        };

        let mut failures: Vec<String> = Vec::new();
        let mut last_error: Option<SourceError> = None;
        let mut paced = false;

        if names_stale {
            paced = true;
            match self.source.champion_names().await {
                Ok(names) => {
                    let mut state = self.cache.lock();
                    state.champ_names = names;
                    state.names_fetched_at = Some(Instant::now());
                }
                Err(err) => {
                    // Old names (if any) keep serving; ids missing from the
                    // map degrade to placeholders downstream.
                    tracing::warn!(error = %err, "leaderboard champion names refresh failed");
                }
            }
        }

        for region in &stale_regions {
            if paced {
                tokio::time::sleep(settings.request_interval).await;
            }
            paced = true;
            let started = Instant::now();
            match self.source.leaderboard(region, settings.parse_depth).await {
                Ok(data) => {
                    tracing::debug!(
                        region = %region,
                        players = data.players.len(),
                        elapsed_ms = started.elapsed().as_millis(),
                        "leaderboard region parsed"
                    );
                    let mut state = self.cache.lock();
                    state
                        .regions
                        .insert(region.clone(), CachedRegion { data, fetched_at: Instant::now() });
                }
                Err(err) => {
                    tracing::warn!(region = %region, error = %err, "leaderboard region refresh failed");
                    failures.push(region.clone());
                    last_error = Some(err);
                }
            }
        }

        // Short lock: build the snapshot from the settled cache.
        let (regions, ages, champ_names) = {
            let state = self.cache.lock();
            let mut regions = Vec::new();
            let mut ages = Vec::new();
            for region in &settings.regions {
                if let Some(entry) = state.regions.get(region) {
                    regions.push(entry.data.clone());
                    ages.push(now.duration_since(entry.fetched_at));
                }
            }
            (regions, ages, state.champ_names.clone())
        };

        if regions.is_empty() {
            return Err(last_error
                .unwrap_or(SourceError::Request("no leaderboard data available".to_owned())));
        }

        let age = ages.iter().copied().max().unwrap_or_default();
        let stale = !failures.is_empty() || age > ttl;
        Ok(Snapshot {
            requested_players: settings.regions.len() * settings.parse_depth as usize,
            regions,
            champ_names,
            failures,
            stale,
            age,
            view: settings.view.clone(),
        })
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! A programmable fake source shared by the engine and command tests.

    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::super::port::LeaderboardSourcePort as Port;
    use super::super::port::{LeaderboardPlayer, RegionLeaderboard, Role, SourceError};
    use super::*;

    /// Fake source: records calls, returns configured boards, optionally
    /// blocks every `leaderboard` call behind a spin gate (singleflight
    /// proof) or fails chosen regions.
    pub struct FakeSource {
        pub calls: AtomicUsize,
        pub name_calls: AtomicUsize,
        pub boards: StdMutex<HashMap<String, Vec<LeaderboardPlayer>>>,
        pub failing: StdMutex<Vec<String>>,
        /// 0 = calls block (yield-spin, race-free on the test runtime);
        /// 1 = calls proceed.
        pub gate_open: AtomicUsize,
    }

    impl FakeSource {
        pub fn new() -> Arc<Self> {
            Self::gated(true)
        }

        pub fn ungated() -> Arc<Self> {
            Self::gated(false)
        }

        fn gated(start_closed: bool) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                name_calls: AtomicUsize::new(0),
                boards: StdMutex::new(HashMap::new()),
                failing: StdMutex::new(Vec::new()),
                gate_open: AtomicUsize::new(usize::from(!start_closed)),
            })
        }

        pub fn set_board(&self, region: &str, players: Vec<LeaderboardPlayer>) {
            self.boards.lock().expect("boards").insert(region.to_owned(), players);
        }

        pub fn fail_region(&self, region: &str) {
            self.failing.lock().expect("failing").push(region.to_owned());
        }

        pub fn heal_region(&self, region: &str) {
            self.failing.lock().expect("failing").retain(|key| key != region);
        }

        pub fn leaderboard_calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        pub fn name_calls(&self) -> usize {
            self.name_calls.load(Ordering::SeqCst)
        }

        pub fn open_gate(&self) {
            self.gate_open.store(1, Ordering::SeqCst);
        }

        pub fn player(position: u32, role: Option<Role>, champ: Option<&str>) -> LeaderboardPlayer {
            LeaderboardPlayer { position, role, champ_id: champ.map(str::to_owned) }
        }

        /// A deterministic board: `depth` players cycling through all roles
        /// with champion ids.
        pub fn board(region: &str, depth: u32) -> RegionLeaderboard {
            let players = (1..=depth)
                .map(|position| {
                    let role = Role::ALL
                        .iter()
                        .cycle()
                        .nth(position as usize - 1)
                        .copied()
                        .expect("position > 0");
                    Self::player(position, Some(role), Some("1"))
                })
                .collect();
            RegionLeaderboard { region: region.to_owned(), players }
        }
    }

    #[async_trait]
    impl Port for FakeSource {
        fn known_regions(&self) -> &'static [&'static str] {
            &["kr", "euw", "na"]
        }

        async fn leaderboard(
            &self,
            region: &str,
            depth: u32,
        ) -> Result<RegionLeaderboard, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            while self.gate_open.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            if self.failing.lock().expect("failing").iter().any(|r| r == region) {
                return Err(SourceError::Request(format!("{region} is down")));
            }
            let players = self
                .boards
                .lock()
                .expect("boards")
                .get(region)
                .cloned()
                .unwrap_or_else(|| Self::board(region, depth.min(10)).players);
            Ok(RegionLeaderboard { region: region.to_owned(), players })
        }

        async fn champion_names(&self) -> Result<HashMap<String, String>, SourceError> {
            self.name_calls.fetch_add(1, Ordering::SeqCst);
            let mut names = HashMap::new();
            names.insert("1".to_owned(), "Annie".to_owned());
            Ok(names)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::FakeSource;
    use super::*;
    use crate::plugins::lol_leaderboard::port::Role;

    fn engine_with(
        source: &Arc<FakeSource>,
        regions: &[&str],
        ttl: Duration,
    ) -> Arc<LeaderboardEngine> {
        let keys: Vec<String> = regions.iter().map(|key| (*key).to_owned()).collect();
        let settings = EngineSettings::new(
            source.as_ref(),
            &keys,
            1000,
            ttl,
            Duration::ZERO,
            ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        );
        Arc::new(LeaderboardEngine::new(
            Arc::clone(source) as Arc<dyn LeaderboardSourcePort>,
            settings,
        ))
    }

    fn engine(source: &Arc<FakeSource>, regions: &[&str]) -> Arc<LeaderboardEngine> {
        engine_with(source, regions, Duration::from_secs(3600))
    }

    /// A stale region must refetch: without this, the dump would serve
    /// frozen data forever after one failed refresh cycle.
    #[tokio::test]
    async fn ttl_expiry_triggers_a_refetch() {
        let source = FakeSource::ungated();
        let engine = engine_with(&source, &["kr"], Duration::from_millis(60));
        let _ = engine.snapshot().await.expect("first snapshot");
        let _ = engine.snapshot().await.expect("fresh snapshot");
        assert_eq!(source.leaderboard_calls(), 1, "fresh cache answers without the source");

        tokio::time::sleep(Duration::from_millis(90)).await;
        let _ = engine.snapshot().await.expect("stale snapshot");
        assert_eq!(source.leaderboard_calls(), 2, "TTL expiry must trigger a refetch");
    }

    #[test]
    fn resolve_view_clamps_sorts_dedups_and_defaults() {
        let view = ResolvedView::resolve(500, &[1000, 300, 300, 0], 50_000, 0);
        assert_eq!(view.buckets, vec![300, 500]);
        assert_eq!(view.champ_pool, 500);
        assert_eq!(view.champs_per_role, 1);

        let empty = ResolvedView::resolve(500, &[], 0, 5);
        assert_eq!(empty.buckets, vec![500]);
        assert_eq!(empty.champ_pool, 500);

        let untouched = ResolvedView::resolve(1000, &[300, 1000], 800, 5);
        assert_eq!(untouched.buckets, vec![300, 1000]);
        assert_eq!(untouched.champ_pool, 800);
    }

    #[test]
    fn settings_normalize_regions_against_the_source() {
        let source = FakeSource::ungated();
        let keys = ["KR".to_owned(), " kr ".to_owned(), String::new(), "mars".to_owned()];
        let settings = EngineSettings::new(
            source.as_ref(),
            &keys,
            1000,
            Duration::from_secs(3600),
            Duration::ZERO,
            ResolvedView::resolve(1000, &[300], 300, 5),
        );
        assert_eq!(settings.regions, vec!["kr"]);
        assert_eq!(settings.parse_depth, 1000);
    }

    /// Hot reload: the data window (regions, depth, TTL, view) swaps, the
    /// pacing interval stays at its boot value, and an empty regions list
    /// flips the engine into not-configured mode - the same state a
    /// degraded section maps to at boot.
    #[test]
    fn update_settings_swaps_the_data_window_but_keeps_pacing() {
        let source = Arc::new(FakeSource::ungated());
        let engine = engine_with(&source, &["kr", "na"], Duration::from_secs(3600));
        assert!(engine.is_configured());

        engine.update_settings(EngineSettings {
            regions: vec!["euw".to_owned()],
            parse_depth: 50,
            cache_ttl: Duration::from_secs(60),
            request_interval: Duration::from_secs(99),
            view: ResolvedView::resolve(50, &[25], 50, 1),
        });
        let settings = engine.settings.lock();
        assert_eq!(settings.regions, vec!["euw"]);
        assert_eq!(settings.parse_depth, 50);
        assert_eq!(settings.cache_ttl, Duration::from_secs(60));
        assert_eq!(settings.request_interval, Duration::ZERO, "pacing stays boot-frozen");
        assert_eq!(settings.view.buckets, vec![25]);
        drop(settings);

        engine.update_settings(EngineSettings {
            regions: Vec::new(),
            parse_depth: 50,
            cache_ttl: Duration::from_secs(60),
            request_interval: Duration::ZERO,
            view: ResolvedView::resolve(50, &[], 50, 1),
        });
        assert!(!engine.is_configured());
    }

    #[tokio::test]
    async fn fresh_cache_answers_without_touching_the_source() {
        let source = FakeSource::ungated();
        let engine = engine(&source, &["kr", "na"]);
        let first = engine.snapshot().await.expect("first snapshot");
        assert_eq!(source.leaderboard_calls(), 2);
        assert_eq!(source.name_calls(), 1);
        assert_eq!(first.regions.len(), 2);
        assert!(!first.stale);
        assert_eq!(first.requested_players, 2000);

        let second = engine.snapshot().await.expect("second snapshot");
        assert_eq!(source.leaderboard_calls(), 2);
        assert_eq!(source.name_calls(), 1);
        assert_eq!(second.regions.len(), 2);
    }

    #[tokio::test]
    async fn refresh_targets_only_stale_or_missing_regions() {
        let source = FakeSource::ungated();
        source.fail_region("na");
        let engine = engine(&source, &["kr", "na"]);
        engine.snapshot().await.expect("first snapshot");
        assert_eq!(source.leaderboard_calls(), 2);

        source.heal_region("na");
        engine.snapshot().await.expect("second snapshot");
        // kr was fresh - only na was retried.
        assert_eq!(source.leaderboard_calls(), 3);
    }

    #[tokio::test]
    async fn failed_region_keeps_its_previous_data_and_is_reported() {
        let source = FakeSource::ungated();
        let engine = engine_with(&source, &["kr", "na"], Duration::ZERO);
        let warm = engine.snapshot().await.expect("warm");
        assert_eq!(warm.regions.len(), 2);

        source.fail_region("na");
        let snapshot = engine.snapshot().await.expect("refresh snapshot");
        // na serves its stale cached board; the failure is named.
        assert_eq!(snapshot.regions.len(), 2);
        assert_eq!(snapshot.failures, vec!["na".to_owned()]);
        assert!(snapshot.stale);
        let na = snapshot.regions.get(1).expect("two served regions");
        assert_eq!(na.region, "na");
    }

    #[tokio::test]
    async fn total_failure_yields_an_error() {
        let source = FakeSource::ungated();
        source.fail_region("kr");
        source.fail_region("na");
        let engine = engine(&source, &["kr", "na"]);
        let result = engine.snapshot().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn concurrent_snapshots_share_one_refresh() {
        let source = FakeSource::new(); // gated: calls block until opened
        let engine = engine(&source, &["kr"]);
        let first = Arc::clone(&engine);
        let task_one = tokio::spawn(async move { first.snapshot().await });
        while source.leaderboard_calls() == 0 {
            tokio::task::yield_now().await;
        }
        let second = Arc::clone(&engine);
        let task_two = tokio::spawn(async move { second_snapshot(second).await });
        tokio::task::yield_now().await;
        source.open_gate();

        let one = task_one.await.expect("join").expect("snapshot");
        let two = task_two.await.expect("join").expect("snapshot");
        // One parse total: the second caller found the cache fresh.
        assert_eq!(source.leaderboard_calls(), 1);
        assert_eq!(one.regions.len(), 1);
        assert_eq!(two.regions.len(), 1);
    }

    async fn second_snapshot(engine: Arc<LeaderboardEngine>) -> Result<Snapshot, SourceError> {
        engine.snapshot().await
    }

    #[tokio::test]
    async fn configured_boards_flow_into_the_snapshot() {
        let source = FakeSource::ungated();
        source.set_board(
            "kr",
            vec![
                FakeSource::player(1, Some(Role::Top), Some("1")),
                FakeSource::player(2, Some(Role::Support), None),
            ],
        );
        let engine = engine_with(&source, &["kr"], Duration::from_secs(3600));
        let snapshot = engine.snapshot().await.expect("snapshot");
        let kr = snapshot.regions.first().expect("kr served");
        assert_eq!(kr.players.len(), 2);
        assert_eq!(snapshot.requested_players, 1000);
        assert_eq!(kr.players.first().expect("2 players").champ_id.as_deref(), Some("1"));
        assert_eq!(kr.players.get(1).expect("2 players").champ_id, None);
        assert_eq!(snapshot.champ_names.get("1").map(String::as_str), Some("Annie"));
    }
}
