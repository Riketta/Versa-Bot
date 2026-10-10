//! The leaderboard engine: a cache over the data source with singleflight
//! refresh, persisted across restarts.
//!
//! The data is world data, not guild data - there is deliberately no
//! `GuildStorage` here. The complete dump (all cached regions + the
//! champion-name table + the completion timestamp) is written to the
//! plugin-global storage after every refresh cycle, and the first snapshot
//! or cycle after a restart restores it: a restart inside the TTL serves
//! the old dump without touching the source, an older one serves it stale
//! (age-honest) until a cycle refreshes it.
//!
//! Refresh has two drivers sharing one singleflight: the background job
//! (default) re-parses TTL-expired regions off the user path, while the
//! command serves cached data as-is and fills only never-fetched regions
//! inline (on-demand mode re-parses everything stale before replying).
//! A failing region keeps its previous data and is reported in the
//! snapshot instead of failing the whole answer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::kernel::plugin_ports::Job;
use crate::kernel::spi_ports::PluginStorage;

use super::port::{LeaderboardPlayer, LeaderboardSourcePort, RegionLeaderboard, SourceError};

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
    /// Background refresh: a scheduler job re-parses stale regions off the
    /// user path (ticking at `cache_ttl`, first tick immediate - the boot
    /// warm-up), and the command serves any cached data as-is, however old
    /// (age-honest), refreshing inline only what was never fetched.
    /// `false` = on-demand mode: the command re-parses every stale region
    /// before replying. Startup-only, like the pacing interval - the job
    /// is a boot decision.
    pub background_refresh: bool,
    /// Wall-clock budget for a command-path (inline) refresh cycle: a
    /// region past the budget keeps its cached data (or is named as a
    /// failure when it has none) instead of parking the invoker behind a
    /// slow source. Zero = unbounded. The background job ignores it -
    /// a deep config must finish its walk eventually.
    pub inline_budget: Duration,
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
        background_refresh: bool,
        inline_budget: Duration,
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
        Self {
            regions: normalized,
            parse_depth,
            cache_ttl,
            request_interval,
            background_refresh,
            inline_budget,
            view,
        }
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
    /// Unix seconds when this region's parse completed. Unix, not
    /// `Instant`, so the age survives a restart (the persisted dump
    /// restores it verbatim).
    fetched_at: u64,
}

/// How long a failed champion-names fetch suppresses the inline (command
/// path) retry - without it, a down names endpoint would be re-attempted
/// by every command in background mode. The background job ignores this
/// and retries on its own tick cadence.
const NAMES_FAILURE_RETRY: Duration = Duration::from_secs(30);

/// Where the dump persists. Plugin-global (world data, platform-scoped by
/// the storage port's own view), one document per deployment.
const STORAGE_NAMESPACE: &str = "leaderboard";
const STORAGE_KEY: &str = "snapshot";

/// One region's entry in the persisted dump: the cached data plus the
/// region's own parse-completion second - per-region, so a dump assembled
/// from mixed cycles (restored regions + a fresh one) stays age-honest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRegion {
    region: String,
    fetched_at: u64,
    players: Vec<LeaderboardPlayer>,
}

/// The persisted shape of the complete dump, written after every refresh
/// cycle. `saved_at` is the unix second the writing cycle completed - the
/// timestamp that says how old the whole dump is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedDump {
    saved_at: u64,
    regions: Vec<PersistedRegion>,
    champ_names: HashMap<String, String>,
}

/// Unix seconds right now - cache timestamps and staleness math. Unix, not
/// `Instant`, because the timestamps must survive restarts.
fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// TTL semantics in unix-second granularity: a zero TTL is always stale
/// (the documented always-reparse escape hatch); otherwise an age of at
/// least the TTL is stale.
fn is_stale(fetched_at: u64, ttl: Duration, now: u64) -> bool {
    ttl.is_zero() || now.saturating_sub(fetched_at) >= ttl.as_secs()
}

#[derive(Default)]
struct CacheState {
    regions: HashMap<String, CachedRegion>,
    champ_names: HashMap<String, String>,
    names_fetched_at: Option<u64>,
    /// Not-before instant of the next inline names retry after a failure.
    /// Process-local backoff - deliberately not persisted.
    names_retry_not_before: Option<Instant>,
}

pub struct LeaderboardEngine {
    source: Arc<dyn LeaderboardSourcePort>,
    settings: Mutex<EngineSettings>,
    cache: Mutex<CacheState>,
    /// Singleflight: concurrent invocations share one refresh cycle - the
    /// second caller waits, then finds everything fresh.
    refresh: tokio::sync::Mutex<()>,
    /// Plugin-global document storage behind the dump persistence; `None`
    /// keeps the engine memory-only.
    storage: Option<Arc<dyn PluginStorage>>,
    /// Restores the persisted dump exactly once, on the first snapshot or
    /// background cycle - whichever comes first after boot.
    restored: tokio::sync::OnceCell<()>,
}

impl LeaderboardEngine {
    #[must_use]
    pub fn new(source: Arc<dyn LeaderboardSourcePort>, settings: EngineSettings) -> Self {
        Self {
            source,
            settings: Mutex::new(settings),
            cache: Mutex::new(CacheState::default()),
            refresh: tokio::sync::Mutex::new(()),
            storage: None,
            restored: tokio::sync::OnceCell::new(),
        }
    }

    /// [`Self::new`] plus dump persistence: every refresh cycle writes the
    /// complete dump (all cached regions + champion names, each region
    /// stamped with its own parse-completion second), and the first
    /// snapshot or cycle after a restart restores it - a restart inside
    /// the TTL serves the old dump with zero source calls, an older one
    /// serves it stale (age-honest) until a cycle refreshes it.
    #[must_use]
    pub fn with_storage(
        source: Arc<dyn LeaderboardSourcePort>,
        settings: EngineSettings,
        storage: Arc<dyn PluginStorage>,
    ) -> Self {
        Self {
            source,
            settings: Mutex::new(settings),
            cache: Mutex::new(CacheState::default()),
            refresh: tokio::sync::Mutex::new(()),
            storage: Some(storage),
            restored: tokio::sync::OnceCell::new(),
        }
    }

    /// False when no usable region survived config normalization: the
    /// command then explains itself instead of parsing nothing.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        !self.settings.lock().regions.is_empty()
    }

    /// Restores the persisted dump exactly once per process, before the
    /// first staleness decision - a restart inside the TTL must serve the
    /// dump without touching the source. Only fills entries the runtime
    /// cache does not have yet: it never overwrites fresher data a cycle
    /// already fetched. A broken storage read degrades to an empty cache
    /// and retries on the next use (the cell init error is not stored); a
    /// document that does not parse is skipped permanently - it cannot get
    /// better on its own.
    async fn ensure_restored(&self) {
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        let _ = self
            .restored
            .get_or_try_init(|| async {
                match storage.get(STORAGE_NAMESPACE, STORAGE_KEY).await {
                    Ok(Some(value)) => match serde_json::from_value::<PersistedDump>(value) {
                        Ok(dump) => {
                            let mut state = self.cache.lock();
                            for entry in dump.regions {
                                state.regions.entry(entry.region.clone()).or_insert(CachedRegion {
                                    data: RegionLeaderboard {
                                        region: entry.region,
                                        players: entry.players,
                                    },
                                    fetched_at: entry.fetched_at,
                                });
                            }
                            if state.names_fetched_at.is_none() {
                                state.champ_names = dump.champ_names;
                                state.names_fetched_at = Some(dump.saved_at);
                            }
                            tracing::debug!(
                                saved_at = dump.saved_at,
                                "leaderboard dump restored from storage"
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                error = %err,
                                "leaderboard persisted dump not understood - starting empty"
                            );
                        }
                    },
                    Ok(None) => {}
                    Err(err) => {
                        tracing::debug!(
                            error = %err,
                            "leaderboard dump restore failed - retrying on next use"
                        );
                        return Err(());
                    }
                }
                Ok(())
            })
            .await;
    }

    /// Writes the complete dump with per-region parse timestamps plus the
    /// writing cycle's completion second. Best-effort: a failed write
    /// keeps the in-memory cache serving; it only reappears as a re-parse
    /// after a restart, never as a failed reply.
    async fn persist(&self) {
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        let dump = {
            let state = self.cache.lock();
            PersistedDump {
                saved_at: unix_now(),
                regions: state
                    .regions
                    .iter()
                    .map(|(region, entry)| PersistedRegion {
                        region: region.clone(),
                        fetched_at: entry.fetched_at,
                        players: entry.data.players.clone(),
                    })
                    .collect(),
                champ_names: state.champ_names.clone(),
            }
        };
        match serde_json::to_value(&dump) {
            Ok(value) => {
                if let Err(err) = storage.set(STORAGE_NAMESPACE, STORAGE_KEY, value).await {
                    tracing::warn!(
                        error = %err,
                        "leaderboard dump persistence failed - a restart will re-parse"
                    );
                }
            }
            Err(err) => tracing::warn!(error = %err, "leaderboard dump serialization failed"),
        }
    }

    /// The current settings (boot-time scheduling reads the TTL and mode
    /// through this).
    #[must_use]
    pub fn settings(&self) -> EngineSettings {
        self.settings.lock().clone()
    }

    /// Hot reload: swaps the data window - regions, parse depth, cache TTL,
    /// display view. The pacing interval, the background-refresh mode, and
    /// the inline budget are kept: the source adapter's own page pacing is
    /// built once at boot, and the job is a boot decision - the two
    /// cadences must not fork. Identical values are a no-op. Cached region
    /// data survives the swap - fresh entries for re-added regions are
    /// reused, removed ones idle until the same name returns (bounded by
    /// served region names).
    pub fn update_settings(&self, settings: EngineSettings) {
        let mut current = self.settings.lock();
        if current.regions == settings.regions
            && current.parse_depth == settings.parse_depth
            && current.cache_ttl == settings.cache_ttl
            && current.view == settings.view
        {
            return;
        }
        let mut changes: Vec<String> = Vec::new();
        if current.regions != settings.regions {
            changes.push(format!("regions {:?} -> {:?}", current.regions, settings.regions));
        }
        if current.parse_depth != settings.parse_depth {
            changes
                .push(format!("parse_depth {} -> {}", current.parse_depth, settings.parse_depth));
        }
        if current.cache_ttl != settings.cache_ttl {
            changes.push(format!("cache_ttl {:?} -> {:?}", current.cache_ttl, settings.cache_ttl));
        }
        if current.view != settings.view {
            changes.push(format!("view {:?} -> {:?}", current.view, settings.view));
        }
        current.regions = settings.regions;
        current.parse_depth = settings.parse_depth;
        current.cache_ttl = settings.cache_ttl;
        current.view = settings.view;
        drop(current);
        tracing::info!(changes = ?changes, "config lol_leaderboard hot-reloaded");
    }

    /// Returns a snapshot for the command. Background mode serves any
    /// cached data immediately (zero source calls, age-honest) and
    /// refreshes inline only what was never fetched - the boot race before
    /// the background job's first cycle lands. On-demand mode re-parses
    /// every stale region first. Both drivers share the singleflight and
    /// the inline budget. Errors only when there is no data at all - a
    /// partially failed cycle serves what it has.
    ///
    /// # Errors
    /// Only when every configured region lacks data (a total failure of
    /// this refresh cycle with an empty cache) - the source error of the
    /// last failed region is returned.
    pub async fn snapshot(&self) -> Result<Snapshot, SourceError> {
        self.ensure_restored().await;
        let settings = self.settings.lock().clone();
        let scope = if settings.background_refresh {
            RefreshScope::MissingOnly
        } else {
            RefreshScope::StaleOrMissing
        };
        let mut failures: Vec<String> = Vec::new();
        let mut last_error: Option<SourceError> = None;
        {
            let pending = self.pending_refresh(&settings, scope);
            if !pending.regions.is_empty() || pending.names {
                let _single = self.refresh.lock().await;
                // Re-decide under the lock: the cycle we waited for may
                // have filled the cache (singleflight semantics).
                let pending = self.pending_refresh(&settings, scope);
                if !pending.regions.is_empty() || pending.names {
                    // Zero = unbounded (the documented escape hatch).
                    let budget =
                        (!settings.inline_budget.is_zero()).then(|| settings.inline_budget);
                    let (region_failures, error) =
                        self.run_refresh(&settings, &pending, budget).await;
                    failures = region_failures;
                    last_error = error;
                }
            }
        }

        // Short lock: build the snapshot from the settled cache. The age
        // is the oldest served region's data - unix-second granularity,
        // which is what makes a restored dump's age honest.
        let now = unix_now();
        let (regions, oldest, champ_names) = {
            let state = self.cache.lock();
            let mut regions = Vec::new();
            let mut oldest: Option<u64> = None;
            for region in &settings.regions {
                if let Some(entry) = state.regions.get(region) {
                    regions.push(entry.data.clone());
                    oldest = Some(
                        oldest.map_or(entry.fetched_at, |seen: u64| seen.min(entry.fetched_at)),
                    );
                }
            }
            (regions, oldest, state.champ_names.clone())
        };

        if regions.is_empty() {
            return Err(last_error
                .unwrap_or(SourceError::Request("no leaderboard data available".to_owned())));
        }

        let oldest = oldest.unwrap_or_else(unix_now);
        let age = Duration::from_secs(now.saturating_sub(oldest));
        let stale = !failures.is_empty() || is_stale(oldest, settings.cache_ttl, now);
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

    /// One background refresh cycle (the scheduler job): re-parse every
    /// region past the TTL, off the user path, under the singleflight,
    /// WITHOUT the inline budget. A cheap no-op when everything is fresh.
    pub async fn refresh_stale(&self) {
        self.ensure_restored().await;
        let settings = self.settings.lock().clone();
        let pending = self.pending_refresh(&settings, RefreshScope::StaleOrMissing);
        if pending.regions.is_empty() && !pending.names {
            return;
        }
        let _single = self.refresh.lock().await;
        // Re-decide under the lock: an inline cycle may have filled the
        // cache while we waited.
        let pending = self.pending_refresh(&settings, RefreshScope::StaleOrMissing);
        if pending.regions.is_empty() && !pending.names {
            return;
        }
        let (failures, _) = self.run_refresh(&settings, &pending, None).await;
        if !failures.is_empty() {
            tracing::warn!(
                ?failures,
                "leaderboard background refresh partially failed - cached data keeps serving"
            );
        }
    }

    /// What a refresh pass still owes right now: never-fetched regions
    /// always parse; `StaleOrMissing` re-parses TTL-expired entries too
    /// (on-demand mode and the background job), `MissingOnly` leaves
    /// cached data alone however old (the background mode's command path).
    /// A recently failed names fetch holds the inline (MissingOnly) retry
    /// back; the job and on-demand commands keep retrying every cycle.
    fn pending_refresh(&self, settings: &EngineSettings, scope: RefreshScope) -> PendingRefresh {
        let now = unix_now();
        let state = self.cache.lock();
        let regions = settings
            .regions
            .iter()
            .filter(|region| match state.regions.get(*region) {
                Some(entry) => {
                    scope == RefreshScope::StaleOrMissing
                        && is_stale(entry.fetched_at, settings.cache_ttl, now)
                }
                None => true,
            })
            .cloned()
            .collect();
        let names = match state.names_fetched_at {
            Some(at) => {
                scope == RefreshScope::StaleOrMissing && is_stale(at, settings.cache_ttl, now)
            }
            None => match scope {
                RefreshScope::StaleOrMissing => true,
                RefreshScope::MissingOnly => state
                    .names_retry_not_before
                    .is_none_or(|not_before| Instant::now() >= not_before),
            },
        };
        PendingRefresh { regions, names }
    }

    /// The shared fetch loop: champion names, then each pending region,
    /// paced, each region a complete build-then-swap into the cache
    /// (readers never see a half-walked region). `budget` bounds the wall
    /// clock (command-path cycles): the names fetch is cut off at the
    /// deadline and a region that would start past it is named as a
    /// failure, keeping whatever data it already has. Returns the failed
    /// region names and the last error (a source error, or the budget
    /// marker).
    async fn run_refresh(
        &self,
        settings: &EngineSettings,
        pending: &PendingRefresh,
        budget: Option<Duration>,
    ) -> (Vec<String>, Option<SourceError>) {
        let deadline = budget.map(|budget| Instant::now() + budget);
        let mut failures: Vec<String> = Vec::new();
        let mut last_error: Option<SourceError> = None;
        let mut paced = false;

        if pending.names {
            paced = true;
            // The names fetch rides the budget like a region: a hung names
            // endpoint must not park the invoker past the deadline (the
            // background job runs unbudgeted, per its contract).
            let attempt = self.source.champion_names();
            let outcome = match deadline {
                Some(deadline) => {
                    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), attempt)
                        .await
                        .map_err(|_| {
                            SourceError::Request("inline refresh budget exceeded".to_owned())
                        })
                        .and_then(|inner| inner)
                }
                None => attempt.await,
            };
            match outcome {
                Ok(names) => {
                    let mut state = self.cache.lock();
                    state.champ_names = names;
                    state.names_fetched_at = Some(unix_now());
                    state.names_retry_not_before = None;
                }
                Err(err) => {
                    // Old names (if any) keep serving; ids missing from the
                    // map degrade to placeholders downstream. The failure
                    // holds the inline retry back for a short window so a
                    // down endpoint is not re-hit by every command.
                    tracing::warn!(error = %err, "leaderboard champion names refresh failed");
                    let mut state = self.cache.lock();
                    state.names_retry_not_before = Some(Instant::now() + NAMES_FAILURE_RETRY);
                }
            }
        }

        for region in &pending.regions {
            if paced {
                tokio::time::sleep(settings.request_interval).await;
            }
            paced = true;
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                tracing::warn!(
                    region = %region,
                    "leaderboard inline refresh budget exceeded - region keeps its cached data"
                );
                failures.push(region.clone());
                last_error.get_or_insert_with(|| {
                    SourceError::Request("inline refresh budget exceeded".to_owned())
                });
                continue;
            }
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
                        .insert(region.clone(), CachedRegion { data, fetched_at: unix_now() });
                }
                Err(err) => {
                    tracing::warn!(region = %region, error = %err, "leaderboard region refresh failed");
                    failures.push(region.clone());
                    last_error = Some(err);
                }
            }
        }

        // The cycle changed (or attempted to change) the dump: persist it
        // so a restart keeps serving whatever this cycle left behind.
        self.persist().await;

        (failures, last_error)
    }
}

/// What a refresh pass still owes: never-fetched regions always parse;
/// the scope decides whether TTL-expired ones re-parse too.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RefreshScope {
    /// On-demand mode and the background job: age past the TTL re-parses.
    StaleOrMissing,
    /// Background mode's command path: cached data serves however old.
    MissingOnly,
}

/// Regions and the champion-name table one refresh pass must parse.
struct PendingRefresh {
    regions: Vec<String>,
    names: bool,
}

/// Scheduler job body: one background refresh per cache-TTL tick (first
/// tick immediate - the boot warm-up runs before the first user can ask).
pub struct RefreshJob {
    pub engine: Arc<LeaderboardEngine>,
}

#[async_trait]
impl Job for RefreshJob {
    async fn run(&self) {
        self.engine.refresh_stale().await;
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
        /// When set, the champion-names fetch fails (names retry backoff).
        pub failing_names: StdMutex<bool>,
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
                failing_names: StdMutex::new(false),
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

        pub fn fail_names(&self) {
            *self.failing_names.lock().expect("failing_names") = true;
        }

        pub fn heal_names(&self) {
            *self.failing_names.lock().expect("failing_names") = false;
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
            if *self.failing_names.lock().expect("failing_names") {
                return Err(SourceError::Request("names endpoint down".to_owned()));
            }
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
    use crate::kernel::spi_ports::PluginStoragePort;
    use crate::plugins::lol_leaderboard::port::Role;
    use crate::test_support::{
        FailingPluginStorage, InMemoryPluginStorage, TEST_PLATFORM_SLUG, test_plugin_storage,
    };

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
            false,
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

    /// Engine with full mode control (pacing, background vs on-demand,
    /// inline budget) - the shape the background-refresh tests need.
    fn engine_mode(
        source: &Arc<FakeSource>,
        regions: &[&str],
        ttl: Duration,
        interval: Duration,
        background: bool,
        budget: Duration,
    ) -> Arc<LeaderboardEngine> {
        let keys: Vec<String> = regions.iter().map(|key| (*key).to_owned()).collect();
        let settings = EngineSettings::new(
            source.as_ref(),
            &keys,
            1000,
            ttl,
            interval,
            background,
            budget,
            ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        );
        Arc::new(LeaderboardEngine::new(
            Arc::clone(source) as Arc<dyn LeaderboardSourcePort>,
            settings,
        ))
    }

    /// A stale region must refetch: without this, the dump would serve
    /// frozen data forever after one failed refresh cycle. Timestamps are
    /// unix seconds (they must survive restarts), so the TTL granularity
    /// is one second - the test uses a 2s TTL and crosses it for real.
    #[tokio::test]
    async fn ttl_expiry_triggers_a_refetch() {
        let source = FakeSource::ungated();
        let engine = engine_with(&source, &["kr"], Duration::from_secs(2));
        let _ = engine.snapshot().await.expect("first snapshot");
        let _ = engine.snapshot().await.expect("fresh snapshot");
        assert_eq!(source.leaderboard_calls(), 1, "fresh cache answers without the source");

        tokio::time::sleep(Duration::from_millis(2100)).await;
        let _ = engine.snapshot().await.expect("stale snapshot");
        assert_eq!(source.leaderboard_calls(), 2, "TTL expiry must trigger a refetch");
    }

    /// Background mode serves whatever is cached, however stale (a zero
    /// TTL makes everything perpetually stale - the job's predicate): a
    /// warm cache answers with zero source calls, because re-parsing is
    /// the background job's job, never the command's wait.
    #[tokio::test]
    async fn background_mode_serves_stale_cache_without_source_calls() {
        let source = FakeSource::ungated();
        let engine = engine_mode(
            &source,
            &["kr", "euw"],
            Duration::ZERO,
            Duration::ZERO,
            true,
            Duration::ZERO,
        );
        engine.refresh_stale().await;
        let calls_after_warmup = source.leaderboard_calls();
        assert_eq!(calls_after_warmup, 2, "the warm-up parsed both regions");

        let snapshot = engine.snapshot().await.expect("cached data serves however stale");

        assert_eq!(snapshot.regions.len(), 2);
        assert!(snapshot.failures.is_empty());
        assert_eq!(source.leaderboard_calls(), calls_after_warmup, "no inline re-parse");
    }

    /// The boot race: with nothing cached, the background-mode command
    /// refreshes the never-fetched regions inline (under the singleflight)
    /// so the first user after boot still gets an answer.
    #[tokio::test]
    async fn background_mode_refreshes_never_fetched_regions_inline() {
        let source = FakeSource::ungated();
        let engine = engine_mode(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            Duration::ZERO,
            true,
            Duration::ZERO,
        );

        let snapshot = engine.snapshot().await.expect("inline fill expected");

        assert_eq!(snapshot.regions.len(), 1);
        assert_eq!(source.leaderboard_calls(), 1, "the missing region parsed inline");
        assert_eq!(source.name_calls(), 1, "names parsed inline too");
    }

    /// On-demand mode (`background_refresh = false`) keeps the legacy
    /// contract: a stale cache re-parses before the reply.
    #[tokio::test]
    async fn on_demand_mode_still_re_parses_stale_regions() {
        let source = FakeSource::ungated();
        let engine =
            engine_mode(&source, &["kr"], Duration::ZERO, Duration::ZERO, false, Duration::ZERO);

        engine.snapshot().await.expect("first fill");
        engine.snapshot().await.expect("second fill");

        assert_eq!(source.leaderboard_calls(), 2, "every stale invocation re-parses");
    }

    /// The inline budget cuts a slow command-path refresh: regions past
    /// the deadline keep their cached data or are named as failures - the
    /// invoker is never parked behind a slow source. Pacing between
    /// regions consumes the budget here (40 ms pacing vs a 30 ms budget).
    #[tokio::test]
    async fn inline_budget_cuts_remaining_regions_and_names_their_failure() {
        let source = FakeSource::ungated();
        // An empty-region cycle parses only the champion names (the region
        // list is empty on purpose), so the names table warms up first.
        let engine = engine_mode(
            &source,
            &[],
            Duration::from_secs(3600),
            Duration::from_millis(40),
            false,
            Duration::from_millis(30),
        );
        assert!(engine.snapshot().await.is_err(), "no regions configured yet");
        assert_eq!(source.name_calls(), 1);

        // Widen the window to two regions: kr parses instantly (well under
        // the budget), euw is past the deadline after the 40 ms pacing.
        engine.update_settings(EngineSettings {
            regions: vec!["kr".to_owned(), "euw".to_owned()],
            parse_depth: 1000,
            cache_ttl: Duration::from_secs(3600),
            request_interval: Duration::from_millis(40),
            background_refresh: false,
            inline_budget: Duration::from_millis(30),
            view: ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        });
        let snapshot = engine.snapshot().await.expect("kr serves within the budget");

        assert_eq!(snapshot.regions.len(), 1, "only kr parsed within the budget");
        assert_eq!(snapshot.failures, vec!["euw".to_owned()], "euw is named as failed");
        assert!(snapshot.stale);
    }

    /// When the budget kills every region, the command errors with the
    /// budget marker instead of hanging or silently serving nothing.
    #[tokio::test]
    async fn inline_budget_total_cut_errors_with_the_budget_marker() {
        let source = FakeSource::ungated();
        // Names parse first; the 40 ms pacing before the only region
        // outlives the 30 ms budget, so kr is cut and nothing serves.
        let engine = engine_mode(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            Duration::from_millis(40),
            false,
            Duration::from_millis(30),
        );

        let err = engine.snapshot().await.expect_err("budget cut expected");

        assert!(err.to_string().contains("budget"), "error: {err}");
    }

    /// The background job re-parses what the TTL expired and is a cheap
    /// no-op when everything is fresh.
    #[tokio::test]
    async fn refresh_stale_skips_fresh_regions() {
        let source = FakeSource::ungated();
        let engine = engine_mode(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            Duration::ZERO,
            true,
            Duration::ZERO,
        );
        engine.refresh_stale().await;
        let calls = source.leaderboard_calls();

        engine.refresh_stale().await;

        assert_eq!(source.leaderboard_calls(), calls, "fresh regions do not re-parse");
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
            false,
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
            background_refresh: false,
            inline_budget: Duration::ZERO,
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
            background_refresh: false,
            inline_budget: Duration::ZERO,
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

    /// The inline budget also cuts in background mode: never-fetched
    /// regions past the deadline are named as failures instead of parking
    /// the invoker - the boot race stays bounded.
    #[tokio::test]
    async fn background_mode_inline_budget_cuts_missing_regions() {
        let source = FakeSource::ungated();
        // Warm the names table first with an empty-region cycle (same shape
        // as the on-demand budget test): the region cycle then paces only
        // between regions - kr parses under the budget, the 40 ms pacing
        // before euw outlives the 30 ms one.
        let engine = engine_mode(
            &source,
            &[],
            Duration::from_secs(3600),
            Duration::from_millis(40),
            true,
            Duration::from_millis(30),
        );
        assert!(engine.snapshot().await.is_err(), "no regions configured yet");
        assert_eq!(source.name_calls(), 1);

        engine.update_settings(EngineSettings {
            regions: vec!["kr".to_owned(), "euw".to_owned()],
            parse_depth: 1000,
            cache_ttl: Duration::from_secs(3600),
            request_interval: Duration::from_millis(40),
            background_refresh: true,
            inline_budget: Duration::from_millis(30),
            view: ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        });

        let snapshot = engine.snapshot().await.expect("kr serves within the budget");

        assert_eq!(snapshot.regions.len(), 1, "only kr parsed within the budget");
        assert_eq!(snapshot.failures, vec!["euw".to_owned()], "euw is named as failed");
        assert!(snapshot.stale);
    }

    /// A failed names fetch holds the inline (command-path) retry back: a
    /// down names endpoint is not re-hit by every background-mode command.
    /// The next attempt succeeds once the short backoff lapses.
    #[tokio::test]
    async fn names_failure_backs_off_inline_retries_in_background_mode() {
        let source = FakeSource::ungated();
        source.fail_names();
        let engine = engine_mode(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            Duration::ZERO,
            true,
            Duration::ZERO,
        );

        let first = engine.snapshot().await.expect("regions serve despite the names failure");
        assert_eq!(first.regions.len(), 1);
        assert!(first.champ_names.is_empty(), "no names were fetched");
        assert_eq!(source.name_calls(), 1);

        // Within the backoff window the command path does not retry.
        engine.snapshot().await.expect("cached data serves");
        assert_eq!(source.name_calls(), 1, "the failure backoff holds the retry");

        // After the window (simulated: the clock is not worth waiting for)
        // the retry succeeds and the table fills.
        source.heal_names();
        engine.cache.lock().names_retry_not_before = None;
        let healed = engine.snapshot().await.expect("snapshot expected");
        assert_eq!(source.name_calls(), 2);
        assert_eq!(healed.champ_names.get("1").map(String::as_str), Some("Annie"));
    }

    /// Hot reload swaps only the data window: the background mode and the
    /// inline budget are boot decisions and survive `update_settings`
    /// unchanged (a boot-time background/budget setup must not be silently
    /// switched off by a config edit).
    #[test]
    fn update_settings_keeps_background_mode_and_budget() {
        let source = Arc::new(FakeSource::ungated());
        let engine = engine_mode(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            Duration::ZERO,
            true,
            Duration::from_secs(180),
        );

        engine.update_settings(EngineSettings {
            regions: vec!["euw".to_owned()],
            parse_depth: 1000,
            cache_ttl: Duration::from_secs(3600),
            request_interval: Duration::ZERO,
            background_refresh: false,
            inline_budget: Duration::ZERO,
            view: ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        });

        let settings = engine.settings();
        assert_eq!(settings.regions, vec!["euw"], "the data window swapped");
        assert!(settings.background_refresh, "background mode stays boot-frozen");
        assert_eq!(settings.inline_budget, Duration::from_secs(180), "budget stays boot-frozen");
    }

    /// The two refresh drivers share the singleflight: a command landing
    /// while the background job is mid-cycle waits, then finds the cache
    /// fresh - never a second parse of the same region.
    #[tokio::test]
    async fn job_and_command_share_one_refresh() {
        let source = FakeSource::new(); // gated: calls block until opened
        let engine = engine_mode(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            Duration::ZERO,
            true,
            Duration::ZERO,
        );

        let job_engine = Arc::clone(&engine);
        let job = tokio::spawn(async move { job_engine.refresh_stale().await });
        while source.leaderboard_calls() == 0 {
            tokio::task::yield_now().await;
        }
        let cmd_engine = Arc::clone(&engine);
        let command = tokio::spawn(async move { cmd_engine.snapshot().await });
        tokio::task::yield_now().await;
        source.open_gate();

        job.await.expect("job joins");
        let snapshot = command.await.expect("command joins").expect("snapshot");

        assert_eq!(source.leaderboard_calls(), 1, "the command shared the job's cycle");
        assert_eq!(snapshot.regions.len(), 1);
        assert!(snapshot.failures.is_empty());
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

    /// Engine backed by real (in-memory) plugin storage - the shape the
    /// persistence tests need.
    fn engine_with_storage(
        source: &Arc<FakeSource>,
        regions: &[&str],
        ttl: Duration,
        background: bool,
        storage: Arc<dyn PluginStorage>,
    ) -> Arc<LeaderboardEngine> {
        let keys: Vec<String> = regions.iter().map(|key| (*key).to_owned()).collect();
        let settings = EngineSettings::new(
            source.as_ref(),
            &keys,
            1000,
            ttl,
            Duration::ZERO,
            background,
            Duration::ZERO,
            ResolvedView::resolve(1000, &[300, 1000], 1000, 5),
        );
        Arc::new(LeaderboardEngine::with_storage(
            Arc::clone(source) as Arc<dyn LeaderboardSourcePort>,
            settings,
            storage,
        ))
    }

    /// The dump persists across a restart: a fresh engine over the same
    /// storage serves the restored regions with zero source calls while
    /// they are inside the TTL.
    #[tokio::test]
    async fn dump_persists_and_restores_across_a_restart() {
        let storage = test_plugin_storage();
        let first_source = FakeSource::ungated();
        let first = engine_with_storage(
            &first_source,
            &["kr"],
            Duration::from_secs(3600),
            true,
            Arc::clone(&storage),
        );
        let first_snapshot = first.snapshot().await.expect("first life parses");
        assert_eq!(first_source.leaderboard_calls(), 1);
        assert_eq!(first_snapshot.regions.len(), 1);

        // The persisted document is there, stamped with a completion time.
        let saved = storage
            .get(STORAGE_NAMESPACE, STORAGE_KEY)
            .await
            .expect("read")
            .expect("dump persisted");
        assert!(saved.get("saved_at").and_then(|value| value.as_u64()).is_some_and(|at| at > 0));

        // Second life over the same storage: restored, not re-parsed.
        let second_source = FakeSource::ungated();
        let second =
            engine_with_storage(&second_source, &["kr"], Duration::from_secs(3600), true, storage);
        let snapshot = second.snapshot().await.expect("restored dump serves");

        assert_eq!(second_source.leaderboard_calls(), 0, "inside the TTL the dump serves as-is");
        assert_eq!(second_source.name_calls(), 0);
        assert_eq!(snapshot.regions, first_snapshot.regions);
        assert!(snapshot.age < Duration::from_secs(5), "a fresh dump's age is ~zero");
        assert!(!snapshot.stale);
    }

    /// A restored dump keeps its true age: the persisted parse timestamps
    /// are unix seconds, so an hours-old dump says so (and shows stale
    /// past the TTL) instead of looking freshly parsed.
    #[tokio::test]
    async fn restored_dump_keeps_its_true_age() {
        let backend = InMemoryPluginStorage::new();
        let seeded_at = unix_now() - 7200;
        backend.seed(
            TEST_PLATFORM_SLUG,
            STORAGE_NAMESPACE,
            STORAGE_KEY,
            serde_json::json!({
                "saved_at": seeded_at,
                "regions": [{
                    "region": "kr",
                    "fetched_at": seeded_at,
                    "players": [
                        {"position": 1, "role": "top", "champ_id": "1"},
                        {"position": 2, "role": "mid", "champ_id": null}
                    ]
                }],
                "champ_names": {"1": "Annie"}
            }),
        );
        let source = FakeSource::ungated();
        let engine = engine_with_storage(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            true,
            backend.plugin_scoped(TEST_PLATFORM_SLUG),
        );

        let snapshot = engine.snapshot().await.expect("restored dump serves");

        assert_eq!(source.leaderboard_calls(), 0, "the dump serves without the source");
        assert_eq!(snapshot.regions.len(), 1);
        assert_eq!(snapshot.regions.first().expect("region").players.len(), 2);
        assert_eq!(snapshot.champ_names.get("1").map(String::as_str), Some("Annie"));
        // 2h old, give or take the second the test itself consumed.
        assert!(snapshot.age >= Duration::from_secs(7200));
        assert!(snapshot.age < Duration::from_secs(7300));
        assert!(snapshot.stale, "2h is past the 1h TTL - the age must show");
    }

    /// A persisted document that does not parse is skipped permanently
    /// (it cannot heal on its own) - the engine starts empty and behaves
    /// exactly like a first boot.
    #[tokio::test]
    async fn corrupt_dump_starts_empty_and_still_answers() {
        let backend = InMemoryPluginStorage::new();
        backend.seed(TEST_PLATFORM_SLUG, STORAGE_NAMESPACE, STORAGE_KEY, serde_json::json!("junk"));
        let source = FakeSource::ungated();
        let engine = engine_with_storage(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            true,
            backend.plugin_scoped(TEST_PLATFORM_SLUG),
        );

        let snapshot = engine.snapshot().await.expect("a corrupt dump must not fail the command");

        assert_eq!(source.leaderboard_calls(), 1, "behaves like a first boot");
        assert_eq!(snapshot.regions.len(), 1);
    }

    /// Persistence is best-effort: a failing storage never fails the
    /// reply - the refresh serves its parsed data and only the write is
    /// lost. Once the storage heals, the next cycle's write lands and a
    /// restarted engine restores the dump.
    #[tokio::test]
    async fn storage_write_failure_does_not_break_the_cycle() {
        let storage = FailingPluginStorage::new();
        let source = FakeSource::ungated();
        // A zero TTL makes every snapshot a full refresh cycle, so each
        // one attempts a dump write.
        let engine = engine_with_storage(
            &source,
            &["kr"],
            Duration::ZERO,
            false,
            storage.plugin_scoped(TEST_PLATFORM_SLUG),
        );

        let snapshot =
            engine.snapshot().await.expect("the reply must not depend on the dump write");
        assert_eq!(snapshot.regions.len(), 1);
        assert_eq!(source.leaderboard_calls(), 1);

        // Storage heals: the next cycle's write lands, and a restarted
        // engine restores the dump without touching the source.
        storage.set_failing(false);
        let _ = engine.snapshot().await.expect("second cycle");
        let restarted = engine_with_storage(
            &source,
            &["kr"],
            Duration::from_secs(3600),
            true,
            storage.plugin_scoped(TEST_PLATFORM_SLUG),
        );
        let restored = restarted.snapshot().await.expect("restored dump serves");
        assert_eq!(restored.regions.len(), 1);
        assert_eq!(source.leaderboard_calls(), 2, "no re-parse for the restored engine");
    }
}
