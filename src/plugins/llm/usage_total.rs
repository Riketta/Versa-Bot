//! Plugin-global LLM token usage tracking: cumulative totals per model, per
//! guild, and per UTC day, persisted to the plugin-global document store
//! ([`PluginStorage`]) as one document per view. This is operator-side
//! observability - counters and model/guild ids only, never user content.
//!
//! Storage shapes (the external contract a graphing tool reads):
//!
//! - `usage_model_totals`: `{"entries":[{"dim":"model","key":"<model>",..}]}`
//!   - all-time totals per model.
//! - `usage_guild_totals:<guild_id>`: the same entry shape - all-time totals
//!   per model within one guild (the guild is the document's key).
//! - `usage_daily:<YYYY-MM-DD>`: `{"day":"..","entries":[..]}` - one UTC
//!   day's entries, `dim:"model"` (deployment-wide) and `dim:"guild"`
//!   (per-guild aggregate). Past days are never rewritten except for one
//!   final post-midnight flush.
//!
//! Counters per entry: `requests`, `prompt_tokens`, `completion_tokens`,
//! `cached_tokens`, `reasoning_tokens`. Cached and reasoning tokens are
//! breakdowns the endpoint reported - cached is a subset of prompt, never
//! additive. In-memory state is the live truth; a 60s scheduler job flushes
//! dirty views, so a crash loses at most one interval (and a day at most its
//! last interval before midnight) - "approximate" by design.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::OnceCell;

use crate::kernel::models::StorageError;
use crate::kernel::spi_ports::PluginStorage;
use crate::plugins::llm::completion_port::TokenUsage;
use crate::plugins::llm::model::NAMESPACE;

/// All-time totals document under the plugin namespace.
pub(super) const USAGE_MODEL_TOTALS_KEY: &str = "usage_model_totals";
/// Per-guild all-time documents, one document per guild (the guild id is the
/// key suffix), so a guild-scoped read structurally cannot see another
/// guild's numbers.
const USAGE_GUILD_TOTALS_PREFIX: &str = "usage_guild_totals:";
/// One document per UTC day - the time series external tools graph.
const USAGE_DAILY_PREFIX: &str = "usage_daily:";

/// How long between flush ticks - a constant, not config: the flush is
/// internal hygiene, and a crash losing up to one interval is the documented
/// "approximate" contract.
pub(super) const FLUSH_INTERVAL_SECS: u64 = 60;

pub(super) fn usage_guild_totals_key(guild_id: u64) -> String {
    format!("{USAGE_GUILD_TOTALS_PREFIX}{guild_id}")
}

pub(super) fn usage_daily_key(day: &str) -> String {
    format!("{USAGE_DAILY_PREFIX}{day}")
}

/// Today's UTC date as the daily documents' `YYYY-MM-DD` key. UTC on
/// purpose: day boundaries must not move with the host timezone.
pub(super) fn current_day() -> String {
    let date = OffsetDateTime::now_utc().date();
    format!("{:04}-{:02}-{:02}", date.year(), u8::from(date.month()), date.day())
}

/// One completion's contribution - the shape [`UsageTracker::record`] takes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Sample {
    pub requests: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,
}

impl Sample {
    /// One completion with an endpoint-reported usage breakdown.
    pub(super) fn reported(usage: &TokenUsage) -> Self {
        Self {
            requests: 1,
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            cached_tokens: usage.cached_tokens.unwrap_or(0),
            reasoning_tokens: usage.reasoning_tokens.unwrap_or(0),
        }
    }

    /// One completion the endpoint reported no usage for: both sides are
    /// estimated from the channel's calibrated tokens-per-character ratio
    /// (prompt context and answer text are the only measures available).
    /// Vision calls have no meaningful character measure (image bytes
    /// dominate) and stay uncounted instead of fabricated.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)] // estimator: precision loss is fine
    pub(super) fn estimated(
        context_chars: u64,
        completion_chars: u64,
        tokens_per_char: f64,
    ) -> Self {
        let estimate = |chars: u64| (chars as f64 * tokens_per_char) as u64;
        Self {
            requests: 1,
            prompt_tokens: estimate(context_chars),
            completion_tokens: estimate(completion_chars),
            cached_tokens: 0,
            reasoning_tokens: 0,
        }
    }

    /// A request nobody can account tokens for (vision without a usage
    /// report): still a request, zero tokens.
    pub(super) fn unreported_request() -> Self {
        Self { requests: 1, ..Self::default() }
    }
}

/// Aggregated counters for one view (a model, or a guild).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Dimension {
    pub requests: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,
}

impl Dimension {
    fn add(&mut self, sample: Sample) {
        // Saturating on purpose: counters are observability - a wrapped or
        // panicking u64 would be worse than a pinned-at-max total.
        self.requests = self.requests.saturating_add(sample.requests);
        self.prompt_tokens = self.prompt_tokens.saturating_add(sample.prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(sample.completion_tokens);
        self.cached_tokens = self.cached_tokens.saturating_add(sample.cached_tokens);
        self.reasoning_tokens = self.reasoning_tokens.saturating_add(sample.reasoning_tokens);
    }

    fn merge(&mut self, other: Self) {
        self.requests = self.requests.saturating_add(other.requests);
        self.prompt_tokens = self.prompt_tokens.saturating_add(other.prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(other.completion_tokens);
        self.cached_tokens = self.cached_tokens.saturating_add(other.cached_tokens);
        self.reasoning_tokens = self.reasoning_tokens.saturating_add(other.reasoning_tokens);
    }
}

/// One guild's usage: the aggregate plus its per-model breakdown (the
/// breakdown is what `/llm_usage` renders; the aggregate is what the daily
/// document stores).
#[derive(Debug, Clone, Default)]
struct GuildUsage {
    total: Dimension,
    models: BTreeMap<String, Dimension>,
}

impl GuildUsage {
    fn add(&mut self, model: &str, sample: Sample) {
        self.total.add(sample);
        self.models.entry(model.to_owned()).or_default().add(sample);
    }

    fn merge_doc(&mut self, doc: TotalsDoc) {
        for entry in doc.entries {
            if entry.dim == EntryDim::Model {
                let dimension = entry.dimension();
                self.total.merge(dimension);
                self.models.entry(entry.key).or_default().merge(dimension);
            }
        }
    }
}

// --- persisted document schema -------------------------------------------

/// Which view an entry belongs to (the daily document mixes both).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum EntryDim {
    Model,
    Guild,
}

/// One persisted counter row.
#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    dim: EntryDim,
    key: String,
    requests: u64,
    prompt_tokens: u64,
    completion_tokens: u64,
    cached_tokens: u64,
    reasoning_tokens: u64,
}

impl Entry {
    fn model(key: &str, dimension: &Dimension) -> Self {
        Self::row(EntryDim::Model, key.to_owned(), *dimension)
    }

    fn guild(guild_id: u64, dimension: &Dimension) -> Self {
        Self::row(EntryDim::Guild, guild_id.to_string(), *dimension)
    }

    fn row(dim: EntryDim, key: String, dimension: Dimension) -> Self {
        Self {
            dim,
            key,
            requests: dimension.requests,
            prompt_tokens: dimension.prompt_tokens,
            completion_tokens: dimension.completion_tokens,
            cached_tokens: dimension.cached_tokens,
            reasoning_tokens: dimension.reasoning_tokens,
        }
    }

    fn dimension(&self) -> Dimension {
        Dimension {
            requests: self.requests,
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
            cached_tokens: self.cached_tokens,
            reasoning_tokens: self.reasoning_tokens,
        }
    }
}

/// The persisted document envelope - flat entries so external tools can
/// project rows without nested-schema knowledge. `day` is present only in
/// daily documents.
#[derive(Debug, Serialize, Deserialize)]
struct TotalsDoc {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    day: Option<String>,
    #[serde(default)]
    entries: Vec<Entry>,
}

impl TotalsDoc {
    fn entries(entries: Vec<Entry>) -> Self {
        Self { day: None, entries }
    }
}

/// One UTC day's in-memory bucket.
#[derive(Debug, Default)]
struct DayBucket {
    models: BTreeMap<String, Dimension>,
    guilds: BTreeMap<u64, Dimension>,
}

impl DayBucket {
    fn add(&mut self, model: &str, guild_id: Option<u64>, sample: Sample) {
        self.models.entry(model.to_owned()).or_default().add(sample);
        if let Some(guild_id) = guild_id {
            self.guilds.entry(guild_id).or_default().add(sample);
        }
    }

    fn merge_doc(&mut self, doc: TotalsDoc) {
        for entry in doc.entries {
            match entry.dim {
                EntryDim::Model => {
                    let dimension = entry.dimension();
                    self.models.entry(entry.key).or_default().merge(dimension);
                }
                EntryDim::Guild => {
                    if let Ok(guild_id) = entry.key.parse::<u64>() {
                        let dimension = entry.dimension();
                        self.guilds.entry(guild_id).or_default().merge(dimension);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    models: BTreeMap<String, Dimension>,
    guilds: BTreeMap<u64, GuildUsage>,
    days: BTreeMap<String, DayBucket>,
    dirty_models: bool,
    dirty_guilds: BTreeSet<u64>,
    dirty_days: BTreeSet<String>,
    /// Highest day a record ever landed in - a clock stepping backwards
    /// must not reopen a finalized day (its bucket is gone; a fresh empty
    /// one would overwrite the stored document on the next flush).
    last_day: Option<String>,
}

/// The live usage truth: in-memory counters, lazily seeded from storage
/// (merge-add, so records that landed before a successful seed are never
/// lost), flushed by the plugin's scheduler job. The clock is injected so
/// day-boundary behavior (rollover, finalized-day protection) is testable.
pub(crate) struct UsageTracker {
    storage: Arc<dyn PluginStorage>,
    inner: Mutex<Inner>,
    seeded: OnceCell<()>,
    now_day: Arc<dyn Fn() -> String + Send + Sync>,
}

impl UsageTracker {
    pub(crate) fn new(storage: Arc<dyn PluginStorage>) -> Arc<Self> {
        Self::with_clock(storage, Arc::new(current_day))
    }

    fn with_clock(
        storage: Arc<dyn PluginStorage>,
        now_day: Arc<dyn Fn() -> String + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            inner: Mutex::new(Inner::default()),
            seeded: OnceCell::new(),
            now_day,
        })
    }

    fn day(&self) -> String {
        (self.now_day)()
    }

    /// Whether any view is waiting to be persisted - the regression probe
    /// for the flush's consume-on-snapshot dirty handling.
    #[cfg(test)]
    fn is_dirty(&self) -> bool {
        let inner = self.inner.lock();
        inner.dirty_models || !inner.dirty_guilds.is_empty() || !inner.dirty_days.is_empty()
    }

    /// Loads the stored totals once per process. Merge-add into whatever is
    /// already in memory: records must never be lost to a late seed, and a
    /// successful seed runs exactly once, so double-counting is impossible.
    /// Failure leaves the tracker unseeded (flush refuses to run) and is
    /// retried on the next call - counters keep accumulating meanwhile.
    pub(crate) async fn ensure_seeded(&self) {
        let storage = Arc::clone(&self.storage);
        let _ignored = self
            .seeded
            .get_or_try_init(|| async { Self::seed(self, &storage).await })
            .await
            .inspect_err(|()| tracing::debug!("usage totals seed failed - retrying on next use"));
    }

    async fn seed(&self, storage: &Arc<dyn PluginStorage>) -> Result<(), ()> {
        // All-time models.
        let models_doc = read_doc(storage, USAGE_MODEL_TOTALS_KEY).await?;
        // All-time guilds: discover the per-guild documents by key.
        let mut guilds = BTreeMap::new();
        let keys = storage.list_keys(NAMESPACE).await.map_err(|_| ())?;
        for key in keys {
            let Some(guild_id) =
                key.strip_prefix(USAGE_GUILD_TOTALS_PREFIX).and_then(|id| id.parse::<u64>().ok())
            else {
                continue;
            };
            let doc = read_doc(storage, &key).await?;
            let mut usage = GuildUsage::default();
            usage.merge_doc(doc);
            guilds.insert(guild_id, usage);
        }
        // Today's bucket (yesterday and older stay on disk as finalized).
        let today = self.day();
        let mut days = BTreeMap::new();
        if let Some(doc) = read_doc_inner(storage, &usage_daily_key(&today)).await? {
            let mut bucket = DayBucket::default();
            bucket.merge_doc(doc);
            days.insert(today, bucket);
        }

        let mut inner = self.inner.lock();
        for (model, dimension) in models_doc.entries.into_iter().map(|entry| {
            let dimension = entry.dimension();
            (entry.key, dimension)
        }) {
            inner.models.entry(model).or_default().merge(dimension);
        }
        for (guild_id, usage) in guilds {
            merge_guild(inner.guilds.entry(guild_id).or_default(), usage);
        }
        for (day, bucket) in days {
            merge_day(inner.days.entry(day).or_default(), bucket);
        }
        Ok(())
    }

    /// Records one completion and returns the model's updated all-time
    /// totals (what the audit log line reports as cumulative). A record
    /// dated BEFORE the highest day ever seen (clock stepped backwards
    /// after a rollover) is counted in the all-time views but not
    /// re-opened into the daily series - that day is finalized on disk,
    /// and rewriting it from a partial bucket would destroy its history.
    pub(crate) fn record(&self, model: &str, guild_id: Option<u64>, sample: Sample) -> Dimension {
        let today = self.day();
        let mut inner = self.inner.lock();
        let cumulative = {
            let total = inner.models.entry(model.to_owned()).or_default();
            total.add(sample);
            *total
        };
        if let Some(guild_id) = guild_id {
            inner.guilds.entry(guild_id).or_default().add(model, sample);
            inner.dirty_guilds.insert(guild_id);
        }
        let reopens_finalized_day =
            inner.last_day.as_ref().is_some_and(|last| today.as_str() < last.as_str());
        if reopens_finalized_day {
            tracing::warn!(
                day = %today,
                last = ?inner.last_day,
                "clock stepped backwards - usage counted in totals, not re-opened into the daily series"
            );
        } else {
            inner.last_day = Some(today.clone());
            inner.days.entry(today.clone()).or_default().add(model, guild_id, sample);
            inner.dirty_days.insert(today);
        }
        inner.dirty_models = true;
        cumulative
    }

    /// The guild's all-time totals plus today's aggregate - what
    /// `/llm_usage` renders. Seeding runs first, so a fresh process serves
    /// history.
    pub(crate) async fn guild_snapshot(&self, guild_id: u64) -> (GuildUsageSnapshot, Dimension) {
        self.ensure_seeded().await;
        let today = self.day();
        let inner = self.inner.lock();
        let all_time = inner
            .guilds
            .get(&guild_id)
            .map(|usage| GuildUsageSnapshot {
                total: usage.total,
                models: usage.models.iter().map(|(model, dim)| (model.clone(), *dim)).collect(),
            })
            .unwrap_or_default();
        let today_total =
            inner.days.get(&today).and_then(|bucket| bucket.guilds.get(&guild_id)).copied();
        (all_time, today_total.unwrap_or_default())
    }

    /// Deployment-wide all-time usage plus today's aggregate - what
    /// `/llm_usage_global` renders (owner-tier, ephemeral). The global
    /// total folds the per-model view; the per-server list carries each
    /// guild's all-time aggregate for the ranking. Seeding runs first.
    pub(crate) async fn global_snapshot(&self) -> (GlobalUsageSnapshot, Dimension) {
        self.ensure_seeded().await;
        let today = self.day();
        let inner = self.inner.lock();
        let models: Vec<(String, Dimension)> =
            inner.models.iter().map(|(model, dim)| (model.clone(), *dim)).collect();
        let mut total = Dimension::default();
        for (_, dim) in &models {
            total.merge(*dim);
        }
        let guilds: Vec<(u64, Dimension)> =
            inner.guilds.iter().map(|(id, usage)| (*id, usage.total)).collect();
        let mut today_total = Dimension::default();
        if let Some(bucket) = inner.days.get(&today) {
            for dim in bucket.models.values() {
                today_total.merge(*dim);
            }
        }
        (GlobalUsageSnapshot { total, models, guilds }, today_total)
    }

    /// The stored all-time totals for one model - what the vision audit
    /// line reports as cumulative.
    pub(crate) fn cumulative(&self, model: &str) -> Dimension {
        *self.inner.lock().models.get(model).unwrap_or(&Dimension::default())
    }

    /// Flushes every dirty view to storage. Refuses to run unseeded - a
    /// partial in-memory view must never overwrite stored history. Each
    /// document is a whole-view upsert; the dirty flags are consumed in the
    /// snapshot critical section and re-marked only for failed writes, so a
    /// record landing mid-flush re-marks its view and the next tick's
    /// whole-view upsert carries it (approximate, never lost while the
    /// process lives). A past day stops being rewritten once its final
    /// flush succeeded.
    pub(crate) async fn flush(&self) {
        self.ensure_seeded().await;
        if self.seeded.get().is_none() {
            return; // storage still unreachable - counters keep accumulating
        }
        let today = self.day();
        // Consume the dirty set and snapshot the data under one short lock:
        // anything recorded after this point re-marks its view dirty (the
        // flags are already clear), so the next tick's upsert covers it. The
        // async writes run without the lock.
        let writes = {
            let mut inner = self.inner.lock();
            let mut writes = FlushWrites::default();
            if inner.dirty_models {
                writes.models = Some(TotalsDoc::entries(
                    inner.models.iter().map(|(model, dim)| Entry::model(model, dim)).collect(),
                ));
                inner.dirty_models = false;
            }
            for guild_id in inner.dirty_guilds.clone() {
                if let Some(usage) = inner.guilds.get(&guild_id) {
                    writes.guilds.push((
                        guild_id,
                        TotalsDoc::entries(
                            usage
                                .models
                                .iter()
                                .map(|(model, dim)| Entry::model(model, dim))
                                .collect(),
                        ),
                    ));
                }
                inner.dirty_guilds.remove(&guild_id);
            }
            for day in inner.dirty_days.clone() {
                if let Some(bucket) = inner.days.get(&day) {
                    writes.days.push((day.clone(), doc_from_day(&day, bucket)));
                }
                inner.dirty_days.remove(&day);
            }
            writes
        };

        // Write failures re-mark their views: the data is still only in
        // memory, and the next flush must retry it.
        let mut models_ok = true;
        if let Some(doc) = writes.models {
            models_ok = write_doc(&self.storage, USAGE_MODEL_TOTALS_KEY, &doc).await;
        }
        let mut guilds_failed = Vec::new();
        for (guild_id, doc) in &writes.guilds {
            let key = usage_guild_totals_key(*guild_id);
            if !write_doc(&self.storage, &key, doc).await {
                guilds_failed.push(*guild_id);
            }
        }
        let mut days_failed = Vec::new();
        for (day, doc) in &writes.days {
            let key = usage_daily_key(day);
            if !write_doc(&self.storage, &key, doc).await {
                days_failed.push(day.clone());
            }
        }

        // Finalize past days: their bucket is dropped once its last write
        // succeeded (a failed past day keeps its bucket and dirty flag for
        // retry - see the re-marking above).
        let mut inner = self.inner.lock();
        if !models_ok {
            inner.dirty_models = true;
        }
        for guild_id in guilds_failed {
            inner.dirty_guilds.insert(guild_id);
        }
        for day in days_failed {
            inner.dirty_days.insert(day);
        }
        let stale: Vec<String> = inner
            .days
            .range(..today.clone())
            .filter(|(day, _)| !inner.dirty_days.contains(*day))
            .map(|(day, _)| day.clone())
            .collect();
        for day in stale {
            inner.days.remove(&day);
        }
    }
}

/// The `/llm_usage` projection of one guild's all-time usage.
#[derive(Debug, Clone, Default)]
pub(crate) struct GuildUsageSnapshot {
    pub total: Dimension,
    pub models: Vec<(String, Dimension)>,
}

/// The `/llm_usage_global` projection: deployment-wide all-time usage and
/// the per-server aggregates behind the ranking.
#[derive(Debug, Clone, Default)]
pub(crate) struct GlobalUsageSnapshot {
    pub total: Dimension,
    pub models: Vec<(String, Dimension)>,
    pub guilds: Vec<(u64, Dimension)>,
}

fn merge_guild(target: &mut GuildUsage, loaded: GuildUsage) {
    target.total.merge(loaded.total);
    for (model, dimension) in loaded.models {
        target.models.entry(model).or_default().merge(dimension);
    }
}

fn merge_day(target: &mut DayBucket, loaded: DayBucket) {
    for (model, dimension) in loaded.models {
        target.models.entry(model).or_default().merge(dimension);
    }
    for (guild_id, dimension) in loaded.guilds {
        target.guilds.entry(guild_id).or_default().merge(dimension);
    }
}

fn doc_from_day(day: &str, bucket: &DayBucket) -> TotalsDoc {
    let mut entries: Vec<Entry> =
        bucket.models.iter().map(|(model, dim)| Entry::model(model, dim)).collect();
    entries.extend(bucket.guilds.iter().map(|(guild_id, dim)| Entry::guild(*guild_id, dim)));
    TotalsDoc { day: Some(day.to_owned()), entries }
}

async fn read_doc(storage: &Arc<dyn PluginStorage>, key: &str) -> Result<TotalsDoc, ()> {
    read_doc_inner(storage, key)
        .await
        .map(|doc| doc.unwrap_or_else(|| TotalsDoc::entries(Vec::new())))
}

/// `Ok(None)` = the document does not exist. A MALFORMED or corrupt
/// document is a distinct outcome: totals are observability, never
/// reply-blocking, so it resets its view (warned) instead of failing the
/// seed forever - a permanently failing seed would stop every future
/// flush, silently freezing all persistence.
async fn read_doc_inner(
    storage: &Arc<dyn PluginStorage>,
    key: &str,
) -> Result<Option<TotalsDoc>, ()> {
    match storage.get(NAMESPACE, key).await {
        Ok(Some(raw)) => match serde_json::from_value(raw) {
            Ok(doc) => Ok(Some(doc)),
            Err(err) => {
                tracing::warn!(key, %err, "usage totals document malformed - starting fresh");
                Ok(None)
            }
        },
        Ok(None) => Ok(None),
        Err(StorageError::Serialization(err)) => {
            tracing::warn!(key, %err, "usage totals document corrupt - starting fresh");
            Ok(None)
        }
        Err(err) => {
            tracing::debug!(key, %err, "usage totals read failed - retrying on next use");
            Err(())
        }
    }
}

async fn write_doc(storage: &Arc<dyn PluginStorage>, key: &str, doc: &TotalsDoc) -> bool {
    let value = match serde_json::to_value(doc) {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(key, %err, "usage totals serialization failed");
            return false;
        }
    };
    match storage.set(NAMESPACE, key, value).await {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(key, %err, "usage totals flush failed");
            false
        }
    }
}

#[derive(Default)]
struct FlushWrites {
    models: Option<TotalsDoc>,
    guilds: Vec<(u64, TotalsDoc)>,
    days: Vec<(String, TotalsDoc)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::spi_ports::PluginStoragePort;
    use crate::test_support::InMemoryPluginStorage;
    use serde_json::Value;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn tracker() -> (Arc<UsageTracker>, Arc<InMemoryPluginStorage>) {
        let storage = Arc::new(InMemoryPluginStorage::new());
        (UsageTracker::new(storage.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG)), storage)
    }

    fn usage(prompt: u64, completion: u64) -> TokenUsage {
        TokenUsage {
            prompt_tokens: prompt,
            completion_tokens: completion,
            total_tokens: prompt + completion,
            cached_tokens: None,
            reasoning_tokens: None,
        }
    }

    #[test]
    fn reported_sample_copies_the_endpoint_breakdown() {
        let mut usage = usage(100, 10);
        usage.cached_tokens = Some(40);
        usage.reasoning_tokens = Some(6);
        let sample = Sample::reported(&usage);
        assert_eq!(sample.requests, 1);
        assert_eq!(sample.prompt_tokens, 100);
        assert_eq!(sample.completion_tokens, 10);
        assert_eq!(sample.cached_tokens, 40);
        assert_eq!(sample.reasoning_tokens, 6);
    }

    #[test]
    fn estimated_sample_scales_both_sides_by_the_ratio() {
        let sample = Sample::estimated(1000, 200, 0.5);
        assert_eq!(sample.requests, 1);
        assert_eq!(sample.prompt_tokens, 500);
        assert_eq!(sample.completion_tokens, 100);
        assert_eq!(sample.cached_tokens, 0);
    }

    #[tokio::test]
    async fn record_aggregates_model_guild_and_day_views() {
        let (tracker, _storage) = tracker();

        let cumulative = tracker.record("zai/glm", Some(42), Sample::reported(&usage(100, 10)));
        assert_eq!(cumulative.requests, 1);
        let cumulative = tracker.record("zai/glm", Some(42), Sample::reported(&usage(50, 5)));
        assert_eq!(cumulative.prompt_tokens, 150);
        tracker.record("local/gemma", None, Sample::reported(&usage(7, 1)));

        let (all_time, today) = tracker.guild_snapshot(42).await;
        assert_eq!(all_time.total.requests, 2);
        assert_eq!(all_time.total.prompt_tokens, 150);
        assert_eq!(all_time.models.len(), 1);
        assert_eq!(today.requests, 2);
        // A guild with no usage is empty, not an error.
        let (other, _) = tracker.guild_snapshot(43).await;
        assert_eq!(other.total.requests, 0);
    }

    #[tokio::test]
    async fn flush_persists_dirty_views_and_skip_is_clean() {
        let (tracker, storage) = tracker();
        tracker.record("zai/glm", Some(42), Sample::reported(&usage(100, 10)));
        tracker.flush().await;

        let rows = storage.rows(crate::test_support::TEST_PLATFORM_SLUG);
        let keys: Vec<String> = rows.iter().map(|(_, key, _)| key.clone()).collect();
        assert!(keys.contains(&USAGE_MODEL_TOTALS_KEY.to_owned()));
        assert!(keys.contains(&usage_guild_totals_key(42)));
        assert_eq!(keys.iter().filter(|key| key.starts_with(USAGE_DAILY_PREFIX)).count(), 1);

        // A clean tracker writes nothing new - the flush is dirty-gated.
        let before = storage.rows(crate::test_support::TEST_PLATFORM_SLUG);
        tracker.flush().await;
        assert_eq!(storage.rows(crate::test_support::TEST_PLATFORM_SLUG), before);
    }

    #[tokio::test]
    async fn seed_merges_stored_totals_and_continues_additively() {
        let (tracker, storage) = tracker();
        tracker.record("zai/glm", Some(42), Sample::reported(&usage(100, 10)));
        tracker.flush().await;

        // A "restarted" tracker over the same storage continues the totals.
        let restarted =
            UsageTracker::new(storage.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG));
        restarted.record("zai/glm", Some(42), Sample::reported(&usage(10, 1)));
        let (all_time, _) = restarted.guild_snapshot(42).await;
        assert_eq!(all_time.total.requests, 2);
        assert_eq!(all_time.total.prompt_tokens, 110);
        assert_eq!(all_time.total.completion_tokens, 11);
    }

    #[tokio::test]
    async fn day_documents_carry_the_flat_entry_schema() {
        let (tracker, storage) = tracker();
        tracker.record("zai/glm", Some(42), Sample::reported(&usage(100, 10)));
        tracker.flush().await;

        let day = usage_daily_key(&current_day());
        let raw = storage
            .plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG)
            .get(NAMESPACE, &day)
            .await
            .expect("read expected")
            .expect("day document expected");
        let doc: TotalsDoc = serde_json::from_value(raw).expect("schema expected");
        assert_eq!(doc.day.as_deref(), Some(current_day().as_str()));
        assert_eq!(doc.entries.len(), 2); // one model entry, one guild entry
        let model_entry = doc.entries.iter().find(|entry| entry.dim == EntryDim::Model);
        assert_eq!(model_entry.map(|entry| entry.key.as_str()), Some("zai/glm"));
        assert_eq!(model_entry.map(|entry| entry.prompt_tokens), Some(100));
        let guild_entry = doc.entries.iter().find(|entry| entry.dim == EntryDim::Guild);
        assert_eq!(guild_entry.map(|entry| entry.key.as_str()), Some("42"));
    }

    /// A storage fake whose first `set` records through a weak tracker
    /// reference - the mid-flush record the flush race test needs.
    struct MidFlushStorage {
        backend: Arc<dyn PluginStorage>,
        tracker: Arc<Mutex<std::sync::Weak<UsageTracker>>>,
        fired: AtomicBool,
    }

    #[async_trait]
    impl PluginStorage for MidFlushStorage {
        async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>, StorageError> {
            self.backend.get(namespace, key).await
        }

        async fn set(&self, namespace: &str, key: &str, value: Value) -> Result<(), StorageError> {
            if !self.fired.swap(true, Ordering::SeqCst) {
                if let Some(tracker) = self.tracker.lock().upgrade() {
                    tracker.record(
                        "zai/glm",
                        Some(42),
                        Sample { requests: 1, ..Sample::default() },
                    );
                }
            }
            self.backend.set(namespace, key, value).await
        }

        async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError> {
            self.backend.delete(namespace, key).await
        }

        async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError> {
            self.backend.list_keys(namespace).await
        }
    }

    /// The flush CONSUMES the dirty flags in its snapshot critical section:
    /// a record landing while the documents are being written re-marks its
    /// view, so the next flush's whole-view upsert carries it. (The old
    /// clear-after-write ordering erased the re-mark and silently dropped
    /// the record until some later record re-dirtied the view.)
    #[tokio::test]
    async fn record_landing_mid_flush_is_not_lost() {
        use std::sync::Weak;
        use std::sync::atomic::{AtomicBool, Ordering};

        let store = Arc::new(InMemoryPluginStorage::new());
        let tracker_cell = Arc::new(Mutex::new(Weak::new()));
        let storage = Arc::new(MidFlushStorage {
            backend: store.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            tracker: Arc::clone(&tracker_cell),
            fired: AtomicBool::new(false),
        });
        let tracker = UsageTracker::with_clock(
            Arc::clone(&storage) as Arc<dyn PluginStorage>,
            Arc::new(|| "2026-01-02".to_owned()),
        );
        *tracker_cell.lock() = Arc::downgrade(&tracker);

        tracker.record("zai/glm", Some(42), Sample::reported(&usage(100, 10)));
        tracker.flush().await;
        // The mid-flush record fired inside the first `set`; the view must
        // still be dirty, and the next flush must persist BOTH records.
        assert!(tracker.is_dirty(), "mid-flush record lost: view not re-marked");
        tracker.flush().await;
        assert!(!tracker.is_dirty());

        let raw = store
            .plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG)
            .get(NAMESPACE, USAGE_MODEL_TOTALS_KEY)
            .await
            .expect("read expected")
            .expect("model document expected");
        let doc: TotalsDoc = serde_json::from_value(raw).expect("schema expected");
        let total = doc.entries.first().expect("model entry expected");
        assert_eq!(total.requests, 2, "mid-flush record missing from storage");
    }

    /// A failed flush re-marks its views (the data lives only in memory);
    /// a healed storage flushes them on the next tick. Seed failure must
    /// refuse the flush entirely - a partial in-memory view must never
    /// overwrite stored history.
    #[tokio::test]
    async fn flush_and_seed_failures_keep_views_dirty_until_storage_recovers() {
        let storage = crate::test_support::FailingPluginStorage::new();
        let tracker =
            UsageTracker::new(storage.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG));
        tracker.record("zai/glm", Some(1), Sample::reported(&usage(100, 10)));

        // Storage down: flush refuses (the seed fails), nothing persists.
        tracker.flush().await;
        assert!(storage.rows(crate::test_support::TEST_PLATFORM_SLUG).is_empty());
        assert!(tracker.is_dirty());

        // Healed: seed lands, flush persists the in-memory record.
        storage.set_failing(false);
        tracker.flush().await;
        assert!(!tracker.is_dirty());
        assert_eq!(
            storage.rows(crate::test_support::TEST_PLATFORM_SLUG).len(),
            3, // model totals + guild totals + the day document
        );
    }

    /// A clock stepping backwards (NTP correction) after a day was
    /// finalized must not reopen it: the record counts in the all-time
    /// views, but the daily series on disk is never rewritten from a
    /// partial bucket.
    #[tokio::test]
    async fn record_never_reopens_a_finalized_day() {
        let day_cell = Arc::new(parking_lot::Mutex::new("2026-01-02".to_owned()));
        let clock: Arc<dyn Fn() -> String + Send + Sync> = {
            let day_cell = Arc::clone(&day_cell);
            Arc::new(move || day_cell.lock().clone())
        };
        let store = Arc::new(InMemoryPluginStorage::new());
        let tracker = UsageTracker::with_clock(
            store.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            clock,
        );

        tracker.record("zai/glm", Some(1), Sample::reported(&usage(100, 10)));
        tracker.flush().await;

        // Clock steps back into the finalized day.
        *day_cell.lock() = "2026-01-01".to_owned();
        tracker.record("zai/glm", Some(1), Sample::reported(&usage(50, 5)));
        tracker.flush().await;

        let scoped = store.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        let model_raw = scoped
            .get(NAMESPACE, USAGE_MODEL_TOTALS_KEY)
            .await
            .expect("read expected")
            .expect("model doc expected");
        let model_doc: TotalsDoc = serde_json::from_value(model_raw).expect("schema expected");
        assert_eq!(
            model_doc.entries.first().map(|entry| entry.requests),
            Some(2),
            "all-time counts both"
        );

        let finalized_raw = scoped
            .get(NAMESPACE, &usage_daily_key("2026-01-02"))
            .await
            .expect("read expected")
            .expect("finalized day doc expected");
        let finalized: TotalsDoc = serde_json::from_value(finalized_raw).expect("schema expected");
        assert_eq!(
            finalized.entries.first().map(|entry| entry.requests),
            Some(1),
            "finalized day untouched"
        );

        assert_eq!(
            scoped.get(NAMESPACE, &usage_daily_key("2026-01-01")).await.expect("read expected"),
            None,
            "the reopened day must not be written"
        );
    }
}
