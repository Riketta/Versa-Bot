//! The poll engine: one scheduler-driven tick fetches the four store
//! sources, diffs against the last-seen state, and fans the delta out to
//! every guild that enabled the tracker; watch subscriptions are matched
//! against the same delta (independent of the announce flags).
//!
//! State layering: the live `last_seen` and the last raw snapshot are
//! in-memory (one store per bot - the League client is local). Both are
//! persisted together in the plugin-global `store_dump` document
//! ([`PluginStorage`], not guild storage - the store is deployment-wide)
//! whenever anything changed: after a restart the engine adopts the
//! document and announces what changed while it was down; with no document
//! the first successful poll is a silent baseline. A deployment upgraded
//! from the old per-guild `last_seen` copies migrates once on boot (adopt,
//! write the document, delete the copies). Per-UTC-day history digests
//! (`history.<day>`, see [`super::history`]) ride the same change gate.
//! The per-guild record log receives every announcement and watch ping -
//! user-facing history, never consulted for detection.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::kernel::models::{ChannelId, Embed, GuildId, Origin, OutboundMessage, UserId};
use crate::kernel::plugin_ports::{EventBusPort, Job};
use crate::kernel::spi_ports::{
    ChatOutputFactoryPort, GuildStorage, PlatformInfoPort, PluginStorage, StoragePort,
};

use super::diff::{
    self, LastSeen, Snapshot, StoreDelta, YourShopStart, rotation_delta, rotation_stores,
};
use super::events::{LolStoreAnnounced, StoreEventKind};
use super::format::{NameIndex, announce_pages, champion_map};
use super::history::{self, DayDigest};
use super::lcu::LcuPort;
use super::watch::{self, StoreSearch, WatchDoc, WatchTarget};

/// Guild storage namespace (the plugin's slug).
pub const NAMESPACE: &str = "lol_store";
/// Guild config document: enabled flag + announcement channel.
pub const CONFIG_KEY: &str = "config";
/// Plugin-global document: the compacted last-seen state plus the last raw
/// snapshot, written whenever either changed - boot catch-up source and the
/// `/lol_store_dump` fallback.
pub const STORE_DUMP_KEY: &str = "store_dump";
/// Legacy boot catch-up document (pre-consolidation: a compacted copy per
/// enabled guild). Migrated into [`STORE_DUMP_KEY`] on first boot and then
/// deleted; never written again.
pub const LAST_SEEN_KEY: &str = "last_seen";
/// Schema version of the [`STORE_DUMP_KEY`] document.
pub(crate) const STORE_SCHEMA: u32 = 1;
/// Days of store history retained by default (`history_days`).
pub const DEFAULT_HISTORY_DAYS: u32 = 90;

/// Which trackers announce. Unannounced trackers still update the state, so
/// re-enabling never replays old events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnounceFlags {
    pub sales: bool,
    pub new_skins: bool,
    pub mythic_rotation: bool,
    pub yourshop: bool,
}

impl AnnounceFlags {
    /// Every tracker announcing (the config default).
    #[must_use]
    pub fn all_on() -> Self {
        Self { sales: true, new_skins: true, mythic_rotation: true, yourshop: true }
    }
}

/// Engine construction settings (`[lol_store]` config section). The announce
/// flags, watch caps, and the history retention hot-reload
/// ([`Self::update_settings`]); the poll interval is boot-frozen with the
/// scheduled poll job.
#[derive(Debug, Clone)]
pub struct EngineSettings {
    pub poll: Duration,
    pub flags: AnnounceFlags,
    /// Maximum watches per user per guild - `/lol_store_watch` rejects
    /// beyond this.
    pub watch_user_cap: u32,
    /// Maximum watches per guild.
    pub watch_guild_cap: u32,
    /// Days of store history retained (`history_days`, today included);
    /// `0` disables recording. Older day digests are pruned on the next
    /// write.
    pub history_days: u32,
}

/// The plugin-global store document (`store_dump` key): the compacted
/// catch-up state plus the last raw snapshot in one atomically-written
/// value. Restored wholesale on boot; written whenever either half changed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PersistedStore {
    pub(crate) schema: u32,
    pub(crate) saved_at_unix: u64,
    #[serde(default)]
    pub(crate) last_seen: LastSeen,
    #[serde(default)]
    pub(crate) snapshot: Snapshot,
}

/// Does the snapshot carry at least one fetched section? The restore and
/// history paths must not act on an all-empty snapshot (a migration adopt
/// or a write-less cycle).
pub(crate) fn has_store_data(snapshot: &Snapshot) -> bool {
    snapshot.sales.is_some()
        || snapshot.catalog.is_some()
        || snapshot.rotations.is_some()
        || snapshot.yourshop.is_some()
}

/// Does the snapshot carry at least one section the current-state dump
/// renders (sales, rotations, Your Shop)? The catalog alone renders
/// nothing there.
fn has_renderable_data(snapshot: &Snapshot) -> bool {
    snapshot.sales.is_some() || snapshot.rotations.is_some() || snapshot.yourshop.is_some()
}

/// Per-guild config document shape.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GuildConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Announcement channel (string - snowflakes exceed JSON numbers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
    /// Optional role tagged on announcements (the "subscription" role:
    /// members join it to opt into pings). The tag rides the message
    /// content - embeds never notify.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_id: Option<String>,
}

/// One guild discovered for a poll cycle's fan-out: the scoped storage
/// handle plus the tracker config's channel assignment. Only guilds that
/// enabled the tracker AND assigned a channel are discovered - the fan-out
/// audience; catch-up persistence is plugin-global and needs no guilds.
struct DiscoveredGuild {
    guild_id: GuildId,
    storage: Arc<dyn GuildStorage>,
    channel: ChannelId,
    role_id: Option<String>,
}

/// The poll engine (see module docs). Generic over the bus like every
/// bus-publishing plugin.
pub struct StoreEngine<B: EventBusPort> {
    lcu: Arc<dyn LcuPort>,
    storage: Arc<dyn StoragePort>,
    /// Plugin-global documents: the consolidated `store_dump` catch-up
    /// document and the per-day history digests live here (the store is
    /// deployment-wide - it must not fan out into guild storage).
    plugin_storage: Arc<dyn PluginStorage>,
    factory: Arc<dyn ChatOutputFactoryPort>,
    bus: B,
    /// The deployment's platform identity - subscriptions of other slugs
    /// (e.g. rows left by a differently-wired storage) are skipped, and it
    /// namespaces every storage binding.
    platform: Arc<dyn PlatformInfoPort>,
    settings: Mutex<EngineSettings>,
    state: Mutex<Option<Arc<LastSeen>>>,
    champions: Mutex<Option<Arc<HashMap<u64, String>>>>,
    /// Last good raw snapshot, one section per source: a source that
    /// failed this cycle keeps its previous data. Feeds name joins when a
    /// source fails mid-cycle (instead of synthetic ids frozen into the
    /// permanent record), subscribe-time watch status, and the current-
    /// state dump. Restored from the persisted document at boot.
    last_snapshot: Mutex<Option<Snapshot>>,
    /// Bumped whenever the retention block above writes; keys the cached
    /// name index (see [`StoreEngine::cached_index`]).
    snapshot_generation: AtomicU64,
    /// Cached joins over the last retained snapshot, keyed by generation:
    /// watch commands reuse it instead of re-cloning the catalog per
    /// invocation.
    name_index_cache: Mutex<Option<(u64, Arc<NameIndex>)>>,
    online: AtomicBool,
    last_poll: Mutex<Option<Instant>>,
    /// The last UTC day a history digest was written for. Quiet observed
    /// cycles consult it so a day the store did not change still gets its
    /// digest exactly once (when the UTC day flips), not once per poll.
    last_history_day: Mutex<Option<String>>,
    /// The announcement as delivered: one entry per embed page. Process
    /// lifetime - a fresh launch has none until the first delta renders.
    last_announcement: Mutex<Option<(Instant, Vec<String>)>>,
}

impl<B: EventBusPort> StoreEngine<B> {
    #[must_use]
    pub fn new(
        lcu: Arc<dyn LcuPort>,
        storage: Arc<dyn StoragePort>,
        plugin_storage: Arc<dyn PluginStorage>,
        factory: Arc<dyn ChatOutputFactoryPort>,
        bus: B,
        platform: Arc<dyn PlatformInfoPort>,
        settings: EngineSettings,
    ) -> Self {
        Self {
            lcu,
            storage,
            plugin_storage,
            factory,
            bus,
            platform,
            settings: Mutex::new(settings),
            state: Mutex::new(None),
            champions: Mutex::new(None),
            last_snapshot: Mutex::new(None),
            snapshot_generation: AtomicU64::new(0),
            name_index_cache: Mutex::new(None),
            online: AtomicBool::new(false),
            last_poll: Mutex::new(None),
            last_history_day: Mutex::new(None),
            last_announcement: Mutex::new(None),
        }
    }

    #[must_use]
    pub fn settings(&self) -> EngineSettings {
        self.settings.lock().clone()
    }

    /// Hot reload: swaps the announce flags, watch caps, and the history
    /// retention; the poll interval is kept - the poll job's cadence is
    /// fixed at schedule time, so a runtime change would only fork the cell
    /// from the running job (enabling or disabling the watcher stays a
    /// boot-time decision). Identical values are a no-op, so unrelated
    /// config edits change nothing.
    pub fn update_settings(&self, settings: EngineSettings) {
        let mut current = self.settings.lock();
        if current.flags == settings.flags
            && current.watch_user_cap == settings.watch_user_cap
            && current.watch_guild_cap == settings.watch_guild_cap
            && current.history_days == settings.history_days
        {
            return;
        }
        let mut changes: Vec<String> = Vec::new();
        if current.flags.sales != settings.flags.sales {
            changes.push(format!("sales {} -> {}", current.flags.sales, settings.flags.sales));
        }
        if current.flags.new_skins != settings.flags.new_skins {
            changes.push(format!(
                "new_skins {} -> {}",
                current.flags.new_skins, settings.flags.new_skins
            ));
        }
        if current.flags.mythic_rotation != settings.flags.mythic_rotation {
            changes.push(format!(
                "mythic_rotation {} -> {}",
                current.flags.mythic_rotation, settings.flags.mythic_rotation
            ));
        }
        if current.flags.yourshop != settings.flags.yourshop {
            changes.push(format!(
                "yourshop {} -> {}",
                current.flags.yourshop, settings.flags.yourshop
            ));
        }
        if current.watch_user_cap != settings.watch_user_cap {
            changes.push(format!(
                "watch_user_cap {} -> {}",
                current.watch_user_cap, settings.watch_user_cap
            ));
        }
        if current.watch_guild_cap != settings.watch_guild_cap {
            changes.push(format!(
                "watch_guild_cap {} -> {}",
                current.watch_guild_cap, settings.watch_guild_cap
            ));
        }
        if current.history_days != settings.history_days {
            changes.push(format!(
                "history_days {} -> {}",
                current.history_days, settings.history_days
            ));
        }
        current.flags = settings.flags;
        current.watch_user_cap = settings.watch_user_cap;
        current.watch_guild_cap = settings.watch_guild_cap;
        current.history_days = settings.history_days;
        drop(current);
        tracing::info!(changes = ?changes, "config lol_store hot-reloaded");
    }

    /// One poll cycle. Never fails outward: every failure mode is a logged,
    /// skipped cycle (the client being closed is the normal steady state).
    pub async fn tick(&self) {
        *self.last_poll.lock() = Some(Instant::now());

        // Boot catch-up: with no in-memory state, adopt the persisted
        // document so the diff announces what happened while the bot was
        // down. The document's raw snapshot also re-arms the name joins,
        // watch subscribe-status, and the current-state dump before the
        // first successful poll (e.g. the client is still offline).
        let mut catchup = false;
        if self.state.lock().is_none() {
            if let Some((persisted, from_legacy)) = self.load_persisted().await {
                tracing::debug!("adopting persisted store state for catch-up");
                if from_legacy {
                    // The migration completes here, not in the persist gate
                    // below: a fully-offline cycle returns early, and state
                    // is no longer `None` on any later tick, so the cleanup
                    // would never run again. Both writes are idempotent;
                    // the poll below rewrites the document when it changed.
                    self.persist_store(&persisted.last_seen).await;
                    self.cleanup_legacy_state().await;
                }
                *self.state.lock() = Some(Arc::new(persisted.last_seen));
                {
                    let mut retained = self.last_snapshot.lock();
                    if retained.is_none() && has_store_data(&persisted.snapshot) {
                        *retained = Some(persisted.snapshot);
                        self.snapshot_generation.fetch_add(1, Ordering::Release);
                    }
                }
                catchup = true;
            }
        }
        // Refcount bump, not a deep copy: the tracked sets stay put.
        let previous = self.state.lock().clone();

        // Fetch every source independently; a source failing keeps its
        // previous state this cycle and retries next tick.
        let (sales, catalog, rotations, yourshop) = tokio::join!(
            self.lcu.sales(),
            self.lcu.catalog(),
            self.lcu.rotations(),
            self.lcu.yourshop_status(),
        );
        let mut snapshot = Snapshot::default();
        let mut failures: Vec<(&'static str, String)> = Vec::new();
        match sales {
            Ok(data) => snapshot.sales = Some(data),
            Err(err) => failures.push(("sales", err.to_string())),
        }
        match catalog {
            Ok(data) => snapshot.catalog = Some(data),
            Err(err) => failures.push(("catalog", err.to_string())),
        }
        match rotations {
            Ok(data) => snapshot.rotations = Some(data),
            Err(err) => failures.push(("rotations", err.to_string())),
        }
        match yourshop {
            Ok(data) => snapshot.yourshop = Some(data),
            Err(err) => failures.push(("yourshop", err.to_string())),
        }

        // The tracker is about skins: the champion ITSELF going on sale is
        // noise in the feed (its id never joins a catalog skin, and it is
        // not what the subscription role signed up for). Other non-skin
        // items (chests, bundles) still pass.
        if let Some(sales) = &mut snapshot.sales {
            sales.retain(|sale| sale.item.inventory_type.as_deref() != Some("CHAMPION"));
        }

        if failures.len() == 4 {
            // Client closed (or restarting): the normal quiet path.
            self.online.store(false, Ordering::Release);
            tracing::debug!(
                sources = ?failures.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
                "store poll skipped - league client unreachable"
            );
            return;
        }
        self.online.store(true, Ordering::Release);
        if !failures.is_empty() {
            tracing::warn!(
                ?failures,
                "some store sources failed this cycle - keeping previous state"
            );
        }

        // Retain the raw snapshot per successful source (name-join fallback,
        // subscribe-time watch status, the current-state dump, and the dump
        // document's raw half); failed sections keep their previous data.
        // Cached at fetch time: the fallback must exist even for cycles
        // where a source failed and nothing announces. The generation keys
        // the name-index cache - it bumps only when a retained section
        // actually changed, so an unchanged store does not force a
        // full-catalog rebuild for the next watch command. The same flag
        // gates the persistence write below.
        let mut raw_changed = false;
        {
            let mut retained = self.last_snapshot.lock();
            let slot = retained.get_or_insert_with(Snapshot::default);
            // Assignment only on change: the unchanged catalog (thousands
            // of localized items) must not deep-clone every cycle. The
            // allocation-free deep compare stays - it keys the generation.
            if snapshot.sales.is_some() && slot.sales != snapshot.sales {
                raw_changed = true;
                slot.sales = snapshot.sales.clone();
            }
            if snapshot.catalog.is_some() && slot.catalog != snapshot.catalog {
                raw_changed = true;
                slot.catalog = snapshot.catalog.clone();
            }
            if snapshot.rotations.is_some() && slot.rotations != snapshot.rotations {
                raw_changed = true;
                slot.rotations = snapshot.rotations.clone();
            }
            if snapshot.yourshop.is_some() && slot.yourshop != snapshot.yourshop {
                raw_changed = true;
                slot.yourshop = snapshot.yourshop.clone();
            }
            if raw_changed {
                self.snapshot_generation.fetch_add(1, Ordering::Release);
            }
        }

        let current = Arc::new(match previous.as_ref() {
            Some(previous) => diff::merge(previous.as_ref(), &snapshot),
            // Silent baseline: first successful poll with no persisted state.
            None => diff::merge(&LastSeen::default(), &snapshot),
        });
        let changed = previous.as_ref().map(Arc::as_ref) != Some(current.as_ref());

        // The full delta drives watches; the announce flags then strip the
        // sections the operator disabled for the general feed - watches are
        // personal and stay independent of those flags.
        let delta = previous.as_ref().map(|previous| diff::compute(previous.as_ref(), &snapshot));
        let full = delta.as_ref().filter(|delta| !delta.is_empty());

        if previous.is_none() {
            tracing::debug!("first successful poll - recording silent baseline");
        }

        // One discovery pass per cycle - but only when something needs it:
        // the fan-out (delta found). Persistence no longer needs guilds -
        // the store document and history digests live plugin-globally - so
        // `list_guilds` runs only for announcements and watch pings.
        let discovered = if full.is_some() { self.discover_guilds().await } else { Vec::new() };

        // Announce and notify before the state swap: formatting joins
        // against the freshly retained catalog (the retention block above
        // has already absorbed every changed section and bumped the
        // generation, so the generation-keyed index cache builds from
        // exactly the data `name_index(&snapshot)` would - without
        // re-cloning the whole catalog per delta cycle).
        if let Some(delta) = full {
            tracing::debug!(
                sales = delta.sales.len(),
                skins = delta.skins.len(),
                rotations = delta.rotations.len(),
                yourshop = delta.yourshop.is_some(),
                catchup,
                "store delta detected"
            );
            let index = self.cached_index().await.unwrap_or_else(|| {
                // Unreachable after the retention block (it materializes a
                // default snapshot), kept total for the same reason.
                Arc::new(NameIndex::empty())
            });
            // Every discovered guild enabled the tracker with a channel -
            // announcements and watch pings see the same discovery snapshot.
            let targets: Vec<&DiscoveredGuild> = discovered.iter().collect();
            // The general feed honors the announce flags (stripped copy);
            // personal watches see the full delta below.
            let mut feed = delta.clone();
            let flags = self.settings.lock().flags;
            if !flags.sales {
                feed.sales.clear();
            }
            if !flags.new_skins {
                feed.skins.clear();
            }
            if !flags.mythic_rotation {
                feed.rotations.clear();
            }
            if !flags.yourshop {
                feed.yourshop = None;
            }
            if !feed.is_empty() {
                let pages = self.announce(&feed, &index, &targets, catchup).await;
                // An empty render announces nothing - storing it would make
                // `/lol_client_status` report a phantom announcement.
                if !pages.is_empty() {
                    *self.last_announcement.lock() = Some((Instant::now(), pages));
                }
            } else {
                tracing::debug!(
                    catchup,
                    "delta found but announce flags strip every section - watch pings only"
                );
            }
            self.notify_subscriptions(delta, &index, &targets, catchup).await;
        }
        let delta_found = full.is_some();

        // Persist when anything the plugin-global document carries changed:
        // the compacted state (`changed`) or any raw section (`raw_changed`
        // - the dump and the day digest ride the same write). Guild storage
        // is no longer touched here.
        let history_written = if changed || raw_changed {
            if changed {
                *self.state.lock() = Some(Arc::clone(&current));
            }
            self.persist_store(&current).await;
            self.write_history_digest(true).await
        } else {
            // Quiet observed cycle: the store answered but nothing changed.
            // The day's digest is still owed - write it once, when the UTC
            // day flips, so every observed day has a document (not just the
            // days the store changed).
            self.write_history_digest(false).await
        };

        tracing::debug!(
            sales = current.sales.len(),
            skins = current.skins.len(),
            rotations = current.rotations.len(),
            delta_found,
            persisted = changed || raw_changed,
            history = history_written,
            catchup,
            "store poll complete"
        );
    }

    /// Formats and fans one delta out: per enabled guild - one embed per
    /// page, record, bus event. Returns the rendered pages (for the status
    /// memory); empty when nothing renders.
    ///
    /// Delivery is at-most-once by design: a guild whose send fails keeps
    /// what landed, logs the miss, and gets no record - and the state swap
    /// still advances, so the delta is not replayed next poll. Cosmetic
    /// pings, not a delivery contract.
    async fn announce(
        &self,
        delta: &StoreDelta,
        index: &NameIndex,
        targets: &[&DiscoveredGuild],
        catchup: bool,
    ) -> Vec<String> {
        if index.catalog.is_empty() {
            tracing::warn!(
                "store catalog unavailable - sale names degrade to bare ids \
                 (check the 'some store sources failed' warnings)"
            );
        }
        let pages = announce_pages(delta, index, self.embed_budget());
        if pages.is_empty() {
            return Vec::new();
        }

        if targets.is_empty() {
            tracing::debug!("store updates found but no guild is tracking them");
            return pages;
        }

        let mut kinds: Vec<StoreEventKind> = Vec::new();
        if !delta.sales.is_empty() {
            kinds.push(StoreEventKind::Sales);
        }
        if !delta.skins.is_empty() {
            kinds.push(StoreEventKind::NewSkins);
        }
        if !delta.rotations.is_empty() {
            kinds.push(StoreEventKind::MythicRotation);
        }
        if delta.yourshop.is_some() {
            kinds.push(StoreEventKind::YourShop);
        }

        let total = pages.len();
        let embeds: Vec<Embed> = pages
            .iter()
            .enumerate()
            .map(|(n, page)| Embed {
                title: if total == 1 {
                    "LoL Store updates".to_owned()
                } else {
                    format!("LoL Store updates ({}/{})", n + 1, total)
                },
                description: page.clone(),
            })
            .collect();
        let at_unix = unix_now();
        let record_text = pages.join("\n\n");

        let mut delivered_guilds = 0usize;
        for target in targets {
            let channel_id = target.channel;
            let guild_id = target.guild_id;
            let role_id = &target.role_id;
            let origin = Origin {
                guild_id: Some(guild_id),
                channel_id,
                user_id: UserId(0),
                message_id: None,
                reply_token: None,
            };
            // The role tag rides the CONTENT: Discord fires notifications
            // from message content only, never from embeds. First page
            // only - one event, one ping, not one per continuation.
            let role_tag = role_id
                .as_deref()
                .and_then(|id| id.parse::<u64>().ok())
                .map_or(String::new(), |id| format!("<@&{id}>"));
            let output = self.factory.channel_output(&origin, channel_id);
            let mut delivered = true;
            for (n, embed) in embeds.iter().enumerate() {
                let message = OutboundMessage {
                    content: if n == 0 { role_tag.clone() } else { String::new() },
                    embeds: vec![embed.clone()],
                    ..OutboundMessage::default()
                };
                if let Err(err) = output.send(message).await {
                    tracing::warn!(%err, guild = guild_id.get(), "failed to deliver store announcement");
                    delivered = false;
                    break;
                }
            }
            if !delivered {
                continue;
            }
            delivered_guilds += 1;
            if let Err(err) = target.storage
                .append(
                    NAMESPACE,
                    serde_json::json!({ "kind": "announcement", "text": record_text, "at_unix": at_unix }),
                )
                .await
            {
                tracing::warn!(%err, guild = guild_id.get(), "failed to record store announcement");
            }
            for kind in &kinds {
                self.bus.publish(Arc::new(LolStoreAnnounced { guild_id, kind: *kind }));
            }
        }

        // Audit-grade and honest: how many guilds actually got it, and
        // whether this was a live hit or a boot catch-up for a change that
        // happened while the bot could not observe the client.
        tracing::info!(
            catchup,
            sales = delta.sales.len(),
            skins = delta.skins.len(),
            rotations = delta.rotations.len(),
            yourshop = delta.yourshop.is_some(),
            guilds = targets.len(),
            delivered = delivered_guilds,
            pages = total,
            kinds = ?kinds.iter().map(|kind| kind.as_str()).collect::<Vec<_>>(),
            "store announcements delivered"
        );
        pages
    }

    /// Catalog + champion joins, with a lazily loaded champion name table
    /// (fetched once per process; a failed fetch retries next cycle while
    /// the cache stays empty).
    async fn name_index(&self, snapshot: &Snapshot) -> NameIndex {
        // Load-once champion table; the guard never crosses an await.
        if self.champions.lock().is_none() {
            // Shared by announcing ticks and watch commands; a hung client
            // must not stall an ephemeral command for the full request
            // timeout, so this fetch gets a short budget of its own. A miss
            // degrades to fallback names exactly like an error.
            match tokio::time::timeout(Duration::from_secs(3), self.lcu.champion_names()).await {
                Ok(Ok(entries)) => {
                    let map = champion_map(entries.into_iter().map(|entry| (entry.id, entry.name)));
                    if map.is_empty() {
                        // Never cache an empty table: a degenerate payload
                        // must retry next cycle, not pin fallback names for
                        // the process lifetime.
                        tracing::warn!(
                            "champion name table came back empty - using fallback names"
                        );
                    } else {
                        let mut cache = self.champions.lock();
                        if cache.is_none() {
                            *cache = Some(Arc::new(map.into_iter().collect()));
                        }
                    }
                }
                Ok(Err(err)) => {
                    tracing::warn!(%err, "champion name table unavailable - using fallback names");
                }
                Err(_) => {
                    tracing::warn!("champion name fetch timed out - using fallback names");
                }
            }
        }
        let champions: std::collections::BTreeMap<u64, String> = {
            let cache = self.champions.lock();
            cache
                .as_ref()
                .map(|cached| cached.iter().map(|(id, name)| (*id, name.clone())).collect())
                .unwrap_or_default()
        };
        let catalog = match &snapshot.catalog {
            Some(catalog) => catalog.clone(),
            // Catalog source failed this cycle: join against the last good
            // snapshot section (kept at fetch time in `tick`) so sale lines
            // keep real names - the alternative would bake "Skin 1031" into
            // the permanent record and `/lol_store_dump`.
            None => self
                .last_snapshot
                .lock()
                .as_ref()
                .and_then(|snapshot| snapshot.catalog.clone())
                .unwrap_or_default(),
        };
        NameIndex::new(catalog, champions)
    }

    /// Watch notifications: per enabled guild, match the FULL delta (the
    /// announce flags govern the general feed, not personal watches)
    /// against the guild's watch document and ping the watchers in the
    /// assigned channel - tags ride the content, embeds never notify.
    /// At-most-once like announcements: a failed send is a log line.
    async fn notify_subscriptions(
        &self,
        delta: &StoreDelta,
        index: &NameIndex,
        targets: &[&DiscoveredGuild],
        catchup: bool,
    ) {
        if targets.is_empty() {
            return;
        }
        let embed_budget = self.embed_budget();
        let tag_budget = watch::tag_budget(self.platform.message_limit());
        for target in targets {
            let channel_id = target.channel;
            let guild_id = target.guild_id;
            let storage = &target.storage;
            let doc = match storage.get(NAMESPACE, watch::WATCH_KEY).await {
                Ok(Some(raw)) => match serde_json::from_value::<WatchDoc>(raw) {
                    Ok(doc) => doc,
                    Err(err) => {
                        tracing::warn!(%err, guild = guild_id.get(), "watch doc unreadable - guild skipped this cycle");
                        continue;
                    }
                },
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(%err, guild = guild_id.get(), "watch doc unreadable - guild skipped this cycle");
                    continue;
                }
            };
            let Some(notification) =
                watch::build_notification(&doc, delta, index, embed_budget, tag_budget)
            else {
                continue;
            };
            let origin = Origin {
                guild_id: Some(guild_id),
                channel_id,
                user_id: UserId(0),
                message_id: None,
                reply_token: None,
            };
            let message = OutboundMessage {
                content: notification.tags,
                embeds: vec![Embed {
                    title: "Store watches".to_owned(),
                    description: notification.text,
                }],
                ..OutboundMessage::default()
            };
            let output = self.factory.channel_output(&origin, channel_id);
            if let Err(err) = output.send(message).await {
                tracing::warn!(%err, guild = guild_id.get(), "failed to deliver store watch notification");
                continue;
            }
            let at_unix = unix_now();
            if let Err(err) = storage
                .append(
                    NAMESPACE,
                    serde_json::json!({
                        "kind": "watch_notification",
                        "watchers": notification.watchers,
                        "at_unix": at_unix
                    }),
                )
                .await
            {
                tracing::warn!(%err, guild = guild_id.get(), "failed to record store watch notification");
            }
            tracing::info!(
                guild = guild_id.get(),
                watchers = notification.watchers,
                catchup,
                "store watch notification delivered"
            );
        }
    }

    /// Name search over the last good store snapshot - the resolution
    /// backend of `/lol_store_watch`. `None` = no store data captured yet
    /// (the client has not polled successfully since boot).
    pub async fn search_store(&self, query: &str) -> Option<StoreSearch> {
        let index = self.cached_index().await?;
        let mut champions = watch::search_champions(query, index.champions.iter());
        let mut skins = watch::search_skins(query, &index);
        // A query naming an id (`41`, or a pasted candidate line like
        // `Gangplank (id 41)`) cannot match any name - fetch the referenced
        // rows directly so the resolvers can select them.
        if let Some(id) = watch::query_id(query) {
            if let Some(name) = index.champions.get(&id) {
                if !champions.iter().any(|hit| hit.champion_id == id) {
                    champions.push(watch::ChampionHit { champion_id: id, champion: name.clone() });
                }
            }
            if let Some(entry) = index.skins.iter().find(|entry| entry.item_id == id) {
                if !skins.iter().any(|hit| hit.item_id == id) {
                    skins.push(watch::SkinHit {
                        item_id: entry.item_id,
                        champion_id: entry.champion_id,
                        champion: entry.champion.clone(),
                        skin: entry.skin.clone(),
                    });
                }
            }
        }
        Some(StoreSearch { skins, champions, store_backed: index.store_backed.clone() })
    }

    /// Current-activity status lines for a watch target ("currently on
    /// sale", "currently in the mythic rotation") against the last good
    /// snapshot. `None` = no store data captured yet.
    pub async fn watch_status(&self, target: &WatchTarget) -> Option<Vec<String>> {
        let index = self.cached_index().await?;
        let snapshot = self.last_snapshot.lock().clone()?;
        let mut lines = Vec::new();
        let on_sale = snapshot.sales.as_ref().is_some_and(|sales| {
            sales.iter().any(|sale| {
                sale.item.item_id.is_some_and(|item_id| {
                    watch::target_matches(target, &watch::Subject::Item(item_id), &index)
                })
            })
        });
        if on_sale {
            lines.push("currently on sale".to_owned());
        }
        let in_mythic = snapshot.rotations.as_ref().is_some_and(|stores| {
            diff::rotation_stores(stores).iter().any(|store| {
                store.catalog_entries.iter().any(|entry| {
                    let subject = watch::entry_subject(&diff::mythic_entry(entry), &index);
                    watch::target_matches(target, &subject, &index)
                })
            })
        });
        if in_mythic {
            lines.push("currently in the mythic rotation".to_owned());
        }
        Some(lines)
    }

    /// One discovery pass per fan-out cycle: `list_guilds` plus one config
    /// read per enabled guild serve announcements and watch pings. Rows of
    /// other slugs (storage written by a differently-wired deployment) are
    /// skipped; a guild without a readable config has no announcement
    /// target and is skipped too.
    async fn discover_guilds(&self) -> Vec<DiscoveredGuild> {
        let mut discovered = Vec::new();
        let Ok(guilds) = self.storage.list_guilds().await else {
            tracing::warn!(
                "guild listing failed - no store announcements or watch pings this cycle"
            );
            return Vec::new();
        };
        for (row_platform, guild_id) in guilds {
            if row_platform != self.platform.slug() {
                continue;
            }
            let storage = self.storage.guild_scoped(self.platform.slug(), guild_id);
            let raw = match storage.get(NAMESPACE, CONFIG_KEY).await {
                Ok(Some(raw)) => raw,
                // No config document: nothing to fan out to.
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(
                        %err,
                        guild = guild_id.get(),
                        "store tracker config unreadable - skipping guild this cycle"
                    );
                    continue;
                }
            };
            let Ok(config) = serde_json::from_value::<GuildConfig>(raw) else {
                tracing::warn!(
                    guild = guild_id.get(),
                    "store tracker config malformed - announcements skipped for this guild"
                );
                continue;
            };
            let channel = config
                .enabled
                .then(|| {
                    config
                        .channel_id
                        .as_deref()
                        .and_then(|id| id.parse::<u64>().ok())
                        .map(ChannelId)
                })
                .flatten();
            let Some(channel) = channel else {
                if config.enabled {
                    tracing::debug!(
                        guild = guild_id.get(),
                        "store tracker enabled but channel missing"
                    );
                }
                continue;
            };
            if let Some(role) = &config.role_id
                && role.parse::<u64>().is_err()
            {
                tracing::debug!(
                    guild = guild_id.get(),
                    "store tracker announce role is not a role id - pings disabled for this guild"
                );
            }
            discovered.push(DiscoveredGuild {
                guild_id,
                storage,
                channel,
                role_id: config.role_id,
            });
        }
        discovered
    }

    /// Cached `NameIndex` over the last retained snapshot, keyed by the
    /// snapshot generation: repeated watch commands between polls reuse one
    /// build instead of re-cloning the catalog per invocation. A stale
    /// generation rebuilds once (two concurrent builders just race the
    /// store - harmless). `None` = nothing retained yet.
    async fn cached_index(&self) -> Option<Arc<NameIndex>> {
        let generation = self.snapshot_generation.load(Ordering::Acquire);
        {
            let cache = self.name_index_cache.lock();
            if let Some((cached_gen, index)) = cache.as_ref() {
                if *cached_gen == generation {
                    return Some(Arc::clone(index));
                }
            }
        }
        let snapshot = self.last_snapshot.lock().clone()?;
        let index = Arc::new(self.name_index(&snapshot).await);
        *self.name_index_cache.lock() = Some((generation, Arc::clone(&index)));
        Some(index)
    }

    /// Boot catch-up source: the plugin-global store document, or - for a
    /// deployment upgrading from the per-guild layout - the first legacy
    /// `last_seen` copy among known guilds (the copies are identical by
    /// construction; a guild whose persist failed keeps a stale copy, and
    /// the first readable one wins - bounded by at-most-once delivery, so
    /// the skew costs at most one replayed or missed delta after a
    /// restart). The `bool` marks the legacy path: the caller force-writes
    /// the new document and deletes the copies.
    async fn load_persisted(&self) -> Option<(PersistedStore, bool)> {
        if let Some(doc) = self.load_persisted_doc().await {
            return Some((doc, false));
        }
        let legacy = self.load_legacy_state().await?;
        tracing::info!(
            "migrating the per-guild store state copy into the plugin-global store document"
        );
        Some((
            PersistedStore {
                schema: STORE_SCHEMA,
                saved_at_unix: unix_now(),
                last_seen: legacy,
                snapshot: Snapshot::default(),
            },
            true,
        ))
    }

    /// Reads and parses the plugin-global store document. A malformed or
    /// unreadable document starts fresh (silent baseline) - detection
    /// state must never block a poll cycle.
    async fn load_persisted_doc(&self) -> Option<PersistedStore> {
        match self.plugin_storage.get(NAMESPACE, STORE_DUMP_KEY).await {
            Ok(Some(raw)) => match serde_json::from_value::<PersistedStore>(raw) {
                Ok(doc) if doc.schema == STORE_SCHEMA => Some(doc),
                Ok(doc) => {
                    tracing::warn!(
                        found = doc.schema,
                        expected = STORE_SCHEMA,
                        "persisted store document has an unknown schema - starting fresh"
                    );
                    None
                }
                Err(err) => {
                    tracing::warn!(%err, "persisted store document malformed - starting fresh");
                    None
                }
            },
            Ok(None) => None,
            Err(err) => {
                tracing::warn!(%err, "persisted store document unreadable - starting fresh");
                None
            }
        }
    }

    /// The legacy per-guild catch-up source (see [`Self::load_persisted`]).
    async fn load_legacy_state(&self) -> Option<LastSeen> {
        let guilds = self.storage.list_guilds().await.ok()?;
        for (row_platform, guild_id) in guilds {
            if row_platform != self.platform.slug() {
                continue;
            }
            let storage = self.storage.guild_scoped(self.platform.slug(), guild_id);
            match storage.get(NAMESPACE, LAST_SEEN_KEY).await {
                Ok(Some(raw)) => match serde_json::from_value::<LastSeen>(raw) {
                    Ok(state) => return Some(state),
                    Err(err) => {
                        tracing::warn!(%err, guild = guild_id.get(), "persisted store state malformed")
                    }
                },
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        %err,
                        guild = guild_id.get(),
                        "persisted store state unreadable - boot catch-up skipped for it"
                    );
                }
            }
        }
        None
    }

    /// Removes the legacy per-guild copies after a successful migration.
    /// Fire-and-forget: a failed delete leaves a stale (ignored) copy.
    async fn cleanup_legacy_state(&self) {
        let Ok(guilds) = self.storage.list_guilds().await else {
            tracing::warn!("guild listing failed - legacy store state copies kept");
            return;
        };
        for (row_platform, guild_id) in guilds {
            if row_platform != self.platform.slug() {
                continue;
            }
            let storage = self.storage.guild_scoped(self.platform.slug(), guild_id);
            if let Err(err) = storage.delete(NAMESPACE, LAST_SEEN_KEY).await {
                tracing::warn!(
                    %err,
                    guild = guild_id.get(),
                    "failed to delete the legacy per-guild store state copy"
                );
            }
        }
    }

    /// Writes the plugin-global store document: the compacted state plus
    /// the retained raw snapshot in one value. A failure is a log line -
    /// the in-memory state still advanced, so the next change rewrites
    /// everything (bounded by at-most-once delivery, like the announces).
    async fn persist_store(&self, state: &LastSeen) {
        let doc = PersistedStore {
            schema: STORE_SCHEMA,
            saved_at_unix: unix_now(),
            last_seen: state.clone(),
            snapshot: self.last_snapshot.lock().clone().unwrap_or_default(),
        };
        let Ok(value) = serde_json::to_value(&doc) else {
            tracing::warn!("store document failed to serialize - persistence skipped");
            return;
        };
        if let Err(err) = self.plugin_storage.set(NAMESPACE, STORE_DUMP_KEY, value).await {
            tracing::warn!(%err, "failed to persist the store document");
        }
    }

    /// Writes today's history digest (and prunes beyond the retention
    /// window). `refresh` marks a cycle where the state or a raw section
    /// changed - the digest is always rewritten. A quiet cycle passes
    /// `false` and writes only when the UTC day flipped (tracked in
    /// [`Self::last_history_day`]), so every observed day has a document
    /// without rewriting it once per poll. `history_days = 0` disables
    /// recording; a cycle without raw data (migration adopt) writes
    /// nothing - an empty digest must never overwrite a real day.
    async fn write_history_digest(&self, refresh: bool) -> bool {
        let history_days = self.settings.lock().history_days;
        if history_days == 0 {
            return false;
        }
        let snapshot = self.last_snapshot.lock().clone();
        let Some(snapshot) = snapshot.filter(has_store_data) else { return false };
        let now = unix_now();
        let day = history::day_key(now);
        if !refresh && self.last_history_day.lock().as_deref() == Some(day.as_str()) {
            return false;
        }
        // The retention block has already absorbed every changed section
        // and bumped the generation, so the generation-keyed index cache
        // serves (and warms) exactly the data a fresh build would use -
        // without re-cloning the whole catalog per cycle.
        let index = self.cached_index().await.unwrap_or_else(|| {
            // Unreachable after the retention block (it materializes a
            // default snapshot), kept total for the same reason.
            Arc::new(NameIndex::empty())
        });
        let digest = history::capture(&day, &snapshot, &index);
        let Ok(value) = serde_json::to_value(&digest) else {
            tracing::warn!("store history digest failed to serialize - day skipped");
            return false;
        };
        let key = format!("{}{day}", history::HISTORY_PREFIX);
        if let Err(err) = self.plugin_storage.set(NAMESPACE, &key, value).await {
            tracing::warn!(%err, day = %day, "failed to persist the store history digest");
            return false;
        }
        *self.last_history_day.lock() = Some(day);
        self.prune_history(now, history_days).await;
        true
    }

    /// Deletes day digests older than the retention window: the newest
    /// `history_days` UTC days (today included) survive. Failures are
    /// logged and retried on the next write - pruning is best-effort
    /// housekeeping, never a correctness path.
    async fn prune_history(&self, now_unix: u64, history_days: u32) {
        let cutoff = history::day_key_before(now_unix, u64::from(history_days).saturating_sub(1));
        let Ok(keys) = self.plugin_storage.list_keys(NAMESPACE).await else {
            tracing::warn!("store history listing failed - pruning skipped");
            return;
        };
        for key in keys {
            let Some(day) = key.strip_prefix(history::HISTORY_PREFIX) else { continue };
            if day < cutoff.as_str()
                && let Err(err) = self.plugin_storage.delete(NAMESPACE, &key).await
            {
                tracing::warn!(%err, key = %key, "failed to prune an old store history digest");
            }
        }
    }

    /// Renders the `/lol_client_status` text (ephemeral reply).
    #[must_use]
    pub fn status_text(&self) -> String {
        let state = self.state.lock();
        let mut lines = vec![format!(
            "League client: {}",
            if self.online.load(Ordering::Acquire) { "online" } else { "offline" }
        )];
        lines.push(match *self.last_poll.lock() {
            Some(at) => format!("Last poll: {}s ago", at.elapsed().as_secs()),
            None => "Last poll: never".to_owned(),
        });
        match *self.last_announcement.lock() {
            Some((at, _)) => {
                lines.push(format!("Last update rendered: {}s ago", at.elapsed().as_secs()));
            }
            None => lines.push("Last update rendered: none yet".to_owned()),
        }
        match state.as_ref() {
            Some(state) => {
                lines.push(format!("Sales tracked: {}", state.sales.len()));
                lines.push(format!("Skins tracked: {}", state.skins.len()));
                if state.rotations.is_empty() {
                    lines.push("Rotations: none seen yet".to_owned());
                } else {
                    for (name, rotation) in &state.rotations {
                        let next = super::format::non_empty(rotation.next_rotation.as_deref())
                            .map(super::format::date)
                            .unwrap_or_else(|| "?".to_owned());
                        lines.push(format!(
                            "Rotation {} ({}): next {}",
                            name,
                            rotation.entry_ids.len(),
                            next
                        ));
                    }
                }
                let yourshop = state
                    .yourshop
                    .as_ref()
                    .map_or("unknown", |state| if state.active { "active" } else { "inactive" });
                lines.push(format!("Your Shop: {yourshop}"));
            }
            None => lines.push("Store state: not captured yet".to_owned()),
        }
        let flags = self.settings.lock().flags;
        lines.push(format!(
            "Announce: sales={} skins={} mythic={} yourshop={}",
            on_off(flags.sales),
            on_off(flags.new_skins),
            on_off(flags.mythic_rotation),
            on_off(flags.yourshop),
        ));
        lines.join("\n")
    }

    /// The announcement as delivered - one entry per embed page (the
    /// `/lol_client_status` "last update rendered" memory).
    #[must_use]
    pub fn last_announcement_pages(&self) -> Vec<String> {
        self.last_announcement.lock().as_ref().map_or_else(Vec::new, |(_, pages)| pages.clone())
    }

    /// Renders the retained raw snapshot as a full "current store" dump -
    /// what `/lol_store_dump` posts: all current deals (sales, mythic
    /// rotations, an active Your Shop), not the last delta. Skins are
    /// omitted because "new in store" is a diff concept a lone snapshot
    /// cannot provide (the alternative would print the whole catalog).
    pub async fn current_store_pages(&self) -> Vec<String> {
        let Some(snapshot) = self.last_snapshot.lock().clone() else { return Vec::new() };
        self.render_current(&snapshot).await
    }

    /// `/lol_store_dump`'s fallback: renders the persisted store document
    /// when this process has not captured a snapshot yet (fresh boot, the
    /// client still offline). Champion names degrade to the synthetic
    /// fallback when the champion table is unreachable - same as a failed
    /// in-cycle fetch.
    pub async fn persisted_store_pages(&self) -> Vec<String> {
        let Some(doc) = self.load_persisted_doc().await else { return Vec::new() };
        self.render_current(&doc.snapshot).await
    }

    /// The shared current-state renderer (see [`Self::current_store_pages`]).
    async fn render_current(&self, snapshot: &Snapshot) -> Vec<String> {
        if !has_renderable_data(snapshot) {
            return Vec::new();
        }
        if snapshot.catalog.is_none() {
            tracing::warn!(
                "store catalog unavailable - sale names degrade to bare ids \
                 (check the 'some store sources failed' warnings)"
            );
        }
        let index = self.name_index(snapshot).await;
        let mut delta = StoreDelta::default();
        if let Some(sales) = &snapshot.sales {
            delta.sales = sales.clone();
        }
        if let Some(stores) = &snapshot.rotations {
            for store in rotation_stores(stores) {
                if let Some(rotation) = rotation_delta(store) {
                    delta.rotations.push(rotation);
                }
            }
        }
        if let Some(status) = &snapshot.yourshop {
            if status.hub_enabled.unwrap_or(false) {
                delta.yourshop = Some(YourShopStart {
                    start: status.start_time.clone(),
                    end: status.end_time.clone(),
                });
            }
        }
        announce_pages(&delta, &index, self.embed_budget())
    }

    /// The recorded deal history for a watch-shaped target: one row per
    /// observed deal, windows folded by identity - an observed day without
    /// the deal closes its window, unobserved days merge across, most
    /// recent window first. Bounded by the retention window; an empty
    /// `Some` = nothing recorded, `None` = the history listing failed
    /// (transient - the caller says so instead of claiming an empty
    /// history).
    pub(crate) async fn deal_history(
        &self,
        target: &WatchTarget,
    ) -> Option<Vec<history::HistoryRow>> {
        let keys = match self.plugin_storage.list_keys(NAMESPACE).await {
            Ok(keys) => keys,
            Err(err) => {
                tracing::warn!(%err, "store history listing failed - history unavailable");
                return None;
            }
        };
        let mut days: Vec<&str> =
            keys.iter().filter_map(|key| key.strip_prefix(history::HISTORY_PREFIX)).collect();
        days.sort_unstable(); // ISO dates sort lexicographically
        let mut rows = Vec::new();
        for day in days {
            let key = format!("{}{day}", history::HISTORY_PREFIX);
            let raw = match self.plugin_storage.get(NAMESPACE, &key).await {
                Ok(Some(raw)) => raw,
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(%err, day, "store history digest unreadable - day skipped");
                    continue;
                }
            };
            match serde_json::from_value::<DayDigest>(raw) {
                Ok(digest) if digest.schema == history::DIGEST_SCHEMA => {
                    history::extend_rows(&mut rows, &digest, day, target);
                }
                Ok(digest) => tracing::warn!(
                    day,
                    found = digest.schema,
                    "store history digest has an unknown schema - day skipped"
                ),
                Err(err) => {
                    tracing::warn!(%err, day, "store history digest malformed - day skipped")
                }
            }
        }
        Some(history::finalize_rows(rows))
    }

    /// Byte budget for one announcement embed, derived from the
    /// deployment's platform embed cap.
    fn embed_budget(&self) -> usize {
        super::format::embed_budget(self.platform.embed_limit())
    }

    /// The deployment's outbound message cap - the budget currency for the
    /// ephemeral history reply (plain content, not an embed).
    pub fn message_limit(&self) -> Option<usize> {
        self.platform.message_limit()
    }
}

fn on_off(flag: bool) -> &'static str {
    if flag { "on" } else { "off" }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|since| since.as_secs()).unwrap_or(0)
}

/// Scheduler adapter: one poll tick.
pub(crate) struct PollJob<B: EventBusPort> {
    pub engine: Arc<StoreEngine<B>>,
}

#[async_trait]
impl<B: EventBusPort> Job for PollJob<B> {
    async fn run(&self) {
        self.engine.tick().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::plugin_ports::EventHandler;
    use crate::kernel::spi_ports::PluginStoragePort;
    use crate::plugins::lol_store::lcu::{
        CatalogItem, ChampionEntry, ItemRef, LcuError, LocalizedText, Price, RotationStore, Sale,
        SaleInfo, StoreEntry, YourShopStatus,
    };
    use crate::test_support::{
        ChannelRecordingFactory, FailingChatOutputFactory, InMemoryPluginStorage, InMemoryStorage,
        RecordingChatOutput, RecordingChatOutputFactory,
    };
    use parking_lot::Mutex as PLMutex;
    use std::collections::BTreeMap;

    type EventsLog = Vec<(&'static str, u64, &'static str)>;

    /// Bus double: records (event, guild, kind) publications; clones share
    /// one log, so the fixture and the engine see the same stream.
    #[derive(Clone, Default)]
    struct RecorderBus {
        events: Arc<PLMutex<EventsLog>>,
    }

    impl RecorderBus {
        fn log(&self) -> EventsLog {
            self.events.lock().clone()
        }
    }

    impl EventBusPort for RecorderBus {
        fn publish(&self, event: Arc<dyn crate::kernel::models::Event>) {
            if let Some(announced) = event.as_any().downcast_ref::<LolStoreAnnounced>() {
                self.events.lock().push((
                    "lol_store.announced",
                    announced.guild_id.get(),
                    announced.kind.as_str(),
                ));
            }
        }

        fn subscribe<E: crate::kernel::models::Event + 'static>(
            &self,
            _handler: Arc<dyn EventHandler<E>>,
        ) -> crate::kernel::plugin_ports::EventBusSubscription {
            // Publisher-side tests only; no subscription ever fires here.
            crate::kernel::plugin_ports::EventBusSubscription::new(Arc::new(|| {}))
        }
    }

    /// Configurable LCU fake: `None` per source = offline error.
    struct FakeLcu {
        catalog: PLMutex<Option<Vec<CatalogItem>>>,
        sales: PLMutex<Option<Vec<Sale>>>,
        rotations: PLMutex<Option<Vec<RotationStore>>>,
        yourshop: PLMutex<Option<YourShopStatus>>,
        /// When set, the champion-table fetch fails - fixture for the
        /// fallback-names degradation path.
        champions_fail: PLMutex<bool>,
    }

    impl FakeLcu {
        fn online() -> Arc<Self> {
            Arc::new(Self {
                catalog: PLMutex::new(Some(Vec::new())),
                sales: PLMutex::new(Some(Vec::new())),
                rotations: PLMutex::new(Some(Vec::new())),
                yourshop: PLMutex::new(Some(YourShopStatus::default())),
                champions_fail: PLMutex::new(false),
            })
        }

        fn offline() -> Arc<Self> {
            Arc::new(Self {
                catalog: PLMutex::new(None),
                sales: PLMutex::new(None),
                rotations: PLMutex::new(None),
                yourshop: PLMutex::new(None),
                champions_fail: PLMutex::new(false),
            })
        }
    }

    #[async_trait]
    impl LcuPort for FakeLcu {
        async fn catalog(&self) -> Result<Vec<CatalogItem>, LcuError> {
            self.catalog.lock().clone().ok_or_else(|| LcuError::Offline("test".to_owned()))
        }

        async fn sales(&self) -> Result<Vec<Sale>, LcuError> {
            self.sales.lock().clone().ok_or_else(|| LcuError::Offline("test".to_owned()))
        }

        async fn rotations(&self) -> Result<Vec<RotationStore>, LcuError> {
            self.rotations.lock().clone().ok_or_else(|| LcuError::Offline("test".to_owned()))
        }

        async fn yourshop_status(&self) -> Result<YourShopStatus, LcuError> {
            self.yourshop.lock().clone().ok_or_else(|| LcuError::Offline("test".to_owned()))
        }

        async fn champion_names(&self) -> Result<Vec<ChampionEntry>, LcuError> {
            if *self.champions_fail.lock() {
                return Err(LcuError::Offline("test".to_owned()));
            }
            Ok(vec![ChampionEntry { id: 103, name: Some("Ahri".to_owned()) }])
        }
    }

    const GUILD: u64 = 700;

    fn plugin_store() -> Arc<dyn PluginStorage> {
        Arc::new(InMemoryPluginStorage::new())
            .plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG)
    }

    struct Fixture {
        storage: Arc<InMemoryStorage>,
        plugin: Arc<InMemoryPluginStorage>,
        output: Arc<RecordingChatOutput>,
        bus: RecorderBus,
        lcu: Arc<FakeLcu>,
        engine: Arc<StoreEngine<RecorderBus>>,
    }

    async fn fixture_with(lcu: Arc<FakeLcu>, flags: AnnounceFlags) -> Fixture {
        fixture_with_settings(
            lcu,
            EngineSettings {
                poll: Duration::from_secs(60),
                flags,
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        )
        .await
    }

    async fn fixture_with_settings(lcu: Arc<FakeLcu>, settings: EngineSettings) -> Fixture {
        let storage = Arc::new(InMemoryStorage::new());
        let plugin = Arc::new(InMemoryPluginStorage::new());
        let output = RecordingChatOutput::new();
        let factory = RecordingChatOutputFactory::new(Arc::clone(&output)).boxed();
        let bus = RecorderBus::default();
        let port: Arc<dyn LcuPort> = lcu.clone();
        let engine = Arc::new(StoreEngine::new(
            port,
            Arc::clone(&storage) as Arc<dyn StoragePort>,
            plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            factory,
            bus.clone(),
            crate::test_support::test_platform_info(),
            settings,
        ));
        Fixture { storage, plugin, output, bus, lcu, engine }
    }

    async fn fixture(lcu: Arc<FakeLcu>) -> Fixture {
        fixture_with(lcu, AnnounceFlags::all_on()).await
    }

    /// Hot reload: the announce flags, watch caps, and the history retention
    /// swap; the poll interval stays at its boot value (the scheduled job's
    /// cadence).
    #[test]
    fn update_settings_swaps_flags_caps_and_history_but_keeps_poll() {
        let plugin = Arc::new(InMemoryPluginStorage::new());
        let engine = Arc::new(StoreEngine::new(
            FakeLcu::online(),
            Arc::new(InMemoryStorage::new()) as Arc<dyn StoragePort>,
            plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecorderBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: 7,
                watch_guild_cap: 9,
                history_days: 30,
            },
        ));

        engine.update_settings(EngineSettings {
            poll: Duration::from_secs(1),
            flags: AnnounceFlags {
                sales: false,
                new_skins: true,
                mythic_rotation: false,
                yourshop: true,
            },
            watch_user_cap: 2,
            watch_guild_cap: 3,
            history_days: 7,
        });
        let settings = engine.settings();
        assert!(!settings.flags.sales);
        assert!(settings.flags.new_skins);
        assert!(!settings.flags.mythic_rotation);
        assert_eq!(settings.watch_user_cap, 2);
        assert_eq!(settings.watch_guild_cap, 3);
        assert_eq!(settings.history_days, 7);
        assert_eq!(settings.poll, Duration::from_secs(60), "poll stays boot-frozen");
    }

    async fn enable_guild(storage: &InMemoryStorage, guild: u64, channel: Option<&str>) {
        let scoped = storage.guild_scoped("test", GuildId(guild));
        scoped
            .set(
                NAMESPACE,
                CONFIG_KEY,
                serde_json::json!({ "enabled": true, "channel_id": channel }),
            )
            .await
            .expect("config write expected");
    }

    fn skin_item_named(item_id: u64, price: u64, name: &str) -> CatalogItem {
        CatalogItem {
            item_id,
            inventory_type: Some("CHAMPION_SKIN".to_owned()),
            prices: vec![Price { cost: Some(price), currency: Some("RP".to_owned()) }],
            localizations: BTreeMap::from([(
                "en_US".to_owned(),
                LocalizedText { name: Some(name.to_owned()) },
            )]),
            item_requirements: vec![ItemRef {
                inventory_type: Some("CHAMPION".to_owned()),
                item_id: Some(103),
            }],
        }
    }

    fn skin_item(item_id: u64, price: u64) -> CatalogItem {
        skin_item_named(item_id, price, "Foxfire Ahri")
    }

    /// Seeds one watch subscription for user 111.
    async fn seed_watch(
        storage: &InMemoryStorage,
        guild: u64,
        target: serde_json::Value,
        kinds: &str,
    ) {
        let scoped = storage.guild_scoped("test", GuildId(guild));
        scoped
            .set(
                NAMESPACE,
                watch::WATCH_KEY,
                serde_json::json!({
                    "version": 1,
                    "next_id": 2,
                    "subs": [{ "id": 1, "user_id": "111", "target": target, "kinds": kinds }]
                }),
            )
            .await
            .expect("watch write expected");
    }

    fn skin_target_value(item_id: u64) -> serde_json::Value {
        serde_json::json!({
            "type": "skin", "item_id": item_id,
            "champion": "Ahri", "skin": "Foxfire Ahri"
        })
    }

    fn champion_target_value() -> serde_json::Value {
        serde_json::json!({ "type": "champion", "champion_id": 103, "champion": "Ahri" })
    }

    fn skin_sale(id: u64, item_id: u64) -> Sale {
        Sale {
            id,
            item: ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(item_id),
            },
            sale: SaleInfo {
                start_date: None,
                end_date: Some("2026-10-05T17:00:00.000+00:00".to_owned()),
                prices: vec![Price { cost: Some(607), currency: Some("RP".to_owned()) }],
            },
        }
    }

    /// A CHAMPION-typed sale (the champion itself, not a skin) - dropped at
    /// ingestion since the feed is about skins.
    fn champion_sale() -> Sale {
        Sale {
            id: 2,
            item: ItemRef { inventory_type: Some("CHAMPION".to_owned()), item_id: Some(799) },
            sale: SaleInfo {
                start_date: None,
                end_date: Some("2026-10-05T17:00:00.000+00:00".to_owned()),
                prices: vec![Price { cost: Some(790), currency: Some("RP".to_owned()) }],
            },
        }
    }

    fn rotation_store(name: &str, ids: &[&str]) -> RotationStore {
        use crate::plugins::lol_store::diff::MYTHIC_SHOP_ID;
        use crate::plugins::lol_store::lcu::{DisplayMetadata, RotatingMetadata, ShoppefrontMeta};
        RotationStore {
            name: Some(name.to_owned()),
            display_metadata: Some(DisplayMetadata {
                shoppefront: Some(ShoppefrontMeta {
                    id: Some(MYTHIC_SHOP_ID.to_owned()),
                    categories: vec!["WEEKLY".to_owned()],
                }),
            }),
            rotating_store_metadata: Some(RotatingMetadata {
                rotation_cadence: Some("PT168H".to_owned()),
                curr_rotation_start_time: Some("2026-10-01T00:00:00.000Z".to_owned()),
                next_rotation_start_time: Some("2026-10-08T00:00:00.000Z".to_owned()),
            }),
            catalog_entries: ids
                .iter()
                .map(|id| StoreEntry {
                    id: Some((*id).to_owned()),
                    name: Some("entry".to_owned()),
                    end_time: None,
                    purchase_units: Vec::new(),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn first_poll_is_a_silent_baseline_but_persists_the_store_document() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        // Champion sales are dropped at ingestion - only the skin sale enters
        // the baseline state.
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031), champion_sale()]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);

        f.engine.tick().await;

        assert!(f.output.messages().is_empty(), "baseline must stay silent");
        assert!(f.bus.log().is_empty());
        let persisted = f
            .plugin
            .plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG)
            .get(NAMESPACE, STORE_DUMP_KEY)
            .await
            .expect("read expected")
            .expect("the store document must be persisted for boot catch-up");
        let last_seen = persisted.get("last_seen").expect("last_seen half expected");
        assert_eq!(
            last_seen.get("sales"),
            Some(&serde_json::json!([1])),
            "only skin sales enter the baseline: {last_seen}"
        );
        assert_eq!(last_seen.get("skins"), Some(&serde_json::json!([1031])));
        // The dump half carries the raw sections.
        let snapshot = persisted.get("snapshot").expect("snapshot half expected");
        assert!(snapshot.get("sales").is_some(), "raw sales persisted: {snapshot}");
        assert!(snapshot.get("catalog").is_some(), "raw catalog persisted: {snapshot}");
    }

    /// A fresh launch has no announcement to dump - the dump falls back to
    /// the current snapshot: sales and rotations as they stand, but no
    /// skins section ("new in store" needs history; a lone snapshot would
    /// otherwise print the whole catalog).
    #[tokio::test]
    async fn fresh_launch_dump_renders_the_current_store_without_skins() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        assert!(f.engine.current_store_pages().await.is_empty(), "no poll yet");

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        *f.lcu.rotations.lock() = Some(vec![rotation_store("WEEKLY", &["e1", "e2"])]);
        f.engine.tick().await; // silent baseline
        assert!(f.engine.last_announcement_pages().is_empty());

        let text = f.engine.current_store_pages().await.join("\n\n");
        assert!(text.contains("New sales"), "current sales show: {text}");
        assert!(text.contains("Mythic rotation"), "current rotation shows: {text}");
        assert!(!text.contains("New in store"), "skins need history: {text}");
    }

    /// The champion ITSELF going on sale is not store news: it is dropped
    /// at ingestion and never reaches announcements, dumps or watches.
    #[tokio::test]
    async fn champion_sales_are_excluded_from_the_feed() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031), champion_sale()]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // silent baseline keeps only the skin sale

        let text = f.engine.current_store_pages().await.join("\n\n");
        assert!(text.contains("Foxfire Ahri"), "skin sale present: {text}");
        assert!(!text.contains("Champion 799"), "champion sale excluded: {text}");
    }

    #[tokio::test]
    async fn new_sale_is_announced_with_record_and_event() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline without the sale

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 1, "exactly one announcement expected");
        let first = messages.first().expect("announcement expected");
        assert!(first.contains("Ahri — Foxfire Ahri"), "joined names expected");
        assert!(first.contains("38%"), "percent off expected: {messages:?}");
        assert!(first.contains("607 RP"));
        assert!(first.contains("until 2026-10-05"));

        let scoped = f.storage.guild_scoped("test", GuildId(GUILD));
        let records = scoped.list_last(NAMESPACE, 10).await.expect("records read expected");
        assert_eq!(records.len(), 1, "the announcement must be recorded");
        assert_eq!(f.bus.log(), vec![("lol_store.announced", GUILD, "sales")]);
    }

    #[tokio::test]
    async fn catalog_failure_degrades_to_the_last_good_catalog() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // silent baseline; caches the catalog

        // The catalog source fails this cycle; the sale still announces -
        // joined against the cached catalog, not synthetic ids.
        *f.lcu.catalog.lock() = None;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 1, "the sale must still announce");
        let first = messages.first().expect("announcement expected");
        assert!(first.contains("Ahri \u{2014} Foxfire Ahri"), "cached join expected: {first}");
        assert!(!first.contains("Skin 1031"), "no synthetic id may reach the record");
    }

    /// One failed source must not kill the cycle: the other sections still
    /// announce, the failed source keeps its previous state, and the client
    /// counts as online (3 of 4 sources answering).
    #[tokio::test]
    async fn a_partial_source_failure_keeps_previous_state_and_stays_online() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await; // baseline

        *f.lcu.sales.lock() = None; // sales source fails this cycle
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975), skin_item(1101, 975)]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 1, "the catalog change still announces");
        let first = messages.first().expect("announcement expected");
        assert!(
            first.contains("**New in store**"),
            "a failed source must not block other sections: {first}"
        );
        assert!(f.engine.status_text().contains("League client: online"));
    }

    /// A tracker toggled off must not announce - but the state still
    /// advances, so re-enabling never replays old events.
    #[tokio::test]
    async fn a_disabled_tracker_still_updates_state_silently() {
        let mut flags = AnnounceFlags::all_on();
        flags.sales = false;
        let f = fixture_with(FakeLcu::online(), flags).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        f.engine.tick().await; // baseline without sales

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await; // sale found, sales announcements off

        assert!(f.output.messages().is_empty(), "no announcement with the flag off");
        assert!(f.bus.log().is_empty());
        assert!(f.engine.status_text().contains("Sales tracked: 1"));
    }

    #[tokio::test]
    async fn configured_role_is_tagged_in_content_not_embed() {
        let f = fixture(FakeLcu::online()).await;
        // Enabled + channel + subscription role.
        f.storage
            .guild_scoped("test", GuildId(GUILD))
            .set(
                NAMESPACE,
                CONFIG_KEY,
                serde_json::json!({ "enabled": true, "channel_id": "55", "role_id": "999" }),
            )
            .await
            .expect("config write expected");
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await;

        let sent = f.output.sent();
        assert_eq!(sent.len(), 1);
        let message = sent.first().expect("announcement expected");
        assert_eq!(message.content, "<@&999>", "the role tag rides the content");
        let embed = message.embeds.first().expect("embed expected");
        assert!(!embed.description.contains("<@&"), "embeds never notify");
    }

    /// The subscription-role ping rides the FIRST page only: one event, one
    /// ping - continuations stay silent instead of re-pinging per page.
    #[tokio::test]
    async fn role_tag_rides_only_the_first_page() {
        let f = fixture(FakeLcu::online()).await;
        f.storage
            .guild_scoped("test", GuildId(GUILD))
            .set(
                NAMESPACE,
                CONFIG_KEY,
                serde_json::json!({ "enabled": true, "channel_id": "55", "role_id": "999" }),
            )
            .await
            .expect("config write expected");
        // Enough sales that the announcement paginates.
        *f.lcu.sales.lock() = Some((0..300u64).map(|i| skin_sale(i + 1, 10_000 + i)).collect());
        f.engine.tick().await; // baseline

        *f.lcu.sales.lock() = Some((0..300u64).map(|i| skin_sale(1_000 + i, 20_000 + i)).collect());
        f.engine.tick().await;

        let mut sent = f.output.sent();
        assert!(sent.len() >= 2, "a multi-page announcement was expected");
        let first = sent.first().expect("first page expected");
        assert_eq!(first.content, "<@&999>", "first page pings the role");
        for message in sent.iter().skip(1) {
            assert!(message.content.is_empty(), "continuations stay silent");
        }
    }

    /// A failed champion-table fetch degrades the champion prefix to the
    /// fallback name - but never stalls the run or swallows the
    /// announcement.
    #[tokio::test]
    async fn champion_table_failure_degrades_names_but_still_announces() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.champions_fail.lock() = true;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline

        *f.lcu.sales.lock() = Some(vec![skin_sale(2, 1031)]);
        f.engine.tick().await;

        let text = f.output.messages().join("\n");
        // The catalog join keeps the skin name; the champion prefix falls
        // back to the id-derived name ("Ahri" would mean the table loaded).
        assert!(text.contains("Champion 103 \u{2014} Foxfire Ahri"), "degraded line: {text}");
    }

    /// Persistence is plugin-global now - a malformed-config guild no
    /// longer participates in anything store-side. The consolidated
    /// document stays current regardless: a later fixed config rejoins
    /// from current state instead of replaying an old delta.
    #[tokio::test]
    async fn malformed_config_guild_is_skipped_entirely_but_state_stays_current() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        f.storage
            .guild_scoped("test", GuildId(999))
            .set(NAMESPACE, CONFIG_KEY, serde_json::json!({ "enabled": "not-a-bool" }))
            .await
            .expect("config write expected");
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // silent baseline

        *f.lcu.sales.lock() = Some(vec![skin_sale(2, 1031)]);
        f.engine.tick().await;

        // The healthy guild announced; the malformed one is absent from the
        // bus trail entirely.
        let log = f.bus.log();
        assert!(!log.is_empty(), "healthy guild announced: {log:?}");
        assert!(log.iter().all(|(_, guild, _)| *guild == GUILD), "skipped guild leaked: {log:?}");
        assert_eq!(f.output.sent().len(), 1, "only the healthy guild was messaged");
        // No legacy per-guild copies exist anymore - the consolidated
        // document is the only persistence site.
        for guild in [GUILD, 999] {
            let copy = f
                .storage
                .guild_scoped("test", GuildId(guild))
                .get(NAMESPACE, LAST_SEEN_KEY)
                .await
                .expect("last_seen read expected");
            assert!(copy.is_none(), "guild {guild} must not hold a legacy copy");
        }
    }

    #[tokio::test]
    async fn offline_poll_is_silent_and_marks_the_client_offline() {
        let f = fixture(FakeLcu::offline()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        f.engine.tick().await;
        assert!(f.output.messages().is_empty());
        assert!(!f.engine.status_text().contains("League client: online"));
    }

    #[tokio::test]
    async fn boot_catchup_announces_what_happened_while_down() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.rotations.lock() = Some(vec![rotation_store("WEEKLY_V1", &["a"])]);
        f.engine.tick().await; // boot 1 baseline

        // Boot 2 shares the plugin-global document store (the storage the
        // boot-1 engine persisted into) and the guild's config document,
        // but starts with empty in-memory state - the rotation swapped
        // while the bot was "down".
        let bus2 = RecorderBus::default();
        let lcu2 = FakeLcu::online();
        *lcu2.rotations.lock() = Some(vec![rotation_store("WEEKLY_V1", &["b"])]);
        let factory2 = RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed();
        let engine2 = Arc::new(StoreEngine::new(
            lcu2,
            Arc::clone(&f.storage) as Arc<dyn StoragePort>,
            f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            factory2,
            bus2.clone(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));
        engine2.tick().await;

        assert!(
            bus2.log().iter().any(|(_, _, kind)| *kind == "mythic_rotation"),
            "catch-up must announce the rotation change"
        );
    }

    /// The persisted store document re-arms the raw snapshot side before
    /// the first successful poll: the current-state dump renders from the
    /// document even while the client is offline, and after the first
    /// (offline) tick the restored snapshot lives in memory.
    #[tokio::test]
    async fn boot_restores_the_raw_snapshot_from_the_store_document() {
        let f = fixture(FakeLcu::online()).await;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline + persist

        let lcu2 = FakeLcu::offline();
        let engine2 = Arc::new(StoreEngine::new(
            lcu2,
            Arc::new(InMemoryStorage::new()) as Arc<dyn StoragePort>,
            f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecorderBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));

        // Before any poll: the document fallback renders the last dump.
        let from_doc = engine2.persisted_store_pages().await.join("\n");
        assert!(from_doc.contains("Foxfire Ahri"), "document fallback renders: {from_doc}");
        assert!(engine2.current_store_pages().await.is_empty(), "nothing in memory yet");

        // The offline tick still adopts + restores (the catch-up runs
        // before the fetch).
        engine2.tick().await;
        let restored = engine2.current_store_pages().await.join("\n");
        assert!(restored.contains("Foxfire Ahri"), "restored snapshot renders: {restored}");
        // Name search works off the restored catalog.
        let search = engine2.search_store("Foxfire").await.expect("search expected");
        assert_eq!(search.skins.len(), 1, "the restored catalog backs the search");
    }

    /// A deployment upgrading from the per-guild layout: the legacy copy is
    /// adopted, the consolidated document written, the copies deleted.
    #[tokio::test]
    async fn legacy_per_guild_copy_migrates_into_the_store_document() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        f.storage
            .guild_scoped("test", GuildId(GUILD))
            .set(NAMESPACE, LAST_SEEN_KEY, serde_json::json!({ "sales": [1], "skins": [1031] }))
            .await
            .expect("legacy write expected");

        // The store kept selling the same sale while down: no delta, but
        // the raw data changed, so the migration write still fires.
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await;

        let doc = f
            .plugin
            .plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG)
            .get(NAMESPACE, STORE_DUMP_KEY)
            .await
            .expect("read expected")
            .expect("the consolidated document must exist after migration");
        assert_eq!(
            doc.get("last_seen").and_then(|seen| seen.get("sales")),
            Some(&serde_json::json!([1])),
            "adopted state: {doc}"
        );
        let copy = f
            .storage
            .guild_scoped("test", GuildId(GUILD))
            .get(NAMESPACE, LAST_SEEN_KEY)
            .await
            .expect("read expected");
        assert!(copy.is_none(), "the legacy copy must be deleted after migration");
    }

    /// History recording: today's digest is written with capture-time
    /// joins (names, champion ids, prices) on every raw change.
    #[tokio::test]
    async fn history_digest_written_with_baked_names() {
        let f = fixture(FakeLcu::online()).await;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item_named(1031, 975, "Foxfire Ahri")]);
        f.engine.tick().await;

        let key = format!("{}{}", history::HISTORY_PREFIX, history::day_key(unix_now()));
        let raw = f
            .plugin
            .plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG)
            .get(NAMESPACE, &key)
            .await
            .expect("read expected")
            .expect("today's digest must exist");
        let digest: DayDigest = serde_json::from_value(raw).expect("digest shape expected");
        let [record] = digest.sales.as_slice() else { panic!("one sale expected: {digest:?}") };
        assert_eq!(record.sale_id, 1);
        assert_eq!(record.champion_id, Some(103));
        assert_eq!(record.label, "Ahri \u{2014} Foxfire Ahri");
        assert_eq!(record.sale_price, Some(607));
        assert_eq!(record.original_price, Some(975));
    }

    /// Retention: days older than `history_days` are pruned on the next
    /// write; today's document survives.
    #[tokio::test]
    async fn history_prunes_days_beyond_retention() {
        let f = fixture(FakeLcu::online()).await;
        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        plugin
            .set(
                NAMESPACE,
                "history.2000-01-01",
                serde_json::json!({ "schema": 1, "day": "2000-01-01" }),
            )
            .await
            .expect("seed write expected");

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await;

        let keys = plugin.list_keys(NAMESPACE).await.expect("list expected");
        assert!(!keys.iter().any(|key| key == "history.2000-01-01"), "stale day pruned: {keys:?}");
        assert!(
            keys.iter().any(|key| key.starts_with(history::HISTORY_PREFIX)),
            "today's digest kept: {keys:?}"
        );
    }

    /// `history_days = 0` disables recording entirely - no digest, no prune.
    #[tokio::test]
    async fn history_disabled_writes_nothing() {
        let f = fixture_with_settings(
            FakeLcu::online(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: 0,
            },
        )
        .await;
        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        plugin
            .set(
                NAMESPACE,
                "history.2000-01-01",
                serde_json::json!({ "schema": 1, "day": "2000-01-01" }),
            )
            .await
            .expect("seed write expected");

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await;

        let keys = plugin.list_keys(NAMESPACE).await.expect("list expected");
        let history_days: Vec<_> =
            keys.iter().filter(|key| key.starts_with(history::HISTORY_PREFIX)).collect();
        assert_eq!(
            history_days,
            vec!["history.2000-01-01"],
            "only the pre-existing seed doc remains - recording wrote nothing"
        );
    }

    /// The history query path: day documents fold into per-deal windows,
    /// most recent first, across the engine's storage handle.
    #[tokio::test]
    async fn deal_history_folds_days_into_windows() {
        let f = fixture(FakeLcu::online()).await;
        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        let digest_for = |day: &str, sale_id: u64| DayDigest {
            schema: history::DIGEST_SCHEMA,
            day: day.to_owned(),
            sales: vec![history::SaleRecord {
                sale_id,
                item_id: 1031,
                champion_id: Some(103),
                label: "Ahri \u{2014} Foxfire Ahri".to_owned(),
                sale_price: Some(607),
                original_price: Some(975),
                start: None,
                end: None,
            }],
            mythic: Vec::new(),
        };
        for (day, sale_id) in [("2026-09-01", 7u64), ("2026-09-02", 7), ("2026-09-04", 8)] {
            plugin
                .set(
                    NAMESPACE,
                    &format!("{}{day}", history::HISTORY_PREFIX),
                    serde_json::to_value(digest_for(day, sale_id)).expect("serialize expected"),
                )
                .await
                .expect("digest write expected");
        }

        let target = WatchTarget::Skin {
            item_id: 1031,
            champion: "Ahri".to_owned(),
            skin: "Foxfire Ahri".to_owned(),
        };
        let rows = f.engine.deal_history(&target).await.expect("history listing expected");
        assert_eq!(rows.len(), 2, "one window per sale id: {rows:?}");
        let (recent, older) = (rows.first().expect("recent row"), rows.get(1).expect("older row"));
        assert_eq!(recent.kind, "sale");
        assert_eq!(recent.last_day, "2026-09-04", "most recent first");
        assert_eq!(older.first_day, "2026-09-01");
        assert_eq!(older.last_day, "2026-09-02");
    }

    /// A `store_dump` document from another schema version (or half-written
    /// by a future format) must baseline silently: deserializing it as an
    /// empty state would replay every active deal as a fresh delta.
    #[tokio::test]
    async fn unknown_schema_store_document_baselines_instead_of_replaying() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        plugin
            .set(NAMESPACE, STORE_DUMP_KEY, serde_json::json!({ "schema": 99, "saved_at_unix": 0 }))
            .await
            .expect("seed write expected");

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await;

        assert!(
            f.output.sent().is_empty(),
            "unknown schema must baseline, not replay: {:?}",
            f.output.sent()
        );
        let raw = plugin
            .get(NAMESPACE, STORE_DUMP_KEY)
            .await
            .expect("read expected")
            .expect("document rewritten");
        assert_eq!(
            raw.get("schema"),
            Some(&serde_json::json!(1)),
            "rewritten at the current schema"
        );

        *f.lcu.sales.lock() = Some(vec![skin_sale(2, 1031)]);
        f.engine.tick().await;
        assert_eq!(f.output.sent().len(), 1, "the next delta announces normally");
    }

    /// The migration completes even when the migrating tick is fully
    /// offline: adoption writes the document and deletes the legacy copies
    /// before the poll - which returns early - can skip them.
    #[tokio::test]
    async fn offline_migrating_tick_still_completes_the_migration() {
        let f = fixture(FakeLcu::offline()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        f.storage
            .guild_scoped("test", GuildId(GUILD))
            .set(NAMESPACE, LAST_SEEN_KEY, serde_json::json!({ "sales": [1], "skins": [1031] }))
            .await
            .expect("legacy write expected");

        f.engine.tick().await; // client offline - the poll skips, the migration must not

        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        let raw = plugin
            .get(NAMESPACE, STORE_DUMP_KEY)
            .await
            .expect("read expected")
            .expect("document written during adoption");
        assert_eq!(raw.get("schema"), Some(&serde_json::json!(1)));
        let copy = f
            .storage
            .guild_scoped("test", GuildId(GUILD))
            .get(NAMESPACE, LAST_SEEN_KEY)
            .await
            .expect("read expected");
        assert!(copy.is_none(), "legacy copy deleted during adoption");
    }

    /// A quiet observed cycle still owes its day digest: the write happens
    /// once, when the UTC day flips, and same-day quiet cycles skip.
    #[tokio::test]
    async fn quiet_observed_cycle_writes_the_flipped_day_once() {
        let f = fixture(FakeLcu::online()).await;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline: today's digest written
        let today = history::day_key(unix_now());
        let key = format!("{}{today}", history::HISTORY_PREFIX);
        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        assert!(plugin.get(NAMESPACE, &key).await.expect("read expected").is_some());

        // Same-day quiet cycle: no rewrite (the deleted doc stays gone).
        plugin.delete(NAMESPACE, &key).await.expect("delete expected");
        *f.engine.last_history_day.lock() = Some(today.clone());
        f.engine.tick().await;
        assert!(
            plugin.get(NAMESPACE, &key).await.expect("read expected").is_none(),
            "same-day quiet cycle must not rewrite"
        );

        // UTC day flip: the quiet cycle writes the new day's digest.
        *f.engine.last_history_day.lock() = Some("2000-01-01".to_owned());
        f.engine.tick().await;
        assert!(
            plugin.get(NAMESPACE, &key).await.expect("read expected").is_some(),
            "flipped day's digest written"
        );
    }

    /// Retention is inclusive: exactly `history_days` UTC days survive a
    /// prune (today included).
    #[tokio::test]
    async fn prune_keeps_exactly_history_days() {
        let f = fixture_with_settings(
            FakeLcu::online(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: 2,
            },
        )
        .await;
        let now = unix_now();
        let day = |back: u64| history::day_key_before(now, back);
        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        for back in [1u64, 2] {
            let d = day(back);
            plugin
                .set(
                    NAMESPACE,
                    &format!("{}{d}", history::HISTORY_PREFIX),
                    serde_json::json!({ "schema": 1, "day": d }),
                )
                .await
                .expect("seed write expected");
        }

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // change cycle: digest write + prune

        let keys = plugin.list_keys(NAMESPACE).await.expect("list expected");
        let mut days: Vec<&str> =
            keys.iter().filter_map(|key| key.strip_prefix(history::HISTORY_PREFIX)).collect();
        days.sort_unstable();
        assert_eq!(
            days,
            vec![day(1).as_str(), day(0).as_str()],
            "exactly history_days survive: {keys:?}"
        );
    }

    /// A failed history listing is not an empty history: the engine
    /// surfaces it as `None` so the command can own the reply.
    #[tokio::test]
    async fn deal_history_listing_failure_is_not_an_empty_history() {
        let failing = crate::test_support::FailingPluginStorage::new();
        let engine = Arc::new(StoreEngine::new(
            FakeLcu::online(),
            Arc::new(InMemoryStorage::new()) as Arc<dyn StoragePort>,
            failing.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed(),
            RecorderBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));
        let target = WatchTarget::Skin {
            item_id: 1031,
            champion: "Ahri".to_owned(),
            skin: "Foxfire Ahri".to_owned(),
        };
        assert!(engine.deal_history(&target).await.is_none());
    }

    /// A day digest with an unknown schema is skipped; readable days still
    /// fold in.
    #[tokio::test]
    async fn unknown_schema_day_digest_is_skipped() {
        let f = fixture(FakeLcu::online()).await;
        let plugin = f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG);
        plugin
            .set(
                NAMESPACE,
                "history.2099-01-01",
                serde_json::json!({ "schema": 99, "day": "2099-01-01" }),
            )
            .await
            .expect("seed write expected");
        plugin
            .set(
                NAMESPACE,
                "history.2026-09-01",
                serde_json::to_value(DayDigest {
                    schema: history::DIGEST_SCHEMA,
                    day: "2026-09-01".to_owned(),
                    sales: vec![history::SaleRecord {
                        sale_id: 7,
                        item_id: 1031,
                        champion_id: Some(103),
                        label: "Ahri \u{2014} Foxfire Ahri".to_owned(),
                        sale_price: Some(607),
                        original_price: Some(975),
                        start: None,
                        end: None,
                    }],
                    mythic: Vec::new(),
                })
                .expect("serialize expected"),
            )
            .await
            .expect("seed write expected");

        let target = WatchTarget::Skin {
            item_id: 1031,
            champion: "Ahri".to_owned(),
            skin: "Foxfire Ahri".to_owned(),
        };
        let rows = f.engine.deal_history(&target).await.expect("listing expected");
        assert_eq!(rows.len(), 1, "only the readable day folds in: {rows:?}");
        assert_eq!(rows.first().expect("row expected").first_day, "2026-09-01");
    }

    #[tokio::test]
    async fn enabled_but_unassigned_guild_receives_nothing() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, None).await;
        f.engine.tick().await;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await;

        assert!(f.output.messages().is_empty(), "no channel assigned - nothing to announce");
    }

    #[tokio::test]
    async fn restart_with_no_store_change_is_silent() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline WITH the sale
        assert!(f.output.messages().is_empty());

        // The fresh engine adopts the persisted state (which already holds
        // the sale) and the store has not changed: silent.
        let factory2 = RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed();
        let engine2 = Arc::new(StoreEngine::new(
            FakeLcu::online(),
            Arc::clone(&f.storage) as Arc<dyn StoragePort>,
            f.plugin.plugin_scoped(crate::test_support::TEST_PLATFORM_SLUG),
            factory2,
            f.bus.clone(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));
        engine2.tick().await;
        assert!(f.bus.log().is_empty(), "restart dedup: no re-announcement");
    }

    #[tokio::test]
    async fn yourshop_start_is_announced_once_with_times() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        f.engine.tick().await; // baseline: inactive

        *f.lcu.yourshop.lock() = Some(YourShopStatus {
            hub_enabled: Some(true),
            start_time: Some("2026-10-01T09:00:00Z".to_owned()),
            end_time: Some("2026-10-08T09:00:00Z".to_owned()),
        });
        f.engine.tick().await;
        let messages = f.output.messages();
        assert_eq!(messages.len(), 1);
        let first = messages.first().expect("announcement expected");
        assert!(first.contains("Your Shop started"));
        assert!(first.contains("2026-10-01"));
        assert!(first.contains("ends 2026-10-08"));

        f.engine.tick().await;
        assert_eq!(f.output.messages().len(), 1, "still active: silent");
    }

    #[tokio::test]
    async fn a_matching_sale_pings_the_watcher_in_the_announcement_channel() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        seed_watch(&f.storage, GUILD, skin_target_value(1031), "sale").await;
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 2, "general announcement + watch ping expected");
        let ping = messages.last().expect("ping expected");
        assert!(ping.contains("<@111>"), "the tag rides the content: {messages:?}");
        assert!(ping.contains("Foxfire Ahri"));

        let scoped = f.storage.guild_scoped("test", GuildId(GUILD));
        let records = scoped.list_last(NAMESPACE, 10).await.expect("records read expected");
        let kinds: Vec<&str> = records
            .iter()
            .filter_map(|record| record.payload.get("kind").and_then(|kind| kind.as_str()))
            .collect();
        assert!(kinds.contains(&"watch_notification"), "the ping must be recorded: {kinds:?}");
    }

    #[tokio::test]
    async fn watch_pings_ignore_disabled_announce_flags() {
        let flags =
            AnnounceFlags { sales: false, new_skins: true, mythic_rotation: true, yourshop: true };
        let f = fixture_with(FakeLcu::online(), flags).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        seed_watch(&f.storage, GUILD, skin_target_value(1031), "sale").await;
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 1, "feed flag off, personal watch still fires");
        let ping = messages.first().expect("ping expected");
        assert!(ping.contains("<@111>"), "watch ping expected: {messages:?}");
    }

    #[tokio::test]
    async fn a_champion_watch_catches_any_skin_of_that_champion() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        seed_watch(&f.storage, GUILD, champion_target_value(), "sale").await;
        *f.lcu.catalog.lock() = Some(vec![skin_item_named(1032, 975, "Dynasty Ahri")]);
        f.engine.tick().await; // baseline

        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1032)]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 2, "announcement + champion watch ping expected");
        let ping = messages.last().expect("ping expected");
        assert!(ping.contains("<@111>"), "champion join must match: {messages:?}");
    }

    #[tokio::test]
    async fn release_watches_fire_on_new_skins_for_champions_only() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        // Champion release watch + a skin release watch that must stay silent.
        let scoped = f.storage.guild_scoped("test", GuildId(GUILD));
        scoped
            .set(
                NAMESPACE,
                watch::WATCH_KEY,
                serde_json::json!({
                    "version": 1,
                    "next_id": 3,
                    "subs": [
                        { "id": 1, "user_id": "111", "target": champion_target_value(), "kinds": "release" },
                        { "id": 2, "user_id": "222", "target": skin_target_value(1031), "kinds": "release" }
                    ]
                }),
            )
            .await
            .expect("watch write expected");
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline

        *f.lcu.catalog.lock() =
            Some(vec![skin_item(1031, 975), skin_item_named(1032, 975, "Dynasty Ahri")]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 2, "announcement + one release ping expected");
        let ping = messages.last().expect("ping expected");
        assert!(ping.contains("<@111>"), "champion watcher pinged: {messages:?}");
        assert!(!ping.contains("<@222>"), "a catalog-resolvable skin never releases: {messages:?}");
    }

    #[tokio::test]
    async fn a_mythic_watch_fires_when_a_watched_skin_enters_the_rotation() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        seed_watch(&f.storage, GUILD, skin_target_value(1031), "mythic").await;
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline: no rotation

        *f.lcu.rotations.lock() = Some(vec![rotation_store("weekly", &["1031"])]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 2, "announcement + mythic watch ping expected");
        let ping = messages.last().expect("ping expected");
        assert!(ping.contains("<@111>"), "rotation entry id must join: {messages:?}");
        assert!(ping.contains("entry"), "the rendered mythic line expected: {messages:?}");
    }

    /// The engine routes to the ASSIGNED channel, not the origin channel:
    /// announcements and watch pings must land on the configured channel
    /// and nowhere else.
    #[tokio::test]
    async fn announcements_and_pings_land_on_the_assigned_channel() {
        let storage = Arc::new(InMemoryStorage::new());
        let factory = Arc::new(ChannelRecordingFactory::new());
        let lcu = FakeLcu::online();
        *lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        let engine = Arc::new(StoreEngine::new(
            lcu.clone(),
            Arc::clone(&storage) as Arc<dyn StoragePort>,
            plugin_store(),
            Arc::clone(&factory) as Arc<dyn ChatOutputFactoryPort>,
            RecorderBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));
        enable_guild(&storage, GUILD, Some("55")).await;
        seed_watch(&storage, GUILD, skin_target_value(1031), "sale").await;
        engine.tick().await; // baseline

        *lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        engine.tick().await;

        let targeted = factory.targeted();
        assert_eq!(targeted.len(), 2, "announcement + watch ping: {targeted:?}");
        assert!(
            targeted.iter().all(|send| send.guild == GUILD && send.channel == 55),
            "both sends land on the assigned channel: {targeted:?}"
        );
        assert!(targeted.last().expect("ping").text.contains("<@111>"));
    }

    /// A failed announcement send is a log line: no record, no bus event -
    /// but the state still advances, so nothing replays next tick.
    #[tokio::test]
    async fn failed_announcement_delivery_does_not_record_or_replay() {
        let storage = Arc::new(InMemoryStorage::new());
        let lcu = FakeLcu::online();
        *lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        let bus = RecorderBus::default();
        let engine = Arc::new(StoreEngine::new(
            lcu.clone(),
            Arc::clone(&storage) as Arc<dyn StoragePort>,
            plugin_store(),
            Arc::new(FailingChatOutputFactory) as Arc<dyn ChatOutputFactoryPort>,
            bus.clone(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));
        enable_guild(&storage, GUILD, Some("55")).await;
        engine.tick().await; // baseline

        *lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        engine.tick().await; // delivery fails

        let scoped = storage.guild_scoped("test", GuildId(GUILD));
        let records = scoped.list_last(NAMESPACE, 10).await.expect("records read expected");
        assert!(records.is_empty(), "no record on failed delivery: {records:?}");
        assert!(bus.log().is_empty(), "no bus event on failed delivery");

        // The state advanced: replaying the same store is silent.
        engine.tick().await;
        assert!(bus.log().is_empty(), "no replay after the failed send");
    }

    /// Same contract for watch pings: a failed notification send records
    /// nothing and never replays (the announcement side fails with it here,
    /// which the assertions cover just the same).
    #[tokio::test]
    async fn failed_watch_delivery_is_also_at_most_once() {
        let storage = Arc::new(InMemoryStorage::new());
        let lcu = FakeLcu::online();
        *lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        let bus = RecorderBus::default();
        let engine = Arc::new(StoreEngine::new(
            lcu.clone(),
            Arc::clone(&storage) as Arc<dyn StoragePort>,
            plugin_store(),
            Arc::new(FailingChatOutputFactory) as Arc<dyn ChatOutputFactoryPort>,
            bus.clone(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));
        enable_guild(&storage, GUILD, Some("55")).await;
        seed_watch(&storage, GUILD, skin_target_value(1031), "sale").await;
        engine.tick().await; // baseline

        *lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        engine.tick().await; // both sends fail

        let scoped = storage.guild_scoped("test", GuildId(GUILD));
        let records = scoped.list_last(NAMESPACE, 10).await.expect("records read expected");
        assert!(records.is_empty(), "neither announcement nor watch recorded");
        assert!(bus.log().is_empty());

        engine.tick().await;
        assert!(bus.log().is_empty(), "no replay after the failed sends");
    }

    /// Two guilds track the same store: each gets its own announcement in
    /// its own channel, and only its own watches fire - a watch in one
    /// guild never pings in another (isolation at the engine level).
    #[tokio::test]
    async fn two_guilds_receive_only_their_own_feed_and_pings() {
        let storage = Arc::new(InMemoryStorage::new());
        let factory = Arc::new(ChannelRecordingFactory::new());
        let lcu = FakeLcu::online();
        *lcu.catalog.lock() =
            Some(vec![skin_item(1031, 975), skin_item_named(1032, 975, "Dynasty Ahri")]);
        let engine = Arc::new(StoreEngine::new(
            lcu.clone(),
            Arc::clone(&storage) as Arc<dyn StoragePort>,
            plugin_store(),
            Arc::clone(&factory) as Arc<dyn ChatOutputFactoryPort>,
            RecorderBus::default(),
            crate::test_support::test_platform_info(),
            EngineSettings {
                poll: Duration::from_secs(60),
                flags: AnnounceFlags::all_on(),
                watch_user_cap: watch::DEFAULT_USER_CAP,
                watch_guild_cap: watch::DEFAULT_GUILD_CAP,
                history_days: DEFAULT_HISTORY_DAYS,
            },
        ));
        enable_guild(&storage, 700, Some("55")).await;
        enable_guild(&storage, 701, Some("66")).await;
        seed_watch(&storage, 700, skin_target_value(1031), "sale").await;
        seed_watch(&storage, 701, skin_target_value(1032), "sale").await;
        engine.tick().await; // baseline

        *lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        engine.tick().await;

        let targeted = factory.targeted();
        // 2 announcements + one ping (guild 700's watch; 701 watches a
        // different skin).
        assert_eq!(targeted.len(), 3, "targeted sends: {targeted:?}");
        let ping = targeted.last().expect("ping expected");
        assert_eq!(ping.guild, 700);
        assert_eq!(ping.channel, 55);
        assert!(ping.text.contains("<@111>"));
        assert!(
            targeted.iter().all(|send| send.guild != 701 || !send.text.contains("<@")),
            "guild 701's feed never carries another guild's pings: {targeted:?}"
        );

        // Records stay guild-scoped: 700 has announcement + watch record,
        // 701 only the announcement.
        let records = |guild: u64| {
            let storage = Arc::clone(&storage);
            async move {
                storage
                    .guild_scoped("test", GuildId(guild))
                    .list_last(NAMESPACE, 10)
                    .await
                    .expect("records read expected")
            }
        };
        let kinds_700: Vec<String> = records(700)
            .await
            .iter()
            .filter_map(|record| {
                record.payload.get("kind").and_then(|kind| kind.as_str()).map(str::to_owned)
            })
            .collect();
        let kinds_701: Vec<String> = records(701)
            .await
            .iter()
            .filter_map(|record| {
                record.payload.get("kind").and_then(|kind| kind.as_str()).map(str::to_owned)
            })
            .collect();
        assert!(kinds_700.contains(&"watch_notification".to_owned()), "{kinds_700:?}");
        assert_eq!(kinds_701, vec!["announcement".to_owned()], "{kinds_701:?}");
    }

    /// `watch_status` reads current activity straight from the retained
    /// snapshot: the watched skin on sale reports it, for both the skin and
    /// the champion target shape; an unrelated skin reports idle.
    #[tokio::test]
    async fn watch_status_reports_current_activity_from_the_snapshot() {
        let f = fixture(FakeLcu::online()).await;
        let target = WatchTarget::Skin {
            item_id: 1031,
            champion: "Ahri".to_owned(),
            skin: "Foxfire Ahri".to_owned(),
        };
        // No snapshot yet.
        assert_eq!(f.engine.watch_status(&target).await, None);

        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        f.engine.tick().await; // baseline captures the active sale

        assert_eq!(
            f.engine.watch_status(&target).await,
            Some(vec!["currently on sale".to_owned()])
        );
        let champion = WatchTarget::Champion { champion_id: 103, champion: "Ahri".to_owned() };
        assert_eq!(
            f.engine.watch_status(&champion).await,
            Some(vec!["currently on sale".to_owned()])
        );
        let other = WatchTarget::Skin {
            item_id: 999_031,
            champion: "Ahri".to_owned(),
            skin: "Some Other Skin".to_owned(),
        };
        assert_eq!(f.engine.watch_status(&other).await, Some(Vec::new()));
    }

    /// The mythic branch of `watch_status` joins the retained rotation
    /// snapshot through the catalog: an entry id that resolves reports
    /// "currently in the mythic rotation" for skin and champion targets, a
    /// catalog-unresolvable entry falls back to its raw display name, and an
    /// unrelated target stays idle.
    #[tokio::test]
    async fn watch_status_reports_mythic_rotation() {
        let f = fixture(FakeLcu::online()).await;
        let target = WatchTarget::Skin {
            item_id: 1031,
            champion: "Ahri".to_owned(),
            skin: "Foxfire Ahri".to_owned(),
        };
        // No snapshot yet.
        assert_eq!(f.engine.watch_status(&target).await, None);

        // Entry "1031" joins the catalog (skin 1031); entry "555" resolves
        // only through its raw display name.
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        *f.lcu.rotations.lock() = Some(vec![rotation_store("weekly", &["1031", "555"])]);
        f.engine.tick().await; // baseline retains the active rotation

        assert_eq!(
            f.engine.watch_status(&target).await,
            Some(vec!["currently in the mythic rotation".to_owned()])
        );
        let champion = WatchTarget::Champion { champion_id: 103, champion: "Ahri".to_owned() };
        assert_eq!(
            f.engine.watch_status(&champion).await,
            Some(vec!["currently in the mythic rotation".to_owned()])
        );
        // A catalog-unresolvable entry matches through its display name.
        let by_name = WatchTarget::Skin {
            item_id: 999_031,
            champion: "Ahri".to_owned(),
            skin: "entry".to_owned(),
        };
        assert_eq!(
            f.engine.watch_status(&by_name).await,
            Some(vec!["currently in the mythic rotation".to_owned()])
        );
        // Negative control: nothing matches, no rotation line.
        let other = WatchTarget::Skin {
            item_id: 999_031,
            champion: "Ahri".to_owned(),
            skin: "Some Other Skin".to_owned(),
        };
        assert_eq!(f.engine.watch_status(&other).await, Some(Vec::new()));
    }

    #[tokio::test]
    async fn unrelated_deltas_never_ping_watchers() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        seed_watch(&f.storage, GUILD, skin_target_value(1031), "all").await;
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);
        f.engine.tick().await; // baseline

        // A sale of a different skin: announcement, but no ping.
        *f.lcu.catalog.lock() =
            Some(vec![skin_item(1031, 975), skin_item_named(999_031, 975, "Some Other Skin")]);
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 999_031)]);
        f.engine.tick().await;

        let messages = f.output.messages();
        assert_eq!(messages.len(), 1, "only the general announcement expected");
        assert!(!messages.first().expect("announcement").contains("<@111>"));
    }
}
