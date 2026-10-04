//! The poll engine: one scheduler-driven tick fetches the four store
//! sources, diffs against the last-seen state, and fans the delta out to
//! every guild that enabled the tracker.
//!
//! State layering: the live `last_seen` is in-memory (one store per bot -
//! the League client is local). A compacted copy is persisted per enabled
//! guild (`last_seen` doc in the plugin namespace) purely for boot catch-up:
//! after a restart the engine loads the first persisted copy and announces
//! what changed while it was down; with no persisted copy the first
//! successful poll is a silent baseline. The per-guild record log receives
//! every announcement - user-facing history, never consulted for detection.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::kernel::models::{ChannelId, Embed, GuildId, Origin, OutboundMessage, Platform, UserId};
use crate::kernel::plugin_ports::{EventBusPort, Job};
use crate::kernel::spi_ports::{ChatOutputFactoryPort, StoragePort};

use super::diff::{self, LastSeen, Snapshot, StoreDelta};
use super::events::{LolStoreAnnounced, StoreEventKind};
use super::format::{NameIndex, announce_text, champion_map};
use super::lcu::LcuPort;

/// Guild storage namespace (the plugin's slug).
pub const NAMESPACE: &str = "lol_store";
/// Guild config document: enabled flag + announcement channel.
pub const CONFIG_KEY: &str = "config";
/// Boot catch-up document: the compacted last-seen store state.
pub const LAST_SEEN_KEY: &str = "last_seen";

/// Which trackers announce. Unannounced trackers still update the state, so
/// re-enabling never replays old events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnounceFlags {
    pub sales: bool,
    pub new_skins: bool,
    pub mythic_rotation: bool,
    pub yourshop: bool,
}

/// Engine construction settings (`[lol]` config section, startup-only).
#[derive(Debug, Clone)]
pub struct EngineSettings {
    pub poll: Duration,
    pub flags: AnnounceFlags,
}

/// Per-guild config document shape.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GuildConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Announcement channel (string - snowflakes exceed JSON numbers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<String>,
}

/// What `/lol_client_status` and `/lol_store_dump` render.
pub struct StatusSnapshot {
    pub configured: bool,
    pub online: bool,
    pub last_poll_ago: Option<Duration>,
    pub last_announcement_ago: Option<Duration>,
    pub sales_tracked: usize,
    pub skins_tracked: usize,
    pub rotations: Vec<(String, String)>,
    pub yourshop_active: Option<bool>,
}

/// The poll engine (see module docs). Generic over the bus like every
/// bus-publishing plugin.
pub struct StoreEngine<B: EventBusPort> {
    lcu: Arc<dyn LcuPort>,
    storage: Arc<dyn StoragePort>,
    factory: Arc<dyn ChatOutputFactoryPort>,
    bus: B,
    settings: EngineSettings,
    state: Mutex<Option<LastSeen>>,
    champions: Mutex<Option<Arc<HashMap<u64, String>>>>,
    online: AtomicBool,
    last_poll: Mutex<Option<Instant>>,
    last_announcement: Mutex<Option<(Instant, String)>>,
}

impl<B: EventBusPort> StoreEngine<B> {
    #[must_use]
    pub fn new(
        lcu: Arc<dyn LcuPort>,
        storage: Arc<dyn StoragePort>,
        factory: Arc<dyn ChatOutputFactoryPort>,
        bus: B,
        settings: EngineSettings,
    ) -> Self {
        Self {
            lcu,
            storage,
            factory,
            bus,
            settings,
            state: Mutex::new(None),
            champions: Mutex::new(None),
            online: AtomicBool::new(false),
            last_poll: Mutex::new(None),
            last_announcement: Mutex::new(None),
        }
    }

    #[must_use]
    pub fn settings(&self) -> &EngineSettings {
        &self.settings
    }

    /// One poll cycle. Never fails outward: every failure mode is a logged,
    /// skipped cycle (the client being closed is the normal steady state).
    pub async fn tick(&self) {
        *self.last_poll.lock() = Some(Instant::now());

        // Boot catch-up: with no in-memory state, adopt the first persisted
        // copy so the diff announces what happened while the bot was down.
        if self.state.lock().is_none() {
            if let Some(persisted) = self.load_persisted_state().await {
                tracing::debug!("adopting persisted store state for catch-up");
                *self.state.lock() = Some(persisted);
            }
        }
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
        let mut failures: Vec<&str> = Vec::new();
        match sales {
            Ok(data) => snapshot.sales = Some(data),
            Err(_) => failures.push("sales"),
        }
        match catalog {
            Ok(data) => snapshot.catalog = Some(data),
            Err(_) => failures.push("catalog"),
        }
        match rotations {
            Ok(data) => snapshot.rotations = Some(data),
            Err(_) => failures.push("rotations"),
        }
        match yourshop {
            Ok(data) => snapshot.yourshop = Some(data),
            Err(_) => failures.push("yourshop"),
        }

        if failures.len() == 4 {
            // Client closed (or restarting): the normal quiet path.
            self.online.store(false, Ordering::Release);
            tracing::debug!(?failures, "store poll skipped - league client unreachable");
            return;
        }
        self.online.store(true, Ordering::Release);
        if !failures.is_empty() {
            tracing::warn!(
                ?failures,
                "some store sources failed this cycle - keeping previous state"
            );
        }

        let current = match &previous {
            Some(previous) => diff::merge(previous, &snapshot),
            // Silent baseline: first successful poll with no persisted state.
            None => diff::merge(&LastSeen::default(), &snapshot),
        };
        let changed = previous.as_ref() != Some(&current);

        let mut delta = previous.as_ref().map(|previous| diff::compute(previous, &snapshot));
        if let Some(delta) = &mut delta {
            if !self.settings.flags.sales {
                delta.sales.clear();
            }
            if !self.settings.flags.new_skins {
                delta.skins.clear();
            }
            if !self.settings.flags.mythic_rotation {
                delta.rotations.clear();
            }
            if !self.settings.flags.yourshop {
                delta.yourshop = None;
            }
        }
        let announces = delta.as_ref().is_some_and(|delta| !delta.is_empty());

        // Announce before the state swap: formatting joins against the
        // freshly fetched catalog.
        if announces {
            let delta = delta.unwrap_or_default();
            match self.announce(&delta, &snapshot).await {
                Ok(text) => {
                    *self.last_announcement.lock() = Some((Instant::now(), text));
                }
                Err(err) => tracing::warn!(%err, "store announcement fan-out failed"),
            }
        }

        if changed {
            *self.state.lock() = Some(current.clone());
            self.persist_state(&current).await;
        }

        tracing::debug!(
            sales = current.sales.len(),
            skins = current.skins.len(),
            rotations = current.rotations.len(),
            announced = announces,
            "store poll complete"
        );
    }

    /// Formats and fans one delta out: per enabled guild - embed, record,
    /// bus event. Returns the rendered text (for the status memory).
    async fn announce(&self, delta: &StoreDelta, snapshot: &Snapshot) -> anyhow::Result<String> {
        let index = self.name_index(snapshot).await;
        let Some(text) = announce_text(delta, &index) else {
            return Ok(String::new());
        };

        let targets = self.enabled_guilds().await;
        if targets.is_empty() {
            tracing::debug!("store updates found but no guild is tracking them");
            return Ok(text);
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

        let message = OutboundMessage::embed(Embed {
            title: "LoL Store updates".to_owned(),
            description: text.clone(),
        });
        let at_unix =
            SystemTime::now().duration_since(UNIX_EPOCH).map(|since| since.as_secs()).unwrap_or(0);

        for &(platform, guild_id, channel_id) in &targets {
            let origin = Origin {
                platform,
                guild_id: Some(guild_id),
                channel_id,
                user_id: UserId(0),
                message_id: None,
                reply_token: None,
            };
            let storage = self.storage.guild_scoped(platform, guild_id);
            let output = self.factory.channel_output(&origin, channel_id);
            if let Err(err) = output.send(message.clone()).await {
                tracing::warn!(%err, guild = guild_id.get(), "failed to deliver store announcement");
                continue;
            }
            if let Err(err) = storage
                .append(
                    NAMESPACE,
                    serde_json::json!({ "kind": "announcement", "text": text, "at_unix": at_unix }),
                )
                .await
            {
                tracing::warn!(%err, guild = guild_id.get(), "failed to record store announcement");
            }
            for kind in &kinds {
                self.bus.publish(Arc::new(LolStoreAnnounced { platform, guild_id, kind: *kind }));
            }
        }

        tracing::info!(
            guilds = targets.len(),
            kinds = ?kinds.iter().map(|kind| kind.as_str()).collect::<Vec<_>>(),
            "store announcements delivered"
        );
        Ok(text)
    }

    /// Catalog + champion joins, with a lazily loaded and cached champion
    /// name table (one process-lifetime fetch unless a name is missing).
    async fn name_index(&self, snapshot: &Snapshot) -> NameIndex {
        // Load-once champion table; the guard never crosses an await.
        if self.champions.lock().is_none() {
            match self.lcu.champion_names().await {
                Ok(entries) => {
                    let map = champion_map(entries.into_iter().map(|entry| (entry.id, entry.name)));
                    let mut cache = self.champions.lock();
                    if cache.is_none() {
                        *cache = Some(Arc::new(map.into_iter().collect()));
                    }
                }
                Err(err) => {
                    tracing::warn!(%err, "champion name table unavailable - using fallback names");
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
        let catalog = snapshot.catalog.clone().unwrap_or_default();
        NameIndex::new(catalog, champions)
    }

    /// Guilds with the tracker enabled and a channel assigned.
    async fn enabled_guilds(&self) -> Vec<(Platform, GuildId, ChannelId)> {
        let mut targets = Vec::new();
        let Ok(guilds) = self.storage.list_guilds().await else {
            return targets;
        };
        for (platform, guild_id) in guilds {
            let storage = self.storage.guild_scoped(platform, guild_id);
            let Ok(Some(raw)) = storage.get(NAMESPACE, CONFIG_KEY).await else {
                continue;
            };
            let Ok(config) = serde_json::from_value::<GuildConfig>(raw) else {
                tracing::warn!(guild = guild_id.get(), "store tracker config malformed - skipping");
                continue;
            };
            if !config.enabled {
                continue;
            }
            let Some(channel) = config.channel_id.as_deref().and_then(|id| id.parse::<u64>().ok())
            else {
                tracing::warn!(guild = guild_id.get(), "store tracker enabled but channel missing");
                continue;
            };
            targets.push((platform, guild_id, ChannelId(channel)));
        }
        targets
    }

    /// Boot catch-up source: the first persisted `last_seen` among known
    /// guilds (the copies are identical by construction).
    async fn load_persisted_state(&self) -> Option<LastSeen> {
        let guilds = self.storage.list_guilds().await.ok()?;
        for (platform, guild_id) in guilds {
            let storage = self.storage.guild_scoped(platform, guild_id);
            if let Ok(Some(raw)) = storage.get(NAMESPACE, LAST_SEEN_KEY).await {
                match serde_json::from_value::<LastSeen>(raw) {
                    Ok(state) => return Some(state),
                    Err(err) => {
                        tracing::warn!(%err, guild = guild_id.get(), "persisted store state malformed")
                    }
                }
            }
        }
        None
    }

    /// Persists the current state to every guild that has a config document
    /// (enabled or not, so a re-enable never replays stale events).
    async fn persist_state(&self, state: &LastSeen) {
        let Ok(value) = serde_json::to_value(state) else {
            tracing::warn!("store state failed to serialize - catch-up persistence skipped");
            return;
        };
        let Ok(guilds) = self.storage.list_guilds().await else {
            return;
        };
        for (platform, guild_id) in guilds {
            let storage = self.storage.guild_scoped(platform, guild_id);
            let has_config = matches!(storage.get(NAMESPACE, CONFIG_KEY).await, Ok(Some(_)));
            if !has_config {
                continue;
            }
            if let Err(err) = storage.set(NAMESPACE, LAST_SEEN_KEY, value.clone()).await {
                tracing::warn!(%err, guild = guild_id.get(), "failed to persist store state");
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
                lines.push(format!("Last announcement: {}s ago", at.elapsed().as_secs()));
            }
            None => lines.push("Last announcement: none yet".to_owned()),
        }
        match state.as_ref() {
            Some(state) => {
                lines.push(format!("Sales tracked: {}", state.sales.len()));
                lines.push(format!("Skins tracked: {}", state.skins.len()));
                if state.rotations.is_empty() {
                    lines.push("Rotations: none seen yet".to_owned());
                } else {
                    for (name, rotation) in &state.rotations {
                        let next = rotation
                            .next_rotation
                            .as_deref()
                            .map(super::format::timestamp)
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
        let flags = &self.settings.flags;
        lines.push(format!(
            "Announce: sales={} skins={} mythic={} yourshop={}",
            on_off(flags.sales),
            on_off(flags.new_skins),
            on_off(flags.mythic_rotation),
            on_off(flags.yourshop),
        ));
        lines.join("\n")
    }

    /// The last delivered announcement text, for `/lol_store_dump`.
    #[must_use]
    pub fn last_announcement_text(&self) -> Option<String> {
        let last = self.last_announcement.lock();
        last.as_ref().map(|(_, text)| text.clone())
    }
}

fn on_off(flag: bool) -> &'static str {
    if flag { "on" } else { "off" }
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
    use crate::plugins::lol_store::lcu::{
        CatalogItem, ChampionEntry, ItemRef, LcuError, LocalizedText, Price, RotationStore, Sale,
        SaleInfo, StoreEntry, YourShopStatus,
    };
    use crate::test_support::{InMemoryStorage, RecordingChatOutput, RecordingChatOutputFactory};
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
    }

    impl FakeLcu {
        fn online() -> Arc<Self> {
            Arc::new(Self {
                catalog: PLMutex::new(Some(Vec::new())),
                sales: PLMutex::new(Some(Vec::new())),
                rotations: PLMutex::new(Some(Vec::new())),
                yourshop: PLMutex::new(Some(YourShopStatus::default())),
            })
        }

        fn offline() -> Arc<Self> {
            Arc::new(Self {
                catalog: PLMutex::new(None),
                sales: PLMutex::new(None),
                rotations: PLMutex::new(None),
                yourshop: PLMutex::new(None),
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
            Ok(vec![ChampionEntry { id: 103, name: Some("Ahri".to_owned()) }])
        }
    }

    const GUILD: u64 = 700;

    struct Fixture {
        storage: Arc<InMemoryStorage>,
        output: Arc<RecordingChatOutput>,
        bus: RecorderBus,
        lcu: Arc<FakeLcu>,
        engine: Arc<StoreEngine<RecorderBus>>,
    }

    async fn fixture(lcu: Arc<FakeLcu>) -> Fixture {
        let storage = Arc::new(InMemoryStorage::new());
        let output = RecordingChatOutput::new();
        let factory = RecordingChatOutputFactory::new(Arc::clone(&output)).boxed();
        let bus = RecorderBus::default();
        let port: Arc<dyn LcuPort> = lcu.clone();
        let engine = Arc::new(StoreEngine::new(
            port,
            Arc::clone(&storage) as Arc<dyn StoragePort>,
            factory,
            bus.clone(),
            EngineSettings { poll: Duration::from_secs(60), flags: AnnounceFlags::all_on() },
        ));
        Fixture { storage, output, bus, lcu, engine }
    }

    async fn enable_guild(storage: &InMemoryStorage, guild: u64, channel: Option<&str>) {
        let scoped = storage.guild_scoped(Platform::Discord, GuildId(guild));
        scoped
            .set(
                NAMESPACE,
                CONFIG_KEY,
                serde_json::json!({ "enabled": true, "channel_id": channel }),
            )
            .await
            .expect("config write expected");
    }

    fn skin_item(item_id: u64, price: u64) -> CatalogItem {
        CatalogItem {
            item_id,
            inventory_type: Some("CHAMPION_SKIN".to_owned()),
            prices: vec![Price { cost: Some(price), currency: Some("RP".to_owned()) }],
            localizations: BTreeMap::from([(
                "en_US".to_owned(),
                LocalizedText { name: Some("Foxfire Ahri".to_owned()) },
            )]),
            item_requirements: vec![ItemRef {
                inventory_type: Some("CHAMPION".to_owned()),
                item_id: Some(103),
            }],
        }
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
    async fn first_poll_is_a_silent_baseline_but_persists_state() {
        let f = fixture(FakeLcu::online()).await;
        enable_guild(&f.storage, GUILD, Some("55")).await;
        *f.lcu.sales.lock() = Some(vec![skin_sale(1, 1031)]);
        *f.lcu.catalog.lock() = Some(vec![skin_item(1031, 975)]);

        f.engine.tick().await;

        assert!(f.output.messages().is_empty(), "baseline must stay silent");
        assert!(f.bus.log().is_empty());
        let scoped = f.storage.guild_scoped(Platform::Discord, GuildId(GUILD));
        let persisted = scoped.get(NAMESPACE, LAST_SEEN_KEY).await.expect("read expected");
        assert!(persisted.is_some(), "last_seen must be persisted for boot catch-up");
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

        let scoped = f.storage.guild_scoped(Platform::Discord, GuildId(GUILD));
        let records = scoped.list_last(NAMESPACE, 10).await.expect("records read expected");
        assert_eq!(records.len(), 1, "the announcement must be recorded");
        assert_eq!(f.bus.log(), vec![("lol_store.announced", GUILD, "sales")]);
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

        // Boot 2 over a copy of the same storage: the rotation swapped while
        // the bot was "down".
        let storage2 = Arc::new(InMemoryStorage::new());
        {
            let from = f.storage.guild_scoped(Platform::Discord, GuildId(GUILD));
            let to = storage2.guild_scoped(Platform::Discord, GuildId(GUILD));
            for key in [CONFIG_KEY, LAST_SEEN_KEY] {
                if let Some(value) = from.get(NAMESPACE, key).await.expect("read expected") {
                    to.set(NAMESPACE, key, value).await.expect("copy expected");
                }
            }
        }
        let bus2 = RecorderBus::default();
        let lcu2 = FakeLcu::online();
        *lcu2.rotations.lock() = Some(vec![rotation_store("WEEKLY_V1", &["b"])]);
        let factory2 = RecordingChatOutputFactory::new(RecordingChatOutput::new()).boxed();
        let engine2 = Arc::new(StoreEngine::new(
            lcu2,
            Arc::clone(&storage2) as Arc<dyn StoragePort>,
            factory2,
            bus2.clone(),
            EngineSettings { poll: Duration::from_secs(60), flags: AnnounceFlags::all_on() },
        ));
        engine2.tick().await;

        assert!(
            bus2.log().iter().any(|(_, _, kind)| *kind == "mythic_rotation"),
            "catch-up must announce the rotation change"
        );
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
            factory2,
            f.bus.clone(),
            EngineSettings { poll: Duration::from_secs(60), flags: AnnounceFlags::all_on() },
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
        assert!(first.contains("2026-10-01 09:00 UTC"));
        assert!(first.contains("ends 2026-10-08 09:00 UTC"));

        f.engine.tick().await;
        assert_eq!(f.output.messages().len(), 1, "still active: silent");
    }
}
