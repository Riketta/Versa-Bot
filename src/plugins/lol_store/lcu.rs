//! The League client boundary: this plugin's private hexagon. The engine
//! depends only on [`LcuPort`]; [`LcuClient`] is the sole adapter (HTTP to
//! the locally running League client). Data is store-catalog-shaped, not
//! chat-shaped, and never crosses into the kernel.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use thiserror::Error;

/// Failure modes of the local League client link. `Offline` is the expected
/// steady state whenever the game client is closed - callers log it and
/// skip, never alert.
#[derive(Debug, Error)]
pub enum LcuError {
    /// The client is not running (lockfile absent/unreadable) or unreachable.
    #[error("league client is not reachable: {0}")]
    Offline(String),
    /// The client rejected the lockfile credentials (client restarting).
    #[error("league client rejected the credentials: {0}")]
    Auth(String),
    /// A request failed at the transport level after the retry.
    #[error("LCU request failed: {0}")]
    Transport(String),
    /// The client answered but not with the expected JSON shape.
    #[error("unexpected LCU response: {0}")]
    Parse(String),
}

/// One store price entry (`cost` in `currency`, RP for everything tracked).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Price {
    #[serde(default)]
    pub cost: Option<u64>,
    #[serde(default)]
    pub currency: Option<String>,
}

/// A pointer to a store item (`inventoryType` + numeric `itemId`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ItemRef {
    #[serde(default)]
    pub inventory_type: Option<String>,
    #[serde(default)]
    pub item_id: Option<u64>,
}

/// Localized store text; the catalog carries one entry per client locale.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct LocalizedText {
    #[serde(default)]
    pub name: Option<String>,
}

/// One full-catalog entry (`GET /lol-store/v1/catalog`). Only the fields the
/// trackers consume are modeled; everything else is ignored, so Riot-side
/// additions stay harmless.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CatalogItem {
    #[serde(default)]
    pub item_id: u64,
    #[serde(default)]
    pub inventory_type: Option<String>,
    #[serde(default)]
    pub prices: Vec<Price>,
    #[serde(default)]
    pub localizations: std::collections::BTreeMap<String, LocalizedText>,
    #[serde(default)]
    pub item_requirements: Vec<ItemRef>,
}

/// One active sale (`GET /lol-store/v1/catalog/sales`). The payload's
/// `discount` field is dead (always `0.0` in practice) - the percentage is
/// computed against the catalog's original price instead.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Sale {
    #[serde(default)]
    pub id: u64,
    #[serde(default)]
    pub item: ItemRef,
    #[serde(default)]
    pub sale: SaleInfo,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SaleInfo {
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub prices: Vec<Price>,
}

/// Shoppefront metadata nested inside a store's `displayMetadata`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct ShoppefrontMeta {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub categories: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DisplayMetadata {
    #[serde(default)]
    pub shoppefront: Option<ShoppefrontMeta>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RotatingMetadata {
    #[serde(default)]
    pub rotation_cadence: Option<String>,
    #[serde(default)]
    pub curr_rotation_start_time: Option<String>,
    #[serde(default)]
    pub next_rotation_start_time: Option<String>,
}

/// One structured payment option of a store entry; the Mythic Essence price
/// lives in the payment named `lol_mythic_essence`.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Payment {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub final_delta: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PaymentOption {
    #[serde(default)]
    pub payments: Vec<Payment>,
}

/// What a purchased store entry grants; its `name` is the display name
/// (e.g. `Prestige Ocean Song Seraphine`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Fulfillment {
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PurchaseUnit {
    #[serde(default)]
    pub fulfillment: Option<Fulfillment>,
    #[serde(default)]
    pub payment_options: Vec<PaymentOption>,
}

/// One catalog entry of a Shoppefront store (one rotation slot).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoreEntry {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
    #[serde(default)]
    pub purchase_units: Vec<PurchaseUnit>,
}

/// One Shoppefront store (`GET /lol-shoppefront/v1/stores`). Rotation stores
/// (Mythic Shop) carry `rotatingStoreMetadata` with a cadence.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RotationStore {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub display_metadata: Option<DisplayMetadata>,
    #[serde(default)]
    pub rotating_store_metadata: Option<RotatingMetadata>,
    #[serde(default)]
    pub catalog_entries: Vec<StoreEntry>,
}

impl RotationStore {
    /// The Shoppefront id (`MYTHIC_SHOP`, ...), when present.
    #[must_use]
    pub fn shoppefront_id(&self) -> Option<&str> {
        Some(self.display_metadata.as_ref()?.shoppefront.as_ref()?.id.as_deref()?)
    }

    /// First display category (`WEEKLY`, `DAILY`, ...), lowercased and
    /// length-clamped - it is interpolated into announcement titles.
    #[must_use]
    pub fn category_label(&self) -> Option<String> {
        let categories = &self.display_metadata.as_ref()?.shoppefront.as_ref()?.categories;
        categories.first().map(|category| category.to_lowercase().chars().take(32).collect())
    }

    /// True when the store is a rotating one (has a rotation cadence).
    #[must_use]
    pub fn is_rotating(&self) -> bool {
        self.rotating_store_metadata.as_ref().is_some_and(|meta| meta.rotation_cadence.is_some())
    }
}

/// Your Shop event state (`GET /lol-yourshop/v1/status`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct YourShopStatus {
    #[serde(default)]
    pub hub_enabled: Option<bool>,
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
}

/// One entry of the champion summary game data - the id-to-name table for
/// champion display names.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ChampionEntry {
    #[serde(default)]
    pub id: u64,
    #[serde(default)]
    pub name: Option<String>,
}

/// Driven port: the running League client's store-facing read API. All
/// methods are snapshots; the implementer owns credentials and retries.
#[async_trait]
pub trait LcuPort: Send + Sync + 'static {
    /// Full store catalog (all inventory types, ~9.5k entries).
    async fn catalog(&self) -> Result<Vec<CatalogItem>, LcuError>;

    /// Currently active sales.
    async fn sales(&self) -> Result<Vec<Sale>, LcuError>;

    /// Shoppefront stores (rotation stores among them).
    async fn rotations(&self) -> Result<Vec<RotationStore>, LcuError>;

    /// Your Shop event state.
    async fn yourshop_status(&self) -> Result<YourShopStatus, LcuError>;

    /// Champion id-to-name table (game data, not store data).
    async fn champion_names(&self) -> Result<Vec<ChampionEntry>, LcuError>;
}

/// Lockfile credentials: `name:pid:port:token:protocol`. The token rotates on
/// every client start, so this is re-read per request cycle - the client
/// self-heals across game restarts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LcuCredentials {
    pub port: u16,
    pub token: String,
    pub protocol: String,
}

/// Parses a lockfile's contents. Returns `None` on any shape deviation.
#[must_use]
pub fn parse_lockfile(contents: &str) -> Option<LcuCredentials> {
    let mut fields = contents.trim().split(':');
    let _name = fields.next()?;
    let _pid = fields.next()?;
    let port = fields.next()?.parse::<u16>().ok()?;
    let token = fields.next()?;
    let protocol = fields.next()?.to_owned();
    if fields.next().is_some() || token.is_empty() || protocol.is_empty() {
        return None;
    }
    Some(LcuCredentials { port, token: token.to_owned(), protocol })
}

/// The raw shape of one LCU HTTP answer, stripped of reqwest: what the
/// retry ladder in [`LcuClient::get_json`] reasons about. `body` is `Err`
/// only for a 2xx whose body failed to decode; non-2xx bodies are not read
/// at all.
#[derive(Debug)]
pub(crate) struct RawAnswer {
    pub(crate) status: u16,
    pub(crate) body: Result<serde_json::Value, String>,
}

/// The HTTP seam under the retry ladder: one GET with the token, nothing
/// else. The real impl wraps the shared reqwest client; tests script
/// [`RawAnswer`]s to pin the self-heal behavior.
#[async_trait]
pub(crate) trait Transport: Send + Sync {
    async fn get(&self, url: &str, token: &str) -> Result<RawAnswer, String>;
}

/// Real transport over the client-wide reqwest handle (self-signed cert
/// accepted - inherent to the LCU).
struct ReqwestTransport {
    http: reqwest::Client,
}

#[async_trait]
impl Transport for ReqwestTransport {
    async fn get(&self, url: &str, token: &str) -> Result<RawAnswer, String> {
        let response = self
            .http
            .get(url)
            .basic_auth("riot", Some(token.to_owned()))
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status().as_u16();
        if !response.status().is_success() {
            return Ok(RawAnswer { status, body: Err(format!("HTTP {status}")) });
        }
        let body = response.json::<serde_json::Value>().await.map_err(|err| err.to_string());
        Ok(RawAnswer { status, body })
    }
}

/// The real [`LcuPort`] adapter: HTTPS to `{protocol}://{address}:{port}` with
/// lockfile basic auth and the client's self-signed certificate accepted.
///
/// Credentials are re-read from the lockfile for every request - a file read
/// is effectively free next to an HTTP round trip, and the strategy makes the
/// link self-healing across client restarts with no invalidation logic. A
/// rejected token or a refused connection triggers one immediate re-read +
/// retry (the client rewrites the lockfile on start; a poll can land mid-
/// restart), after which the failure is reported.
pub struct LcuClient {
    transport: Box<dyn Transport>,
    lockfile_path: PathBuf,
    address: String,
}

impl LcuClient {
    /// Builds the adapter. `address` is host-only - the port always comes
    /// from the lockfile. `lockfile_path` empty means "never configured";
    /// requests then fail with [`LcuError::Offline`].
    ///
    /// # Errors
    /// Only when the HTTP client cannot be built (TLS backend failure).
    pub fn new(
        lockfile_path: impl Into<PathBuf>,
        address: impl Into<String>,
    ) -> reqwest::Result<Self> {
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Self {
            transport: Box::new(ReqwestTransport { http }),
            lockfile_path: lockfile_path.into(),
            address: address.into(),
        })
    }

    /// Test constructor: script the transport, keep the real ladder.
    #[cfg(test)]
    fn with_transport(
        transport: Box<dyn Transport>,
        lockfile_path: PathBuf,
        address: String,
    ) -> Self {
        Self { transport, lockfile_path, address }
    }

    async fn read_credentials(&self) -> Result<LcuCredentials, LcuError> {
        if self.lockfile_path.as_os_str().is_empty() {
            return Err(LcuError::Offline("lockfile path is not configured".to_owned()));
        }
        let contents = tokio::fs::read_to_string(&self.lockfile_path).await.map_err(|err| {
            LcuError::Offline(format!("{} unreadable: {err}", self.lockfile_path.display()))
        })?;
        parse_lockfile(&contents).ok_or_else(|| {
            LcuError::Offline(format!("{} has an unexpected shape", self.lockfile_path.display()))
        })
    }

    /// One GET with the lockfile credentials; one immediate credential
    /// re-read + retry on 401 or transport failure (client mid-restart). A
    /// re-read that lands while the lockfile is momentarily absent (the file
    /// rewrite races the poll) gets a short beat and one more read before
    /// the cycle gives up - an instant bail here would skip whole polls. A
    /// re-read failing a second time reports the FIRST error - the original
    /// failure is the diagnosis, the follow-up is its echo (the second is
    /// logged at debug).
    async fn get_json(&self, path: &str) -> Result<serde_json::Value, LcuError> {
        for attempt in 0..2 {
            let creds = match self.read_credentials().await {
                Ok(creds) => creds,
                Err(err) if attempt == 0 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    self.read_credentials().await.map_err(|second| {
                        tracing::debug!(second = %second, "credential re-read also failed");
                        err
                    })?
                }
                Err(err) => return Err(err),
            };
            let url =
                format!("{}://{}:{port}{path}", creds.protocol, self.address, port = creds.port);
            let answer = self.transport.get(&url, &creds.token).await;

            match answer {
                Ok(answer) if answer.status == 401 => {
                    if attempt == 0 {
                        continue;
                    }
                    return Err(LcuError::Auth("rejected after credential re-read".to_owned()));
                }
                Ok(answer) if !(200..300).contains(&answer.status) => {
                    return Err(LcuError::Transport(format!("{path} answered {}", answer.status)));
                }
                Ok(answer) => {
                    return answer.body.map_err(|err| LcuError::Parse(format!("{path}: {err}")));
                }
                Err(err) => {
                    if attempt == 0 {
                        continue;
                    }
                    return Err(LcuError::Offline(format!("{path}: {err}")));
                }
            }
        }
        // The loop always returns within its two iterations.
        Err(LcuError::Transport("request loop exhausted".to_owned()))
    }

    async fn get_json_typed<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
    ) -> Result<T, LcuError> {
        let value = self.get_json(path).await?;
        serde_json::from_value(value).map_err(|err| LcuError::Parse(format!("{path}: {err}")))
    }
}

#[async_trait]
impl LcuPort for LcuClient {
    async fn catalog(&self) -> Result<Vec<CatalogItem>, LcuError> {
        self.get_json_typed("/lol-store/v1/catalog").await
    }

    async fn sales(&self) -> Result<Vec<Sale>, LcuError> {
        self.get_json_typed("/lol-store/v1/catalog/sales").await
    }

    async fn rotations(&self) -> Result<Vec<RotationStore>, LcuError> {
        self.get_json_typed("/lol-shoppefront/v1/stores").await
    }

    async fn yourshop_status(&self) -> Result<YourShopStatus, LcuError> {
        self.get_json_typed("/lol-yourshop/v1/status").await
    }

    async fn champion_names(&self) -> Result<Vec<ChampionEntry>, LcuError> {
        self.get_json_typed("/lol-game-data/assets/v1/champion-summary.json").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lockfile_parses_all_five_fields() {
        let creds = parse_lockfile("LeagueClient:49044:38436:SyntheticTestTokenABC123xyz:https")
            .expect("valid lockfile expected to parse");
        assert_eq!(creds.port, 38436);
        assert_eq!(creds.token, "SyntheticTestTokenABC123xyz");
        assert_eq!(creds.protocol, "https");
    }

    #[test]
    fn lockfile_rejects_malformed_shapes() {
        assert!(parse_lockfile("").is_none());
        assert!(parse_lockfile("LeagueClient:123:abc:token:https").is_none());
        assert!(parse_lockfile("LeagueClient:123:1234:token").is_none());
        assert!(parse_lockfile("a:1:2:3:4:5").is_none());
        assert!(parse_lockfile("LeagueClient:1:2::https").is_none());
    }

    #[test]
    fn sale_dto_parses_live_shape() {
        let sale: Sale = serde_json::from_str(
            r#"{"active": true, "id": 75092,
                "item": {"inventoryType": "CHAMPION_SKIN", "itemId": 1031},
                "sale": {"endDate": "2026-10-05T17:00:00.000+00:00",
                         "prices": [{"cost": 607, "currency": "RP", "discount": 0.0}],
                         "startDate": "2026-09-28T17:00:00.000+00:00"}}"#,
        )
        .expect("sale shape expected to parse");
        assert_eq!(sale.id, 75092);
        assert_eq!(sale.item.item_id, Some(1031));
        assert_eq!(sale.sale.prices.first().and_then(|p| p.cost), Some(607));
        assert_eq!(sale.sale.end_date.as_deref(), Some("2026-10-05T17:00:00.000+00:00"));
    }

    #[test]
    fn catalog_item_dto_parses_live_shape() {
        let item: CatalogItem = serde_json::from_str(
            r#"{"active": true, "inventoryType": "CHAMPION_SKIN", "itemId": 10002,
                "localizations": {"en_US": {"name": "Viridian Kayle"}},
                "itemRequirements": [{"inventoryType": "CHAMPION", "itemId": 10}],
                "prices": [{"cost": 520, "currency": "RP", "discount": 0.0}]}"#,
        )
        .expect("catalog item shape expected to parse");
        assert_eq!(item.item_id, 10002);
        assert_eq!(item.inventory_type.as_deref(), Some("CHAMPION_SKIN"));
        assert_eq!(
            item.localizations.get("en_US").and_then(|l| l.name.clone()).as_deref(),
            Some("Viridian Kayle")
        );
        assert_eq!(item.item_requirements.first().and_then(|r| r.item_id), Some(10));
    }

    #[test]
    fn rotation_store_dto_parses_live_shape() {
        let store: RotationStore = serde_json::from_str(
            r#"{"name": "MYTHIC_SHOPPE_WEEKLY_ROTATION_V6",
                "displayMetadata": {"shoppefront": {"id": "MYTHIC_SHOP", "categories": ["WEEKLY"]}},
                "rotatingStoreMetadata": {"rotationCadence": "PT168H",
                                          "currRotationStartTime": "2026-10-01T00:00:00.000Z",
                                          "nextRotationStartTime": "2026-10-08T00:00:00.000Z"},
                "catalogEntries": [{
                    "id": "e477de71-757f-4da3-b6fa-fc4cecf3afbc",
                    "name": "Script generated price - ME: 35",
                    "purchaseUnits": [{
                        "fulfillment": {"name": "K/DA ALL OUT Akali (BADDEST)"},
                        "paymentOptions": [{"payments": [{
                            "name": "lol_mythic_essence", "finalDelta": 35}]}]}]}]}"#,
        )
        .expect("rotation store shape expected to parse");
        assert_eq!(store.shoppefront_id(), Some("MYTHIC_SHOP"));
        assert_eq!(store.category_label().as_deref(), Some("weekly"));
        assert!(store.is_rotating());
        let entry = store.catalog_entries.first().expect("entry expected");
        let unit = entry.purchase_units.first().expect("unit expected");
        assert_eq!(
            unit.fulfillment.as_ref().and_then(|f| f.name.clone()).as_deref(),
            Some("K/DA ALL OUT Akali (BADDEST)")
        );
        let price = unit
            .payment_options
            .iter()
            .flat_map(|option| option.payments.iter())
            .find(|payment| payment.name.as_deref() == Some("lol_mythic_essence"))
            .and_then(|payment| payment.final_delta);
        assert_eq!(price, Some(35));
    }

    #[test]
    fn yourshop_status_dto_parses_live_shape() {
        let status: YourShopStatus = serde_json::from_str(
            r#"{"endTime": "2030-01-01T09:00:00Z", "hubEnabled": false,
                "name": "YS Deactivated", "startTime": "2020-01-01T09:00:00Z"}"#,
        )
        .expect("yourshop status shape expected to parse");
        assert_eq!(status.hub_enabled, Some(false));
        assert_eq!(status.start_time.as_deref(), Some("2020-01-01T09:00:00Z"));
    }

    // --- the credential retry ladder, pinned against a scripted transport

    use std::path::Path;
    use std::sync::Arc;

    use parking_lot::Mutex;

    /// Scripts answers in call order and records every (url, token) pair.
    /// `rewrite` models the client finishing its restart: written to disk
    /// before the first request lands.
    struct ScriptedTransport {
        answers: Mutex<Vec<Result<RawAnswer, String>>>,
        seen: Mutex<Vec<(String, String)>>,
        rewrite: Option<(PathBuf, String)>,
    }

    impl ScriptedTransport {
        fn ok(status: u16, body: serde_json::Value) -> Result<RawAnswer, String> {
            Ok(RawAnswer { status, body: Ok(body) })
        }

        fn status(status: u16) -> Result<RawAnswer, String> {
            Ok(RawAnswer { status, body: Err(format!("HTTP {status}")) })
        }
    }

    #[async_trait]
    impl Transport for ScriptedTransport {
        async fn get(&self, url: &str, token: &str) -> Result<RawAnswer, String> {
            self.seen.lock().push((url.to_owned(), token.to_owned()));
            if let Some((path, contents)) = &self.rewrite {
                std::fs::write(path, contents).expect("lockfile rewrite expected to succeed");
            }
            self.answers.lock().remove(0)
        }
    }

    /// Arc wrapper: the test keeps the fake to inspect `seen` while the
    /// client owns it behind the box.
    #[async_trait]
    impl Transport for Arc<ScriptedTransport> {
        async fn get(&self, url: &str, token: &str) -> Result<RawAnswer, String> {
            (**self).get(url, token).await
        }
    }

    fn lockfile_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("versa-lcu-{tag}-{}.txt", std::process::id()))
    }

    fn write_lockfile(path: &Path, token: &str) {
        std::fs::write(path, format!("LeagueClient:1:12345:{token}:https"))
            .expect("lockfile write expected to succeed");
    }

    fn client_at(transport: Arc<ScriptedTransport>, lockfile: PathBuf) -> LcuClient {
        LcuClient::with_transport(Box::new(transport), lockfile, "127.0.0.1".to_owned())
    }

    /// The self-heal happy path: a 401 against the stale token is followed
    /// by a credential re-read (the fake rewrites the lockfile mid-retry,
    /// modeling the client finishing its restart) and a successful retry.
    #[tokio::test]
    async fn unauthorized_retries_with_reread_credentials() {
        let lockfile = lockfile_path("rotate");
        write_lockfile(&lockfile, "stale-token");
        let transport = Arc::new(ScriptedTransport {
            answers: Mutex::new(vec![
                ScriptedTransport::status(401),
                ScriptedTransport::ok(200, serde_json::json!({ "ok": true })),
            ]),
            seen: Mutex::new(Vec::new()),
            rewrite: Some((lockfile.clone(), "LeagueClient:1:12345:fresh-token:https".to_owned())),
        });
        let client = client_at(Arc::clone(&transport), lockfile);

        // The typed port methods deserialize; the ladder is asserted raw.
        let value =
            client.get_json("/lol-store/v1/catalog").await.expect("retry expected to succeed");
        assert_eq!(value, serde_json::json!({ "ok": true }));

        let seen = transport.seen.lock();
        assert_eq!(seen.len(), 2, "one 401, one retry: {seen:?}");
        let (first_url, first_token) = seen.first().expect("first call expected");
        assert_eq!(first_token, "stale-token");
        assert_eq!(
            first_url, "https://127.0.0.1:12345/lol-store/v1/catalog",
            "url joins lockfile protocol+port, config address, request path"
        );
        assert_eq!(seen.get(1).map(|(_, token)| token.as_str()), Some("fresh-token"));
    }

    /// A refused connection (client restarting) retries once against the
    /// same credentials and succeeds.
    #[tokio::test]
    async fn transport_failure_retries_once() {
        let lockfile = lockfile_path("transport");
        write_lockfile(&lockfile, "token");
        let transport = Arc::new(ScriptedTransport {
            answers: Mutex::new(vec![
                Err("connection refused".to_owned()),
                ScriptedTransport::ok(200, serde_json::json!([])),
            ]),
            seen: Mutex::new(Vec::new()),
            rewrite: None,
        });
        let client = client_at(transport, lockfile);

        client.sales().await.expect("retry expected to succeed");
    }

    /// A lockfile momentarily absent (the rewrite race) gets the 500 ms
    /// beat and one more read before the cycle gives up.
    #[tokio::test]
    async fn missing_lockfile_waits_and_recovers() {
        let lockfile = lockfile_path("race");
        let _ = std::fs::remove_file(&lockfile);
        let spawner = lockfile.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            write_lockfile(&spawner, "late-token");
        });
        let transport = Arc::new(ScriptedTransport {
            answers: Mutex::new(vec![ScriptedTransport::ok(200, serde_json::json!([]))]),
            seen: Mutex::new(Vec::new()),
            rewrite: None,
        });
        let client = client_at(transport, lockfile);

        client.rotations().await.expect("recovery expected to succeed");
    }

    /// A re-read failing a second time surfaces the FIRST failure - the
    /// original diagnosis, not its echo.
    #[tokio::test]
    async fn double_creds_failure_reports_the_first_error() {
        let lockfile = lockfile_path("double");
        let _ = std::fs::remove_file(&lockfile);
        let transport = Arc::new(ScriptedTransport {
            answers: Mutex::new(vec![ScriptedTransport::ok(200, serde_json::json!({}))]),
            seen: Mutex::new(Vec::new()),
            rewrite: None,
        });
        let client = client_at(transport, lockfile.clone());

        let err = client.catalog().await.expect_err("no lockfile expected to fail");
        assert!(matches!(err, LcuError::Offline(_)), "{err}");
        assert!(err.to_string().contains("unreadable"), "first error expected: {err}");
    }

    /// Rejected after the retry is an auth failure, and exactly two calls
    /// were made.
    #[tokio::test]
    async fn unauthorized_twice_is_auth_failure() {
        let lockfile = lockfile_path("auth");
        write_lockfile(&lockfile, "token");
        let transport = Arc::new(ScriptedTransport {
            answers: Mutex::new(vec![
                ScriptedTransport::status(401),
                ScriptedTransport::status(401),
            ]),
            seen: Mutex::new(Vec::new()),
            rewrite: None,
        });
        let client = client_at(Arc::clone(&transport), lockfile);

        let err = client.catalog().await.expect_err("401 twice expected to fail");
        assert!(matches!(err, LcuError::Auth(_)), "{err}");
        assert_eq!(transport.seen.lock().len(), 2);
    }

    /// An HTTP error status is a transport error and does NOT retry - the
    /// ladder is for restart races, not server bugs.
    #[tokio::test]
    async fn http_error_status_fails_without_retry() {
        let lockfile = lockfile_path("http500");
        write_lockfile(&lockfile, "token");
        let transport = Arc::new(ScriptedTransport {
            answers: Mutex::new(vec![ScriptedTransport::status(500)]),
            seen: Mutex::new(Vec::new()),
            rewrite: None,
        });
        let client = client_at(Arc::clone(&transport), lockfile);

        let err = client.catalog().await.expect_err("500 expected to fail");
        assert!(matches!(err, LcuError::Transport(_)), "{err}");
        assert!(err.to_string().contains("500"), "{err}");
        assert_eq!(transport.seen.lock().len(), 1);
    }

    /// A 2xx body that fails to decode is a parse error, distinct from
    /// transport/auth failures.
    #[tokio::test]
    async fn undecodable_body_is_parse_error() {
        let lockfile = lockfile_path("parse");
        write_lockfile(&lockfile, "token");
        let transport = Arc::new(ScriptedTransport {
            answers: Mutex::new(vec![Ok(RawAnswer {
                status: 200,
                body: Err("expected value".to_owned()),
            })]),
            seen: Mutex::new(Vec::new()),
            rewrite: None,
        });
        let client = client_at(transport, lockfile);

        let err = client.catalog().await.expect_err("bad body expected to fail");
        assert!(matches!(err, LcuError::Parse(_)), "{err}");
    }
}
