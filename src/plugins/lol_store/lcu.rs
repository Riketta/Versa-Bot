//! The League client boundary: this plugin's private hexagon. The engine
//! depends only on [`LcuPort`]; [`LcuClient`] is the sole adapter (HTTP to
//! the locally running League client). Data is store-catalog-shaped, not
//! chat-shaped, and never crosses into the kernel.

use std::path::{Path, PathBuf};
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

    /// First display category (`WEEKLY`, `DAILY`, ...), lowercased - the
    /// human label of a rotation store.
    #[must_use]
    pub fn category_label(&self) -> Option<String> {
        let categories = &self.display_metadata.as_ref()?.shoppefront.as_ref()?.categories;
        categories.first().map(|category| category.to_lowercase())
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
    http: reqwest::Client,
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
        Ok(Self { http, lockfile_path: lockfile_path.into(), address: address.into() })
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
    /// the cycle gives up - an instant bail here would skip whole polls.
    async fn get_json(&self, path: &str) -> Result<reqwest::Response, LcuError> {
        for attempt in 0..2 {
            let creds = match self.read_credentials().await {
                Ok(creds) => creds,
                Err(err) if attempt == 0 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    self.read_credentials().await.map_err(|_| err)?
                }
                Err(err) => return Err(err),
            };
            let url =
                format!("{}://{}:{port}{path}", creds.protocol, self.address, port = creds.port);
            let response =
                self.http.get(&url).basic_auth("riot", Some(creds.token.clone())).send().await;

            match response {
                Ok(resp) if resp.status() == reqwest::StatusCode::UNAUTHORIZED => {
                    if attempt == 0 {
                        continue;
                    }
                    return Err(LcuError::Auth("rejected after credential re-read".to_owned()));
                }
                Ok(resp) => {
                    if let Err(err) = resp.error_for_status_ref() {
                        let status = err.status().map_or("unknown".to_owned(), |s| s.to_string());
                        return Err(LcuError::Transport(format!("{path} answered {status}")));
                    }
                    return Ok(resp);
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
        let value = self
            .get_json(path)
            .await?
            .json::<T>()
            .await
            .map_err(|err| LcuError::Parse(format!("{path}: {err}")))?;
        Ok(value)
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
}
