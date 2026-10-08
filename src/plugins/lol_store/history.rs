//! Store history: per-UTC-day digests of the observed deal set, persisted
//! to the plugin-global document store as `history.<YYYY-MM-DD>` documents
//! (plugin namespace). Each document holds that day's FULL observed deal
//! state - not a delta - with display names joined at capture time, so a
//! digest stays self-contained even after items leave the store catalog
//! later. `/lol_store_history` groups the days into per-deal windows: a
//! window merges across days the bot could not observe (no document, or
//! the section absent from the digest) and closes on the first recorded
//! day that observed the section without the deal.
//!
//! Your Shop is deliberately absent: its offers are the logged-in
//! operator's personal shop, not guild-shareable store state. Days the bot
//! could not observe (client offline) simply have no document - history is
//! "as observed".

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::diff::{Snapshot, mythic_entry, rotation_stores};
use super::format::{NameIndex, localized_name, percent_off};
use super::lcu::Price;
use super::watch::WatchTarget;

/// Storage-key prefix of the day documents (plugin namespace); the suffix
/// is the UTC `YYYY-MM-DD` day.
pub(crate) const HISTORY_PREFIX: &str = "history.";

/// Schema version of the day documents - a bump signals a future shape
/// migration; no reader migrates today.
pub(crate) const DIGEST_SCHEMA: u32 = 1;

/// One skin sale as observed on a day, names and prices frozen at capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SaleRecord {
    /// Riot's sale id - the identity a multi-day window groups on.
    pub(crate) sale_id: u64,
    pub(crate) item_id: u64,
    /// Champion behind the skin at capture time (`None`: the catalog join
    /// failed) - the handle champion-wide history queries match on.
    pub(crate) champion_id: Option<u64>,
    /// "Champion — Skin" as rendered at capture; the synthetic "Skin N"
    /// when the join failed.
    pub(crate) label: String,
    pub(crate) sale_price: Option<u64>,
    pub(crate) original_price: Option<u64>,
    /// Raw payload dates (full ISO text, not the display cut).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) end: Option<String>,
}

/// One Mythic Shop slot as observed on a day. A slot whose entry id joins
/// the catalog carries its item/champion ids (matchable); a name-only slot
/// carries neither and can never be matched by a query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MythicRecord {
    /// The rotation store's name (`DAILY`, `WEEKLY`, ...).
    pub(crate) rotation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) item_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) champion_id: Option<u64>,
    pub(crate) label: String,
    pub(crate) mythic_price: Option<u64>,
}

/// One UTC day's observed deal set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DayDigest {
    pub(crate) schema: u32,
    pub(crate) day: String,
    #[serde(default)]
    pub(crate) sales: Vec<SaleRecord>,
    #[serde(default)]
    pub(crate) mythic: Vec<MythicRecord>,
}

/// One grouped history row: a deal observed on one or more recorded days,
/// collapsed into a window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoryRow {
    /// `"sale"` or `"mythic"` - the reply groups on it.
    pub(crate) kind: &'static str,
    /// Kind-specific grouping identity (sale id; rotation + item id, or
    /// rotation + captured label for name-only slots).
    identity: String,
    pub(crate) label: String,
    /// Rendered price/detail tail (percent, RP cost; rotation, ME price).
    pub(crate) detail: String,
    pub(crate) first_day: String,
    pub(crate) last_day: String,
    /// A recorded day observed the section without this deal - the window
    /// ended there; a later sighting opens a fresh row.
    closed: bool,
}

/// Today's UTC date as the day documents' `YYYY-MM-DD` key. UTC on purpose:
/// day boundaries must not move with the host timezone.
pub(crate) fn day_key(unix_secs: u64) -> String {
    day_key_before(unix_secs, 0)
}

/// The UTC date `days` before `unix_secs`, same format - the retention
/// cutoff (documents older than it are pruned). Saturating on purpose: an
/// absurd retention (a config typo) floors at the lowest representable
/// date instead of panicking the poll job, which reads as "prune
/// everything".
pub(crate) fn day_key_before(unix_secs: u64, days: u64) -> String {
    let Ok(at) = OffsetDateTime::from_unix_timestamp(i64::try_from(unix_secs).unwrap_or(0)) else {
        return "1970-01-01".to_owned();
    };
    // 100M days outspan every representable `time` date many times over,
    // so the subtract saturates via `checked_sub` long before the
    // `Duration` multiplication could overflow.
    let date = at
        .date()
        .checked_sub(time::Duration::days(i64::try_from(days.min(100_000_000)).unwrap_or(0)))
        .unwrap_or(time::Date::MIN);
    format!("{:04}-{:02}-{:02}", date.year(), u8::from(date.month()), date.day())
}

/// Freezes the observed deal state of one snapshot into a day document.
/// Mythic slots resolve exactly like watch matching: a catalog-verified
/// entry id becomes a matchable record; everything else stays name-only.
pub(crate) fn capture(day: &str, snapshot: &Snapshot, index: &NameIndex) -> DayDigest {
    let mut digest = DayDigest {
        schema: DIGEST_SCHEMA,
        day: day.to_owned(),
        sales: Vec::new(),
        mythic: Vec::new(),
    };

    for sale in snapshot.sales.iter().flatten() {
        let Some(item_id) = sale.item.item_id else { continue };
        let (champion_id, label) = describe_item(item_id, index);
        digest.sales.push(SaleRecord {
            sale_id: sale.id,
            item_id,
            champion_id,
            label,
            sale_price: rp_price(&sale.sale.prices),
            original_price: index.original_price(item_id),
            start: sale.sale.start_date.clone(),
            end: sale.sale.end_date.clone(),
        });
    }

    for store in rotation_stores(snapshot.rotations.as_deref().unwrap_or(&[])) {
        let Some(rotation) = store.name.clone() else { continue };
        for entry in &store.catalog_entries {
            let entry = mythic_entry(entry);
            let (item_id, champion_id, label) = match entry
                .entry_id
                .as_deref()
                .and_then(|id| id.parse::<u64>().ok())
            {
                Some(id) if index.catalog.contains_key(&id) => {
                    let (champion_id, label) = describe_item(id, index);
                    (Some(id), champion_id, label)
                }
                _ => (None, None, entry.name.clone().unwrap_or_else(|| "Unknown item".to_owned())),
            };
            digest.mythic.push(MythicRecord {
                rotation: rotation.clone(),
                item_id,
                champion_id,
                label,
                mythic_price: entry.mythic_price,
            });
        }
    }

    digest
}

/// Champion id + display label of a catalog item, the capture-time joins.
fn describe_item(item_id: u64, index: &NameIndex) -> (Option<u64>, String) {
    match index.catalog.get(&item_id) {
        Some(item) => {
            let champion_id = index.champion_id_of_item(item_id);
            let label = match index.skin_champion(item) {
                Some(champion) => format!("{champion} \u{2014} {}", localized_name(item)),
                None => localized_name(item),
            };
            (champion_id, label)
        }
        None => (None, format!("Skin {item_id}")),
    }
}

fn rp_price(prices: &[Price]) -> Option<u64> {
    prices.iter().find(|price| price.currency.as_deref() == Some("RP")).and_then(|price| price.cost)
}

/// Does one recorded deal (by its matchable ids) hit the query target?
/// Name-only mythic slots match nothing - the query side resolves through
/// the same catalog the capture side joined with.
fn matches(record_item: Option<u64>, record_champion: Option<u64>, target: &WatchTarget) -> bool {
    match target {
        WatchTarget::Skin { item_id, .. } => record_item == Some(*item_id),
        WatchTarget::Champion { champion_id, .. } => record_champion == Some(*champion_id),
    }
}

/// Folds one day's matching records into the row set (days arrive in
/// ascending order). A day that observed the relevant section without a
/// deal closes that deal's window - a later sighting opens a new one -
/// while days without any record of the section merge across. Matchable
/// mythic slots group by item id, so capture-time label drift cannot
/// split a window.
pub(crate) fn extend_rows(
    rows: &mut Vec<HistoryRow>,
    digest: &DayDigest,
    day: &str,
    target: &WatchTarget,
) {
    let mut sales_today: Vec<HistoryRow> = Vec::new();
    for record in &digest.sales {
        if !matches(Some(record.item_id), record.champion_id, target) {
            continue;
        }
        let detail = sale_detail(record);
        sales_today.push(HistoryRow {
            kind: "sale",
            identity: format!("sale:{}", record.sale_id),
            label: record.label.clone(),
            detail,
            first_day: day.to_owned(),
            last_day: day.to_owned(),
            closed: false,
        });
    }
    // A day with any recorded sale observed the sales list: every open
    // window absent from it is over. An empty list is indistinguishable
    // from "never observed" and merges across - the conservative reading.
    if !digest.sales.is_empty() {
        close_absent(rows, "sale", &sales_today);
    }
    for row in sales_today {
        upsert(rows, row);
    }

    let mut mythic_today: Vec<HistoryRow> = Vec::new();
    for record in &digest.mythic {
        if !matches(record.item_id, record.champion_id, target) {
            continue;
        }
        let detail = match record.mythic_price {
            Some(price) => format!("{} \u{b7} {price} ME", record.rotation),
            None => record.rotation.clone(),
        };
        mythic_today.push(HistoryRow {
            kind: "mythic",
            identity: mythic_identity(record),
            label: record.label.clone(),
            detail,
            first_day: day.to_owned(),
            last_day: day.to_owned(),
            closed: false,
        });
    }
    // Per rotation: a day recording any slot of that rotation observed it.
    // Rotations absent from the digest were simply not recorded.
    let observed: BTreeSet<&str> =
        digest.mythic.iter().map(|record| record.rotation.as_str()).collect();
    if !observed.is_empty() {
        close_mythic_absent(rows, &observed, &mythic_today);
    }
    for row in mythic_today {
        upsert(rows, row);
    }
}

/// Closes every open window of `kind` that today's observed records do not
/// contain (by identity).
fn close_absent(rows: &mut Vec<HistoryRow>, kind: &str, today: &[HistoryRow]) {
    for row in rows.iter_mut().filter(|row| row.kind == kind && !row.closed) {
        if !today.iter().any(|fresh| fresh.identity == row.identity) {
            row.closed = true;
        }
    }
}

/// Mythic variant of [`close_absent`]: only windows whose rotation was
/// observed today can close. The rotation is the identity's second
/// segment (`mythic:{rotation}:...`); rotation names never carry colons.
fn close_mythic_absent(
    rows: &mut Vec<HistoryRow>,
    observed_rotations: &BTreeSet<&str>,
    today: &[HistoryRow],
) {
    for row in rows.iter_mut().filter(|row| row.kind == "mythic" && !row.closed) {
        let Some(rotation) = row.identity.split(':').nth(1) else { continue };
        if observed_rotations.contains(rotation)
            && !today.iter().any(|fresh| fresh.identity == row.identity)
        {
            row.closed = true;
        }
    }
}

/// Matchable slots group by item id (stable across capture-time label
/// drift); name-only slots fall back to their captured label.
fn mythic_identity(record: &MythicRecord) -> String {
    match record.item_id {
        Some(item_id) => format!("mythic:{}:id:{item_id}", record.rotation),
        None => format!("mythic:{}:label:{}", record.rotation, record.label),
    }
}

/// Extends the open window with this identity to `row.last_day`, or opens
/// a new one. A closed window with the same identity stays closed - the
/// new row is a separate presence.
fn upsert(rows: &mut Vec<HistoryRow>, row: HistoryRow) {
    if let Some(existing) =
        rows.iter_mut().find(|existing| existing.identity == row.identity && !existing.closed)
    {
        existing.last_day = row.last_day;
    } else {
        rows.push(row);
    }
}

/// Sales before mythic, most recent first, stable within ties.
pub(crate) fn finalize_rows(mut rows: Vec<HistoryRow>) -> Vec<HistoryRow> {
    rows.sort_by(|a, b| {
        a.kind
            .cmp(b.kind)
            .then(b.last_day.cmp(&a.last_day))
            .then(b.first_day.cmp(&a.first_day))
            .then(a.label.cmp(&b.label))
    });
    rows
}

fn sale_detail(record: &SaleRecord) -> String {
    let mut detail = String::new();
    if let (Some(original), Some(cost)) = (record.original_price, record.sale_price) {
        if let Some(percent) = percent_off(original, cost) {
            detail.push_str(&format!("\u{2212}{percent}%"));
        }
    }
    if let Some(cost) = record.sale_price {
        if !detail.is_empty() {
            detail.push_str(" \u{b7} ");
        }
        detail.push_str(&format!("{cost} RP"));
    }
    detail
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::diff::Snapshot;
    use super::super::format::{NameIndex, champion_map};
    use super::super::lcu::{
        CatalogItem, ItemRef, LocalizedText, RotationStore, Sale, SaleInfo, StoreEntry,
    };
    use super::*;

    fn index() -> NameIndex {
        NameIndex::new(
            vec![CatalogItem {
                item_id: 1031,
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                prices: vec![Price { cost: Some(975), currency: Some("RP".to_owned()) }],
                localizations: BTreeMap::from([(
                    "en_US".to_owned(),
                    LocalizedText { name: Some("Foxfire Ahri".to_owned()) },
                )]),
                item_requirements: vec![ItemRef {
                    inventory_type: Some("CHAMPION".to_owned()),
                    item_id: Some(103),
                }],
            }],
            champion_map([(103, Some("Ahri".to_owned()))]),
        )
    }

    fn sale(id: u64, item_id: u64, cost: u64) -> Sale {
        Sale {
            id,
            item: ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(item_id),
            },
            sale: SaleInfo {
                start_date: Some("2026-09-01T00:00:00.000+00:00".to_owned()),
                end_date: Some("2026-09-05T00:00:00.000+00:00".to_owned()),
                prices: vec![Price { cost: Some(cost), currency: Some("RP".to_owned()) }],
            },
        }
    }

    fn rotation_entry(id: &str, name: &str) -> StoreEntry {
        StoreEntry { id: Some(id.to_owned()), name: Some(name.to_owned()), ..StoreEntry::default() }
    }

    /// A MYTHIC_SHOP rotation store - the only family [`capture`] tracks.
    fn rotation_store(name: &str, entries: Vec<StoreEntry>) -> RotationStore {
        use super::super::diff::MYTHIC_SHOP_ID;
        use super::super::lcu::{DisplayMetadata, RotatingMetadata, ShoppefrontMeta};
        RotationStore {
            name: Some(name.to_owned()),
            display_metadata: Some(DisplayMetadata {
                shoppefront: Some(ShoppefrontMeta {
                    id: Some(MYTHIC_SHOP_ID.to_owned()),
                    categories: vec![name.to_owned()],
                }),
            }),
            rotating_store_metadata: Some(RotatingMetadata {
                rotation_cadence: Some("PT24H".to_owned()),
                curr_rotation_start_time: Some("2026-10-07T00:00:00.000Z".to_owned()),
                next_rotation_start_time: Some("2026-10-08T00:00:00.000Z".to_owned()),
            }),
            catalog_entries: entries,
        }
    }

    fn snapshot_with(sales: Vec<Sale>, rotations: Vec<RotationStore>) -> Snapshot {
        Snapshot { sales: Some(sales), catalog: None, rotations: Some(rotations), yourshop: None }
    }

    #[test]
    fn day_keys_are_utc_and_cutoff_shifts() {
        // 2026-10-07T01:10:52Z
        let unix = 1_791_335_452u64;
        assert_eq!(day_key(unix), "2026-10-07");
        assert_eq!(day_key_before(unix, 90), "2026-07-09");
        assert_eq!(day_key_before(unix, 0), "2026-10-07");
        assert_eq!(day_key(0), "1970-01-01");
    }

    #[test]
    fn capture_joins_names_prices_and_ids() {
        let digest = capture(
            "2026-10-07",
            &snapshot_with(
                vec![sale(75092, 1031, 607)],
                vec![rotation_store("DAILY", vec![rotation_entry("4001", "Blood Moon Ahri")])],
            ),
            &index(),
        );
        assert_eq!(digest.schema, DIGEST_SCHEMA);
        assert_eq!(digest.day, "2026-10-07");
        let [record] = digest.sales.as_slice() else { panic!("one sale expected") };
        assert_eq!(record.sale_id, 75092);
        assert_eq!(record.item_id, 1031);
        assert_eq!(record.champion_id, Some(103));
        assert_eq!(record.label, "Ahri \u{2014} Foxfire Ahri");
        assert_eq!(record.sale_price, Some(607));
        assert_eq!(record.original_price, Some(975));
        let [slot] = digest.mythic.as_slice() else { panic!("one mythic slot expected") };
        assert_eq!(slot.rotation, "DAILY");
        // The entry id joins nothing in this catalog - name-only record.
        assert_eq!(slot.item_id, None);
        assert_eq!(slot.label, "Blood Moon Ahri");
    }

    #[test]
    fn unknown_items_degrade_to_synthetic_labels() {
        let digest =
            capture("2026-10-07", &snapshot_with(vec![sale(1, 999_999, 100)], vec![]), &index());
        let [record] = digest.sales.as_slice() else { panic!("one sale expected") };
        assert_eq!(record.champion_id, None);
        assert_eq!(record.label, "Skin 999999");
        assert_eq!(record.original_price, None);
    }

    fn digest_with_sales(sales: Vec<SaleRecord>) -> DayDigest {
        DayDigest { schema: DIGEST_SCHEMA, day: String::new(), sales, mythic: Vec::new() }
    }

    fn skin_target(item_id: u64) -> WatchTarget {
        WatchTarget::Skin { item_id, champion: "Ahri".to_owned(), skin: "Foxfire Ahri".to_owned() }
    }

    fn champion_target(champion_id: u64) -> WatchTarget {
        WatchTarget::Champion { champion_id, champion: "Ahri".to_owned() }
    }

    fn sale_record(sale_id: u64, item_id: u64, champion_id: Option<u64>) -> SaleRecord {
        SaleRecord {
            sale_id,
            item_id,
            champion_id,
            label: "Ahri \u{2014} Foxfire Ahri".to_owned(),
            sale_price: Some(607),
            original_price: Some(975),
            start: None,
            end: None,
        }
    }

    #[test]
    fn rows_group_one_sale_across_days_and_split_distinct_sales() {
        let target = skin_target(1031);
        let mut rows = Vec::new();
        for day in ["2026-09-01", "2026-09-02", "2026-09-03"] {
            extend_rows(
                &mut rows,
                &digest_with_sales(vec![sale_record(75092, 1031, Some(103))]),
                day,
                &target,
            );
        }
        extend_rows(
            &mut rows,
            &digest_with_sales(vec![sale_record(75100, 1031, Some(103))]),
            "2026-09-05",
            &target,
        );
        let rows = finalize_rows(rows);
        assert_eq!(rows.len(), 2, "one window per sale id");
        let (recent, older) = (rows.first().expect("recent row"), rows.get(1).expect("older row"));
        assert_eq!(recent.first_day, "2026-09-05", "most recent first");
        assert_eq!(older.first_day, "2026-09-01");
        assert_eq!(older.last_day, "2026-09-03");
    }

    #[test]
    fn champion_target_matches_every_skin_of_the_champion() {
        let target = champion_target(103);
        let mut rows = Vec::new();
        extend_rows(
            &mut rows,
            &digest_with_sales(vec![sale_record(1, 1031, Some(103)), sale_record(2, 1032, None)]),
            "2026-09-01",
            &target,
        );
        let rows = finalize_rows(rows);
        assert_eq!(rows.len(), 1, "only records carrying the champion id match");
    }

    #[test]
    fn name_only_mythic_slots_match_nothing() {
        let digest = DayDigest {
            schema: DIGEST_SCHEMA,
            day: String::new(),
            sales: Vec::new(),
            mythic: vec![MythicRecord {
                rotation: "DAILY".to_owned(),
                item_id: None,
                champion_id: None,
                label: "Blood Moon Ahri".to_owned(),
                mythic_price: Some(100),
            }],
        };
        let mut rows = Vec::new();
        extend_rows(&mut rows, &digest, "2026-09-01", &skin_target(1031));
        extend_rows(&mut rows, &digest, "2026-09-01", &champion_target(103));
        assert!(rows.is_empty(), "a name-only slot has no matchable ids");
    }

    #[test]
    fn capture_joins_matchable_mythic_slots_by_entry_id() {
        let digest = capture(
            "2026-10-07",
            &snapshot_with(
                vec![],
                vec![rotation_store("DAILY", vec![rotation_entry("1031", "Blood Moon Ahri")])],
            ),
            &index(),
        );
        let [slot] = digest.mythic.as_slice() else { panic!("one mythic slot expected") };
        assert_eq!(slot.rotation, "DAILY");
        assert_eq!(slot.item_id, Some(1031), "the entry id joins the catalog");
        assert_eq!(slot.champion_id, Some(103));
        assert_eq!(slot.label, "Ahri \u{2014} Foxfire Ahri", "the label is the joined name");
        assert_eq!(slot.mythic_price, None);
    }

    #[test]
    fn observed_absence_closes_a_sale_window_and_a_later_sighting_reopens() {
        let target = skin_target(1031);
        let mut rows = Vec::new();
        extend_rows(
            &mut rows,
            &digest_with_sales(vec![sale_record(7, 1031, Some(103))]),
            "2026-09-01",
            &target,
        );
        // The sales list was observed today - without sale 7. Window over.
        extend_rows(
            &mut rows,
            &digest_with_sales(vec![sale_record(8, 1032, Some(103))]),
            "2026-09-02",
            &target,
        );
        extend_rows(
            &mut rows,
            &digest_with_sales(vec![sale_record(7, 1031, Some(103))]),
            "2026-09-03",
            &target,
        );
        let rows = finalize_rows(rows);
        assert_eq!(rows.len(), 2, "the re-sighting is a separate presence");
        let (recent, older) = (rows.first().expect("recent row"), rows.get(1).expect("older row"));
        assert_eq!(
            (recent.first_day.as_str(), recent.last_day.as_str()),
            ("2026-09-03", "2026-09-03")
        );
        assert_eq!(
            (older.first_day.as_str(), older.last_day.as_str()),
            ("2026-09-01", "2026-09-01")
        );
    }

    #[test]
    fn unobserved_days_merge_but_observed_absence_splits_mythic_windows() {
        let target = skin_target(1031);
        let slot = |item_id: u64| MythicRecord {
            rotation: "DAILY".to_owned(),
            item_id: Some(item_id),
            champion_id: Some(103),
            label: "Ahri \u{2014} Foxfire Ahri".to_owned(),
            mythic_price: Some(100),
        };
        let day_with = |slots: Vec<MythicRecord>| DayDigest {
            schema: DIGEST_SCHEMA,
            day: String::new(),
            sales: Vec::new(),
            mythic: slots,
        };
        let mut rows = Vec::new();
        extend_rows(&mut rows, &day_with(vec![slot(1031)]), "2026-09-01", &target);
        // The rotation was observed, but the slot was not in it: closed.
        extend_rows(&mut rows, &day_with(vec![slot(1032)]), "2026-09-02", &target);
        extend_rows(&mut rows, &day_with(vec![slot(1031)]), "2026-09-03", &target);
        // No mythic records at all - unobserved, merges across (nothing
        // left open to close anyway; the window that re-opened stays).
        extend_rows(&mut rows, &day_with(vec![]), "2026-09-04", &target);
        extend_rows(&mut rows, &day_with(vec![slot(1031)]), "2026-09-05", &target);
        let rows = finalize_rows(rows);
        assert_eq!(rows.len(), 2, "absence split, unobserved gap merged");
        let (recent, older) = (rows.first().expect("recent row"), rows.get(1).expect("older row"));
        assert_eq!(
            (recent.first_day.as_str(), recent.last_day.as_str()),
            ("2026-09-03", "2026-09-05")
        );
        assert_eq!(
            (older.first_day.as_str(), older.last_day.as_str()),
            ("2026-09-01", "2026-09-01")
        );
    }

    #[test]
    fn absurd_retention_saturates_instead_of_panicking() {
        // 2026-10-07T01:10:52Z; `u64::MAX` days must floor at the lowest
        // representable date, not panic the poll job. (The pinned `time`
        // build has no `large-dates`, so the floor is year -9999.)
        assert_eq!(day_key_before(1_791_335_452, u64::MAX), "-9999-01-01");
    }
}
