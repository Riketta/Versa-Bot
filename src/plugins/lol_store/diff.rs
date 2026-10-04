//! Store watchers' state and change detection. Pure logic: no I/O, no
//! channels - [`LastSeen`] is the compacted "what the store looked like last
//! time" (persisted per guild for boot catch-up), [`StoreDelta`] is what a
//! diff announces.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::lcu::{CatalogItem, RotationStore, Sale, StoreEntry, YourShopStatus};

/// Shoppefront id of the Mythic Shop stores - the only rotation family
/// tracked today.
pub const MYTHIC_SHOP_ID: &str = "MYTHIC_SHOP";

/// Per-rotation-store state: the entry-id set identifies "the same rotation"
/// (Riot swaps entries wholesale on rotation).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RotationState {
    #[serde(default)]
    pub entry_ids: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_rotation: Option<String>,
}

/// Your Shop event state; `active: false` covers both "deactivated" and
/// "never seen" - the tracker only reacts to the `false -> true` edge.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct YourShopState {
    #[serde(default)]
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
}

/// The compacted last-seen store state. `BTree*` everywhere so the persisted
/// JSON is stable and comparisons are order-insensitive.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastSeen {
    #[serde(default)]
    pub sales: BTreeSet<u64>,
    #[serde(default)]
    pub skins: BTreeSet<u64>,
    #[serde(default)]
    pub rotations: BTreeMap<String, RotationState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub yourshop: Option<YourShopState>,
}

/// One rotation's worth of newly listed Mythic Shop entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationDelta {
    /// Human label (`weekly`, `biweekly`, `daily`).
    pub label: String,
    pub rotation_start: Option<String>,
    pub next_rotation: Option<String>,
    pub entries: Vec<MythicEntry>,
}

/// One Mythic Shop slot: display name + Mythic Essence price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MythicEntry {
    pub name: Option<String>,
    pub mythic_price: Option<u64>,
}

/// Your Shop `false -> true` edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YourShopStart {
    pub start: Option<String>,
    pub end: Option<String>,
}

/// Everything a poll cycle found new, before formatting. Sources that failed
/// to fetch simply contribute nothing this cycle (they retry next tick).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreDelta {
    pub sales: Vec<Sale>,
    pub skins: Vec<CatalogItem>,
    pub rotations: Vec<RotationDelta>,
    pub yourshop: Option<YourShopStart>,
}

impl StoreDelta {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sales.is_empty()
            && self.skins.is_empty()
            && self.rotations.is_empty()
            && self.yourshop.is_none()
    }
}

/// The store data of one successful poll cycle (any part may be absent when
/// that source failed).
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub sales: Option<Vec<Sale>>,
    pub catalog: Option<Vec<CatalogItem>>,
    pub rotations: Option<Vec<RotationStore>>,
    pub yourshop: Option<YourShopStatus>,
}

/// Extracts the tracked rotation stores: `MYTHIC_SHOP` stores with a
/// rotation cadence (excludes the non-rotating Featured shelves).
#[must_use]
pub fn rotation_stores(stores: &[RotationStore]) -> Vec<&RotationStore> {
    stores
        .iter()
        .filter(|store| store.is_rotating() && store.shoppefront_id() == Some(MYTHIC_SHOP_ID))
        .collect()
}

/// Builds the last-seen state from a snapshot, overwriting only the sources
/// that actually arrived (failed sources keep their previous state).
#[must_use]
pub fn merge(previous: &LastSeen, snapshot: &Snapshot) -> LastSeen {
    let mut merged = previous.clone();

    if let Some(sales) = &snapshot.sales {
        merged.sales = sales.iter().map(|sale| sale.id).collect();
    }
    if let Some(catalog) = &snapshot.catalog {
        let skins: BTreeSet<u64> = catalog
            .iter()
            .filter(|item| item.inventory_type.as_deref() == Some("CHAMPION_SKIN"))
            .map(|item| item.item_id)
            .collect();
        // Collapse guard: an Ok payload that empties a non-empty skin set is
        // almost certainly a glitch (e.g. a mid-login store cold start), not
        // the store losing every skin at once - adopting it would mass
        // false-announce the whole catalog next poll. Keep the previous set.
        if !previous.skins.is_empty() && skins.is_empty() {
            tracing::warn!(
                kept = previous.skins.len(),
                "store catalog answered Ok but listed no skins - keeping the previous set"
            );
        } else {
            merged.skins = skins;
        }
    }
    if let Some(stores) = &snapshot.rotations {
        // Per-store merge over the previous map: a store absent from the
        // payload simply ends its rotation, but a present store whose entry
        // list collapsed to nothing gets the same suspicion as the skins -
        // its previous set is kept.
        let mut rotations = previous.rotations.clone();
        for store in rotation_stores(stores) {
            let Some(name) = store.name.clone() else { continue };
            let Some(meta) = store.rotating_store_metadata.as_ref() else { continue };
            let entry_ids: BTreeSet<String> = store
                .catalog_entries
                .iter()
                .map(|entry| entry.id.clone().unwrap_or_default())
                .collect();
            if entry_ids.is_empty()
                && previous.rotations.get(&name).is_some_and(|state| !state.entry_ids.is_empty())
            {
                tracing::warn!(
                    rotation = %name,
                    "rotation answered Ok but empty - keeping the previous entry set"
                );
                continue;
            }
            rotations.insert(
                name,
                RotationState {
                    entry_ids,
                    rotation_start: meta.curr_rotation_start_time.clone(),
                    next_rotation: meta.next_rotation_start_time.clone(),
                },
            );
        }
        // Stores absent from the payload end their rotations (the original
        // whole-map replacement semantics) - the collapse keep above only
        // applies to stores that are present.
        let present: BTreeSet<String> =
            rotation_stores(stores).iter().filter_map(|store| store.name.clone()).collect();
        rotations.retain(|name, _| present.contains(name));
        merged.rotations = rotations;
    }
    if let Some(status) = &snapshot.yourshop {
        merged.yourshop = Some(YourShopState {
            active: status.hub_enabled.unwrap_or(false),
            start: status.start_time.clone(),
            end: status.end_time.clone(),
        });
    }

    merged
}

/// Diffs the fetched snapshot against the last-seen state. Every section is
/// raw data - naming and pricing join happens at formatting time.
#[must_use]
pub fn compute(previous: &LastSeen, snapshot: &Snapshot) -> StoreDelta {
    let mut delta = StoreDelta::default();

    if let Some(sales) = &snapshot.sales {
        delta.sales =
            sales.iter().filter(|sale| !previous.sales.contains(&sale.id)).cloned().collect();
    }
    if let Some(catalog) = &snapshot.catalog {
        delta.skins = catalog
            .iter()
            .filter(|item| {
                item.inventory_type.as_deref() == Some("CHAMPION_SKIN")
                    && !previous.skins.contains(&item.item_id)
            })
            .cloned()
            .collect();
    }
    if let Some(stores) = &snapshot.rotations {
        for store in rotation_stores(stores) {
            let Some(name) = &store.name else { continue };
            let previous_state = previous.rotations.get(name);
            let changed = previous_state.is_none_or(|state| {
                state.entry_ids
                    != store
                        .catalog_entries
                        .iter()
                        .map(|entry| entry.id.clone().unwrap_or_default())
                        .collect::<BTreeSet<_>>()
            });
            if !changed {
                continue;
            }
            delta.rotations.push(RotationDelta {
                label: store.category_label().unwrap_or_else(|| "rotation".to_owned()),
                rotation_start: store
                    .rotating_store_metadata
                    .as_ref()
                    .and_then(|meta| meta.curr_rotation_start_time.clone()),
                next_rotation: store
                    .rotating_store_metadata
                    .as_ref()
                    .and_then(|meta| meta.next_rotation_start_time.clone()),
                entries: store.catalog_entries.iter().map(mythic_entry).collect(),
            });
        }
    }
    if let Some(status) = &snapshot.yourshop {
        let active = status.hub_enabled.unwrap_or(false);
        let was_active = previous.yourshop.as_ref().is_some_and(|state| state.active);
        if active && !was_active {
            delta.yourshop = Some(YourShopStart {
                start: status.start_time.clone(),
                end: status.end_time.clone(),
            });
        }
    }

    delta
}

/// Projects a store entry onto its display name and Mythic Essence price.
fn mythic_entry(entry: &StoreEntry) -> MythicEntry {
    let name = entry
        .purchase_units
        .first()
        .and_then(|unit| unit.fulfillment.as_ref())
        .and_then(|fulfillment| fulfillment.name.clone())
        .or_else(|| entry.name.clone());
    let mythic_price = entry
        .purchase_units
        .iter()
        .flat_map(|unit| unit.payment_options.iter())
        .flat_map(|option| option.payments.iter())
        .find(|payment| payment.name.as_deref() == Some("lol_mythic_essence"))
        .and_then(|payment| payment.final_delta);
    MythicEntry { name, mythic_price }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::lol_store::lcu::ItemRef;
    use crate::plugins::lol_store::lcu::{DisplayMetadata, RotatingMetadata, ShoppefrontMeta};

    fn sale(id: u64) -> Sale {
        Sale {
            id,
            item: ItemRef { inventory_type: Some("CHAMPION_SKIN".to_owned()), item_id: Some(id) },
            sale: Default::default(),
        }
    }

    fn catalog_skin(item_id: u64) -> CatalogItem {
        CatalogItem {
            item_id,
            inventory_type: Some("CHAMPION_SKIN".to_owned()),
            ..Default::default()
        }
    }

    fn rotation_store(name: &str, entry_ids: &[&str]) -> RotationStore {
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
            catalog_entries: entry_ids
                .iter()
                .map(|id| StoreEntry { id: Some((*id).to_owned()), ..Default::default() })
                .collect(),
        }
    }

    #[test]
    fn merge_overwrites_only_arrived_sources() {
        let previous = LastSeen {
            sales: BTreeSet::from([1]),
            skins: BTreeSet::from([100]),
            rotations: BTreeMap::new(),
            yourshop: Some(YourShopState { active: true, start: None, end: None }),
        };
        let merged =
            merge(&previous, &Snapshot { sales: Some(vec![sale(2)]), ..Default::default() });
        // Sales refreshed, everything else kept.
        assert_eq!(merged.sales, BTreeSet::from([2]));
        assert_eq!(merged.skins, BTreeSet::from([100]));
        assert_eq!(merged.yourshop.as_ref().map(|state| state.active), Some(true));
    }

    #[test]
    fn compute_reports_only_new_ids() {
        let previous = LastSeen {
            sales: BTreeSet::from([1]),
            skins: BTreeSet::from([100]),
            ..Default::default()
        };
        let snapshot = Snapshot {
            sales: Some(vec![sale(1), sale(2)]),
            catalog: Some(vec![catalog_skin(100), catalog_skin(200)]),
            ..Default::default()
        };
        let delta = compute(&previous, &snapshot);
        assert_eq!(delta.sales.iter().map(|sale| sale.id).collect::<Vec<_>>(), vec![2]);
        assert_eq!(delta.skins.iter().map(|item| item.item_id).collect::<Vec<_>>(), vec![200]);
        assert!(delta.rotations.is_empty());
        assert!(delta.yourshop.is_none());
    }

    #[test]
    fn compute_detects_rotation_entry_swap() {
        let previous = LastSeen {
            rotations: BTreeMap::from([(
                "WEEKLY_ROTATION".to_owned(),
                RotationState {
                    entry_ids: BTreeSet::from(["a".to_owned()]),
                    rotation_start: None,
                    next_rotation: None,
                },
            )]),
            ..Default::default()
        };
        let snapshot = Snapshot {
            rotations: Some(vec![rotation_store("WEEKLY_ROTATION", &["b", "c"])]),
            ..Default::default()
        };
        let delta = compute(&previous, &snapshot);
        assert_eq!(delta.rotations.len(), 1);
        let rotation = delta.rotations.first().expect("rotation expected");
        assert_eq!(rotation.label, "weekly");
        assert_eq!(rotation.entries.len(), 2);

        // Unchanged rotation is silent.
        let again = compute(&merge(&previous, &snapshot), &snapshot);
        assert!(again.rotations.is_empty());
    }

    #[test]
    fn compute_announces_only_the_yourshop_start_edge() {
        let inactive = LastSeen {
            yourshop: Some(YourShopState { active: false, ..Default::default() }),
            ..Default::default()
        };
        let active_status = YourShopStatus {
            hub_enabled: Some(true),
            start_time: Some("2026-10-01T09:00:00Z".to_owned()),
            end_time: Some("2026-10-08T09:00:00Z".to_owned()),
        };
        let delta = compute(
            &inactive,
            &Snapshot { yourshop: Some(active_status.clone()), ..Default::default() },
        );
        assert_eq!(
            delta.yourshop.as_ref().map(|start| start.end.clone()),
            Some(Some("2026-10-08T09:00:00Z".to_owned()))
        );

        // Still active next tick: silent.
        let current = merge(
            &inactive,
            &Snapshot { yourshop: Some(active_status.clone()), ..Default::default() },
        );
        assert!(
            compute(&current, &Snapshot { yourshop: Some(active_status), ..Default::default() })
                .yourshop
                .is_none()
        );
    }

    #[test]
    fn merge_keeps_the_skin_set_when_an_ok_catalog_collapses() {
        let previous = LastSeen { skins: BTreeSet::from([100, 200]), ..Default::default() };
        // Ok-but-empty catalog: a mid-login glitch, not the store losing
        // every skin. The previous set must survive.
        let merged =
            merge(&previous, &Snapshot { catalog: Some(Vec::new()), ..Default::default() });
        assert_eq!(merged.skins, BTreeSet::from([100, 200]));

        // A genuinely empty store from the start (no previous) adopts empty.
        let merged = merge(
            &LastSeen::default(),
            &Snapshot { catalog: Some(Vec::new()), ..Default::default() },
        );
        assert!(merged.skins.is_empty());

        // A shrink that stays non-empty adopts normally: skins legitimately
        // rotate out of the catalog.
        let merged = merge(
            &previous,
            &Snapshot { catalog: Some(vec![catalog_skin(999)]), ..Default::default() },
        );
        assert_eq!(merged.skins, BTreeSet::from([999]));
    }

    /// Only rotating MYTHIC_SHOP stores are tracked: a non-rotating shelf
    /// or a different shoppefront family must never enter the state (and
    /// thus never announce).
    #[test]
    fn non_rotating_and_non_mythic_stores_are_excluded() {
        let mut featured = rotation_store("FEATURED_SHELF", &["a"]);
        featured.rotating_store_metadata = None; // not rotating at all
        let mut other_family = rotation_store("OTHER_FAMILY", &["b"]);
        if let Some(meta) = other_family.display_metadata.as_mut() {
            if let Some(shoppefront) = meta.shoppefront.as_mut() {
                shoppefront.id = Some("ARAM_SHOP".to_owned());
            }
        }
        let merged = merge(
            &LastSeen::default(),
            &Snapshot { rotations: Some(vec![featured, other_family]), ..Default::default() },
        );
        assert!(merged.rotations.is_empty(), "neither store is a tracked rotation");

        // The real thing still lands.
        let merged = merge(
            &LastSeen::default(),
            &Snapshot {
                rotations: Some(vec![rotation_store("WEEKLY_ROTATION", &["a"])]),
                ..Default::default()
            },
        );
        assert_eq!(merged.rotations.len(), 1);
    }

    #[test]
    fn merge_keeps_a_rotation_whose_entries_collapse_but_drops_gone_stores() {
        let previous = LastSeen {
            rotations: BTreeMap::from([
                (
                    "WEEKLY_ROTATION".to_owned(),
                    RotationState {
                        entry_ids: BTreeSet::from(["a".to_owned(), "b".to_owned()]),
                        rotation_start: None,
                        next_rotation: None,
                    },
                ),
                (
                    "DAILY_ROTATION".to_owned(),
                    RotationState {
                        entry_ids: BTreeSet::from(["x".to_owned()]),
                        rotation_start: None,
                        next_rotation: None,
                    },
                ),
            ]),
            ..Default::default()
        };
        // WEEKLY answers Ok with an empty entry list (suspect collapse:
        // kept), DAILY vanishes from the payload (rotation ended: dropped).
        let snapshot = Snapshot {
            rotations: Some(vec![rotation_store("WEEKLY_ROTATION", &[])]),
            ..Default::default()
        };
        let merged = merge(&previous, &snapshot);
        let weekly = merged.rotations.get("WEEKLY_ROTATION").expect("kept rotation");
        assert_eq!(weekly.entry_ids, BTreeSet::from(["a".to_owned(), "b".to_owned()]));
        assert!(merged.rotations.get("DAILY_ROTATION").is_none());
    }

    #[test]
    fn last_seen_survives_a_serde_round_trip() {
        let state = LastSeen {
            sales: BTreeSet::from([7, 8]),
            skins: BTreeSet::from([900]),
            rotations: BTreeMap::from([(
                "DAILY".to_owned(),
                RotationState {
                    entry_ids: BTreeSet::from(["e".to_owned()]),
                    rotation_start: Some("t".to_owned()),
                    next_rotation: None,
                },
            )]),
            yourshop: Some(YourShopState { active: false, ..Default::default() }),
        };
        let json = serde_json::to_string(&state).expect("serialize expected");
        let back: LastSeen = serde_json::from_str(&json).expect("deserialize expected");
        assert_eq!(back, state);
    }
}
