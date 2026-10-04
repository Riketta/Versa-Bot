//! Delta rendering: joins raw store data against the catalog and champion
//! name tables to produce the announcement text. Every line degrades
//! gracefully - a missing join yields a synthetic name, never an error.

use std::collections::{BTreeMap, HashMap};

use super::diff::StoreDelta;
use super::lcu::CatalogItem;

/// Lines shown per section before the "+N more" cut.
const MAX_LINES_PER_SECTION: usize = 15;

/// Hard cap for the joined announcement. The embed-description limit is
/// 4096 chars and the send path truncates nothing - an oversized embed is
/// rejected and silently loses the update for every guild. Stay under it
/// with margin for the hidden-count footer.
const MAX_ANNOUNCEMENT_CHARS: usize = 3800;

/// Name/pricing joins for one announcement. The catalog doubles as the
/// original-price source for the sale percentage (the sale payload's own
/// `discount` field is dead - always `0.0`).
pub struct NameIndex {
    pub catalog: HashMap<u64, CatalogItem>,
    pub champions: HashMap<u64, String>,
}

impl NameIndex {
    #[must_use]
    pub fn new(catalog: Vec<CatalogItem>, champions: BTreeMap<u64, String>) -> Self {
        Self {
            catalog: catalog.into_iter().map(|item| (item.item_id, item)).collect(),
            champions: champions.into_iter().collect(),
        }
    }

    /// Empty index: everything degrades to synthetic names.
    #[cfg(test)]
    #[must_use]
    pub fn empty() -> Self {
        Self { catalog: HashMap::new(), champions: HashMap::new() }
    }

    fn champion_name(&self, champion_id: u64) -> String {
        self.champions
            .get(&champion_id)
            .cloned()
            .unwrap_or_else(|| format!("Champion {champion_id}"))
    }

    /// Champion behind a skin: the catalog's `itemRequirements` champion,
    /// or - for actual champion skins only - the skin-id convention
    /// (`championId * 1000 + n`) as fallback. Other item kinds (chests,
    /// orbs, bundles) get no champion prefix: their ids mean nothing by
    /// that convention.
    pub(crate) fn skin_champion(&self, item: &CatalogItem) -> Option<String> {
        let champion_id = self.champion_id(item)?;
        Some(self.champion_name(champion_id))
    }

    /// The champion id behind a catalog item (see [`NameIndex::skin_champion`]).
    fn champion_id(&self, item: &CatalogItem) -> Option<u64> {
        let from_requirements = item
            .item_requirements
            .iter()
            .find(|req| req.inventory_type.as_deref() == Some("CHAMPION"))
            .and_then(|req| req.item_id)
            .filter(|id| *id > 0);
        match from_requirements {
            Some(id) => Some(id),
            None => {
                if item.inventory_type.as_deref() != Some("CHAMPION_SKIN") {
                    return None;
                }
                let id = item.item_id / 1000;
                if id == 0 { None } else { Some(id) }
            }
        }
    }

    /// Champion id behind a store item id, via the catalog. Watch matching
    /// joins on this - base champions and non-skin items join to nothing.
    pub(crate) fn champion_id_of_item(&self, item_id: u64) -> Option<u64> {
        let item = self.catalog.get(&item_id)?;
        self.champion_id(item)
    }

    fn original_price(&self, item_id: u64) -> Option<u64> {
        self.catalog.get(&item_id).and_then(|item| rp_price(&item.prices))
    }
}

/// Renders the full announcement within the embed-description budget;
/// `None` when nothing renders (all sections empty or every line stripped).
#[must_use]
pub fn announce_text(delta: &StoreDelta, index: &NameIndex) -> Option<String> {
    let mut sections: Vec<String> = Vec::new();

    if !delta.sales.is_empty() {
        let lines: Vec<String> = delta.sales.iter().map(|sale| sale_line(sale, index)).collect();
        push_section(&mut sections, "New sales", &lines);
    }
    if !delta.skins.is_empty() {
        let lines: Vec<String> = delta.skins.iter().map(|item| skin_line(item, index)).collect();
        push_section(&mut sections, "New in store", &lines);
    }
    for rotation in &delta.rotations {
        let lines: Vec<String> = rotation.entries.iter().map(mythic_line).collect();
        if lines.is_empty() {
            continue;
        }
        let mut title = format!("Mythic rotation ({})", rotation.label);
        if let Some(ends) = &rotation.next_rotation {
            title.push_str(&format!(" \u{b7} ends {}", timestamp(ends)));
        }
        push_section(&mut sections, &title, &lines);
    }
    if let Some(start) = &delta.yourshop {
        let mut line = String::from("**Your Shop started**");
        if let Some(started) = start.start.as_deref() {
            line.push_str(&format!(" \u{b7} started {}", timestamp(started)));
        }
        if let Some(ends) = start.end.as_deref() {
            line.push_str(&format!(" \u{b7} ends {}", timestamp(ends)));
        }
        sections.push(line);
    }

    fit(sections)
}

/// Fits the built sections into the embed budget: whole sections are
/// dropped from the tail (lowest build priority) until the join fits, a
/// hidden-count footer records what was cut, and a single pathological
/// section (absurd item names) is hard-cut char-safely. The join budget
/// counts BYTES while the hard cut counts CHARS - deliberately
/// conservative: multibyte locales drop sections a little earlier than
/// strictly needed, but the char cut always stays under Discord's
/// code-point-based embed limit.
pub(crate) fn fit(mut sections: Vec<String>) -> Option<String> {
    let join_len = |sections: &[String]| {
        sections.iter().map(String::len).sum::<usize>() + sections.len().saturating_sub(1) * 2
    };
    let mut hidden = 0usize;
    while sections.len() > 1 && join_len(&sections) > MAX_ANNOUNCEMENT_CHARS {
        sections.pop();
        hidden += 1;
    }
    if join_len(&sections) > MAX_ANNOUNCEMENT_CHARS {
        if let Some(first) = sections.first_mut() {
            *first = format!(
                "{}\u{2026}",
                first.chars().take(MAX_ANNOUNCEMENT_CHARS).collect::<String>()
            );
        }
    }
    if sections.is_empty() {
        return None;
    }
    let mut text = sections.join("\n\n");
    if hidden > 0 {
        text.push_str(&format!("\n\n\u{2026}and {hidden} more update groups hidden"));
    }
    Some(text)
}

/// Builds the name index's champions map source: champion id -> name.
#[must_use]
pub fn champion_map(
    entries: impl IntoIterator<Item = (u64, Option<String>)>,
) -> BTreeMap<u64, String> {
    entries.into_iter().filter_map(|(id, name)| name.map(|name| (id, name))).collect()
}

pub(crate) fn push_section(sections: &mut Vec<String>, title: &str, lines: &[String]) {
    if lines.is_empty() {
        return;
    }
    let shown: Vec<&str> = lines.iter().take(MAX_LINES_PER_SECTION).map(String::as_str).collect();
    let mut body = shown.join("\n");
    let hidden = lines.len().saturating_sub(MAX_LINES_PER_SECTION);
    if hidden > 0 {
        body.push_str(&format!("\n…and {hidden} more"));
    }
    sections.push(format!("**{title}**\n{body}"));
}

/// Best display name of a catalog item (any localization wins over a
/// synthetic placeholder).
pub(crate) fn localized_name(item: &CatalogItem) -> String {
    item.localizations
        .values()
        .find_map(|text| text.name.clone())
        .unwrap_or_else(|| format!("Skin {}", item.item_id))
}

pub(crate) fn sale_line(sale: &super::lcu::Sale, index: &NameIndex) -> String {
    let item_id = sale.item.item_id.unwrap_or(0);
    let is_champion_sale = sale.item.inventory_type.as_deref() == Some("CHAMPION");

    let subject = if is_champion_sale {
        index.champion_name(item_id)
    } else {
        match index.catalog.get(&item_id) {
            Some(item) => match index.skin_champion(item) {
                Some(champion) => format!("{champion} — {}", localized_name(item)),
                None => localized_name(item),
            },
            None => format!("Skin {item_id}"),
        }
    };

    let mut line = format!("- {subject}");
    let sale_price = rp_price(&sale.sale.prices);
    // Percentage off, computed against the catalog's original price (the
    // payload's own `discount` field is dead - always 0.0).
    if let (Some(original), Some(cost)) = (index.original_price(item_id), sale_price) {
        if original > cost && original > 0 {
            // u128 intermediates: catalog prices are untrusted LCU data, and
            // `original * 200` would overflow u64 on corrupted values.
            let percent = (u128::from(original - cost) * 200 + u128::from(original))
                / (u128::from(original) * 2);
            line.push_str(&format!(" \u{2212}{percent}%"));
        }
    }
    if let Some(cost) = sale_price {
        line.push_str(&format!(" · {cost} RP"));
    }
    if let Some(ends) = sale.sale.end_date.as_deref() {
        line.push_str(&format!(" · until {}", date(ends)));
    }
    line
}

pub(crate) fn skin_line(item: &CatalogItem, index: &NameIndex) -> String {
    let subject = match index.skin_champion(item) {
        Some(champion) => format!("{champion} — {}", localized_name(item)),
        None => localized_name(item),
    };
    let mut line = format!("- {subject}");
    if let Some(price) = rp_price(&item.prices) {
        line.push_str(&format!(" · {price} RP"));
    }
    line
}

/// The RP cost of a price list - Riot payloads can carry other currencies
/// alongside RP, and announcements only ever quote RP.
fn rp_price(prices: &[super::lcu::Price]) -> Option<u64> {
    prices.iter().find(|price| price.currency.as_deref() == Some("RP")).and_then(|price| price.cost)
}

/// One Mythic Shop slot line - shared by announcements and watch pings.
pub(crate) fn mythic_line(entry: &super::diff::MythicEntry) -> String {
    match (entry.name.clone(), entry.mythic_price) {
        (Some(name), Some(price)) => format!("- {name} \u{b7} {price} ME"),
        (Some(name), None) => format!("- {name}"),
        (None, Some(price)) => format!("- Unknown item \u{b7} {price} ME"),
        (None, None) => "- Unknown item".to_owned(),
    }
}

/// `2026-10-05T17:00:00.000+00:00` -> `2026-10-05`.
fn date(iso: &str) -> String {
    iso.split('T').next().unwrap_or(iso).to_owned()
}

/// `2026-10-01T09:00:00Z` -> `2026-10-01 09:00 UTC`; a non-zero offset is
/// rendered as-is WITH its sign (dropping it would misread `-05:30` as
/// `+05:30`). Falls back to the raw string on any deviation.
#[must_use]
pub(crate) fn timestamp(iso: &str) -> String {
    let mut parts = iso.split('T');
    let day = parts.next().unwrap_or(iso);
    let Some(time) = parts.next() else { return iso.to_owned() };
    let hhmm: String = time.chars().take(5).collect();
    // Locate the sign char itself (split_once would strip it): everything
    // after it is the offset body.
    let offset = time
        .char_indices()
        .find(|(_, ch)| *ch == '+' || *ch == '-')
        .and_then(|(idx, sign)| {
            let rest = time.get(idx + 1..)?;
            (!rest.is_empty()).then(|| format!("{sign}{rest}"))
        })
        .unwrap_or_else(|| "Z".to_owned());
    let label = if offset == "Z" || offset == "+00:00" || offset == "-00:00" {
        "UTC"
    } else {
        offset.as_str()
    };
    format!("{day} {hhmm} {label}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::lol_store::lcu::{ItemRef, LocalizedText, Price, SaleInfo};

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

    fn skin_sale(cost: u64, end: Option<&str>) -> super::super::lcu::Sale {
        super::super::lcu::Sale {
            id: 1,
            item: ItemRef { inventory_type: Some("CHAMPION_SKIN".to_owned()), item_id: Some(1031) },
            sale: SaleInfo {
                start_date: None,
                end_date: end.map(str::to_owned),
                prices: vec![Price { cost: Some(cost), currency: Some("RP".to_owned()) }],
            },
        }
    }

    #[test]
    fn sale_line_joins_names_price_percent_and_date() {
        let line = sale_line(&skin_sale(607, Some("2026-10-05T17:00:00.000+00:00")), &index());
        assert_eq!(line, "- Ahri — Foxfire Ahri −38% · 607 RP · until 2026-10-05");
    }

    #[test]
    fn sale_line_skips_percent_when_not_discounted() {
        let line = sale_line(&skin_sale(975, None), &index());
        assert_eq!(line, "- Ahri — Foxfire Ahri · 975 RP");
    }

    #[test]
    fn champion_sale_uses_the_champion_map() {
        let sale = super::super::lcu::Sale {
            id: 2,
            item: ItemRef { inventory_type: Some("CHAMPION".to_owned()), item_id: Some(103) },
            sale: SaleInfo { start_date: None, end_date: None, prices: vec![] },
        };
        let line = sale_line(&sale, &index());
        assert_eq!(line, "- Ahri");
    }

    #[test]
    fn unknown_items_degrade_to_synthetic_names() {
        let sale = super::super::lcu::Sale {
            id: 3,
            item: ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(42_042),
            },
            sale: SaleInfo {
                start_date: None,
                end_date: None,
                prices: vec![Price { cost: Some(500), currency: Some("RP".to_owned()) }],
            },
        };
        let line = sale_line(&sale, &NameIndex::empty());
        assert_eq!(line, "- Skin 42042 · 500 RP");
    }

    #[test]
    fn announce_text_renders_all_sections() {
        let delta = StoreDelta {
            sales: vec![skin_sale(607, None)],
            skins: vec![CatalogItem {
                item_id: 10002,
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                prices: vec![Price { cost: Some(520), currency: Some("RP".to_owned()) }],
                localizations: BTreeMap::from([(
                    "en_US".to_owned(),
                    LocalizedText { name: Some("Viridian Kayle".to_owned()) },
                )]),
                item_requirements: vec![ItemRef {
                    inventory_type: Some("CHAMPION".to_owned()),
                    item_id: Some(10),
                }],
            }],
            rotations: vec![super::super::diff::RotationDelta {
                label: "weekly".to_owned(),
                rotation_start: None,
                next_rotation: None,
                entries: vec![super::super::diff::MythicEntry {
                    entry_id: None,
                    name: Some("Prestige Ocean Song Seraphine".to_owned()),
                    mythic_price: Some(35),
                }],
            }],
            yourshop: Some(super::super::diff::YourShopStart {
                start: Some("2026-10-01T09:00:00Z".to_owned()),
                end: Some("2026-10-08T09:00:00Z".to_owned()),
            }),
        };
        let mut index = index();
        index.champions.insert(10, "Kayle".to_owned());
        let text = announce_text(&delta, &index).expect("announcement expected");
        assert!(text.contains("**New sales**"));
        assert!(text.contains("- Ahri — Foxfire Ahri −38% · 607 RP"));
        assert!(text.contains("**New in store**"));
        assert!(text.contains("- Kayle — Viridian Kayle · 520 RP"));
        assert!(text.contains("**Mythic rotation (weekly)**"));
        assert!(text.contains("- Prestige Ocean Song Seraphine · 35 ME"));
        assert!(text.contains(
            "**Your Shop started** · started 2026-10-01 09:00 UTC · ends 2026-10-08 09:00 UTC"
        ));
    }

    #[test]
    fn empty_delta_renders_nothing() {
        assert!(announce_text(&StoreDelta::default(), &NameIndex::empty()).is_none());
    }

    #[test]
    fn sections_cut_at_fifteen_lines_with_a_more_marker() {
        let sales: Vec<super::super::lcu::Sale> = (0..20)
            .map(|id| {
                let mut sale = skin_sale(975, None);
                sale.id = id;
                sale.item.item_id = None;
                sale.item.inventory_type = Some("CHAMPION".to_owned());
                sale.sale.prices.clear();
                sale
            })
            .collect();
        let delta = StoreDelta { sales, ..Default::default() };
        let text = announce_text(&delta, &NameIndex::empty()).expect("announcement expected");
        // Title line + 15 shown lines + the marker = 16 newlines.
        assert_eq!(text.matches('\n').count(), 16);
        assert!(text.contains("…and 5 more"));
    }

    #[test]
    fn timestamp_trims_to_minutes() {
        assert_eq!(timestamp("2026-10-01T09:07:33.000Z"), "2026-10-01 09:07 UTC");
        assert_eq!(timestamp("weird"), "weird");
    }

    /// A non-zero offset is rendered as given, WITH its sign - dropping it
    /// would misread `-05:30` as `+05:30`.
    #[test]
    fn timestamp_keeps_nonzero_offsets() {
        assert_eq!(timestamp("2026-10-01T09:07:33.000+00:00"), "2026-10-01 09:07 UTC");
        assert_eq!(timestamp("2026-10-01T09:07:33.000+02:00"), "2026-10-01 09:07 +02:00");
        assert_eq!(timestamp("2026-10-01T09:07:33-05:30"), "2026-10-01 09:07 -05:30");
    }

    /// The rotation section title carries the next-rotation time when the
    /// payload provides it.
    #[test]
    fn rotation_section_title_shows_when_it_ends() {
        let delta = StoreDelta {
            rotations: vec![super::super::diff::RotationDelta {
                label: "weekly".to_owned(),
                rotation_start: None,
                next_rotation: Some("2026-10-08T00:00:00.000Z".to_owned()),
                entries: vec![super::super::diff::MythicEntry {
                    entry_id: None,
                    name: Some("Prestige Ocean Song Seraphine".to_owned()),
                    mythic_price: Some(35),
                }],
            }],
            ..StoreDelta::default()
        };
        let text = announce_text(&delta, &NameIndex::empty()).expect("announcement expected");
        assert!(
            text.contains("**Mythic rotation (weekly) · ends 2026-10-08 00:00 UTC**"),
            "text: {text}"
        );
    }

    /// A discounted non-skin item (chest, orb, bundle) gets no champion
    /// prefix: the `id / 1000` convention only means anything for skins.
    #[test]
    fn non_skin_sales_get_no_champion_prefix() {
        let mut catalog_index = index();
        catalog_index.catalog.insert(
            4_001,
            CatalogItem {
                item_id: 4_001,
                inventory_type: Some("CHEST".to_owned()),
                prices: vec![Price { cost: Some(300), currency: Some("RP".to_owned()) }],
                localizations: BTreeMap::from([(
                    "en_US".to_owned(),
                    LocalizedText { name: Some("Hextech Chest".to_owned()) },
                )]),
                item_requirements: Vec::new(),
            },
        );
        let sale = super::super::lcu::Sale {
            id: 9,
            item: ItemRef { inventory_type: Some("CHEST".to_owned()), item_id: Some(4_001) },
            sale: SaleInfo {
                start_date: None,
                end_date: None,
                prices: vec![Price { cost: Some(150), currency: Some("RP".to_owned()) }],
            },
        };
        let line = sale_line(&sale, &catalog_index);
        assert_eq!(line, "- Hextech Chest −50% · 150 RP");
    }

    /// Corrupted catalog prices must not overflow the percentage math.
    #[test]
    fn percent_math_survives_absurd_prices() {
        let mut catalog_index = index();
        catalog_index.catalog.insert(
            u64::MAX,
            CatalogItem {
                item_id: u64::MAX,
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                prices: vec![Price { cost: Some(u64::MAX), currency: Some("RP".to_owned()) }],
                localizations: BTreeMap::from([(
                    "en_US".to_owned(),
                    LocalizedText { name: Some("Broken Item".to_owned()) },
                )]),
                item_requirements: Vec::new(),
            },
        );
        let sale = super::super::lcu::Sale {
            id: 10,
            item: ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(u64::MAX),
            },
            sale: SaleInfo {
                start_date: None,
                end_date: None,
                prices: vec![Price { cost: Some(1), currency: Some("RP".to_owned()) }],
            },
        };
        let line = sale_line(&sale, &catalog_index);
        assert!(line.contains("\u{2212}100%"), "deep discount clamps to ~100: {line}");
    }

    /// Sections exceeding the embed budget are dropped from the tail, and
    /// the hidden count is recorded - the whole point is that the send path
    /// truncates nothing, so the text must always fit.
    #[test]
    fn oversized_announcements_drop_tail_sections() {
        let fat_rotation = |label: &str| super::super::diff::RotationDelta {
            label: label.to_owned(),
            rotation_start: None,
            next_rotation: None,
            entries: (0..15)
                .map(|n| super::super::diff::MythicEntry {
                    entry_id: None,
                    name: Some(format!("Prestige Skin Entry {n} of {label} {}", "x".repeat(90))),
                    mythic_price: Some(100),
                })
                .collect(),
        };
        let delta = StoreDelta {
            sales: vec![skin_sale(607, None)],
            rotations: vec![
                fat_rotation("daily"),
                fat_rotation("weekly"),
                fat_rotation("biweekly"),
                fat_rotation("monthly"),
            ],
            ..Default::default()
        };
        let text = announce_text(&delta, &index()).expect("announcement expected");
        assert!(text.contains("**New sales**"), "the headline section always survives");
        assert!(text.contains("more update groups hidden"));
        assert!(text.len() < 4096, "must fit the embed description limit");
        assert!(!text.contains("monthly"), "the lowest-priority tail goes first");
    }

    /// A single pathological section (absurd names) is hard-cut to the
    /// budget instead of being dropped wholesale.
    #[test]
    fn single_oversized_section_is_hard_cut() {
        let long_name = "A".repeat(400);
        let skins: Vec<CatalogItem> = (0..15)
            .map(|n| CatalogItem {
                item_id: 1000 + n,
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                prices: Vec::new(),
                localizations: BTreeMap::from([(
                    "en_US".to_owned(),
                    LocalizedText { name: Some(format!("{long_name} {n}")) },
                )]),
                item_requirements: Vec::new(),
            })
            .collect();
        let delta = StoreDelta { skins, ..Default::default() };
        let text = announce_text(&delta, &NameIndex::empty()).expect("announcement expected");
        assert!(text.len() < 4096, "must fit the embed description limit");
        assert!(text.ends_with('\u{2026}'), "hard cut marker expected");
    }
}
