//! Delta rendering: joins raw store data against the catalog and champion
//! name tables to produce the announcement text. Every line degrades
//! gracefully - a missing join yields a synthetic name, never an error.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::diff::StoreDelta;
use super::lcu::CatalogItem;

/// Lines shown per section before the "+N more" cut.
const MAX_LINES_PER_SECTION: usize = 15;

/// Hard cap for the joined announcement. The embed-description limit is
/// 4096 chars and the send path truncates nothing - an oversized embed is
/// rejected and silently loses the update for every guild. Stay under it
/// with margin (the bytes count BYTES, Discord counts code points).
const MAX_ANNOUNCEMENT_CHARS: usize = 3800;

/// Clamp for payload strings interpolated into section TITLES (dates,
/// timestamps, rotation labels). The lines of a section are cut per page
/// budget, but the title rides every chunk - an unbounded one would poison
/// the whole budget arithmetic.
const MAX_TITLE_FIELD_BYTES: usize = 32;

/// One catalog skin pre-joined for the `/lol_store_watch` name search:
/// champion prefix and display name resolved once at index build time, in
/// name-sorted order - the search is a linear filter over this list, not a
/// per-query re-sort of the ~9.5k catalog.
#[derive(Debug, Clone)]
pub(crate) struct SkinEntry {
    pub(crate) item_id: u64,
    pub(crate) champion: String,
    pub(crate) skin: String,
}

/// Name/pricing joins for one announcement. The catalog doubles as the
/// original-price source for the sale percentage (the sale payload's own
/// `discount` field is dead - always `0.0`).
pub struct NameIndex {
    pub catalog: HashMap<u64, CatalogItem>,
    pub champions: HashMap<u64, String>,
    pub(crate) skins: Vec<SkinEntry>,
    /// Champion ids the store catalog can actually reach - the ids its
    /// items resolve to. Matching is purely catalog-driven, so a
    /// champion-table row without one can never fire a watch (the live
    /// table lists some champions twice: live + Classic variants).
    pub(crate) store_backed: HashSet<u64>,
}

impl NameIndex {
    #[must_use]
    pub fn new(catalog: Vec<CatalogItem>, champions: BTreeMap<u64, String>) -> Self {
        let catalog: HashMap<u64, CatalogItem> =
            catalog.into_iter().map(|item| (item.item_id, item)).collect();
        let store_backed: HashSet<u64> =
            catalog.values().filter_map(Self::item_champion_id).collect();
        let mut index = Self {
            catalog,
            champions: champions.into_iter().collect(),
            skins: Vec::new(),
            store_backed,
        };
        let mut skins: Vec<SkinEntry> = index
            .catalog
            .values()
            .filter(|item| item.inventory_type.as_deref() == Some("CHAMPION_SKIN"))
            .filter_map(|item| {
                let champion = index.skin_champion(item)?;
                Some(SkinEntry { item_id: item.item_id, champion, skin: localized_name(item) })
            })
            .collect();
        // Deterministic order: name, then id - the search's exact-before-
        // partial split preserves this order within each bucket.
        skins.sort_by(|a, b| a.skin.cmp(&b.skin).then(a.item_id.cmp(&b.item_id)));
        index.skins = skins;
        index
    }

    /// Empty index: everything degrades to synthetic names.
    #[cfg(test)]
    #[must_use]
    pub fn empty() -> Self {
        Self {
            catalog: HashMap::new(),
            champions: HashMap::new(),
            skins: Vec::new(),
            store_backed: HashSet::new(),
        }
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
        let champion_id = Self::item_champion_id(item)?;
        Some(self.champion_name(champion_id))
    }

    /// The champion id behind a catalog item (see [`NameIndex::skin_champion`]).
    fn item_champion_id(item: &CatalogItem) -> Option<u64> {
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
        Self::item_champion_id(item)
    }

    fn original_price(&self, item_id: u64) -> Option<u64> {
        self.catalog.get(&item_id).and_then(|item| rp_price(&item.prices))
    }
}

/// One renderable announcement block: a bold title and optional body
/// lines. Sales render one section per end-date group; Your Shop is a bare
/// single-line section.
pub(crate) struct Section {
    title: String,
    lines: Vec<String>,
}

impl Section {
    fn new(title: impl Into<String>, lines: Vec<String>) -> Self {
        Self { title: title.into(), lines }
    }

    /// A title-only section - the whole text is the bold line.
    fn bare(title: String) -> Self {
        Self { title, lines: Vec::new() }
    }

    fn render(&self) -> String {
        if self.lines.is_empty() {
            format!("**{}**", self.title)
        } else {
            format!("**{}**\n{}", self.title, self.lines.join("\n"))
        }
    }

    /// Rendered size in bytes - the budget currency of `fit`/`paginate`.
    fn byte_len(&self) -> usize {
        self.render().len()
    }
}

/// Sort key for the price-ascending listings: priceless items sink to the
/// end; equal prices keep the API's arrival order (stable sort).
fn by_price(price: Option<u64>) -> u64 {
    price.unwrap_or(u64::MAX)
}

/// Sales grouped by end date: most of a cycle's sales share one end date,
/// and repeating it per line burns embed budget. Groups sort by date
/// ascending; within a group by sale price ascending (priceless last);
/// undated sales land in a trailing untitled group in the same order.
fn sales_sections(sales: &[super::lcu::Sale], index: &NameIndex) -> Vec<Section> {
    let mut dated: BTreeMap<String, Vec<&super::lcu::Sale>> = BTreeMap::new();
    let mut undated: Vec<&super::lcu::Sale> = Vec::new();
    for sale in sales {
        match sale.sale.end_date.as_deref() {
            Some(ends) => dated.entry(date(ends)).or_default().push(sale),
            None => undated.push(sale),
        }
    }
    let mut sections: Vec<Section> = dated
        .into_iter()
        .map(|(day, mut group)| {
            group.sort_by_key(|sale| by_price(rp_price(&sale.sale.prices)));
            let lines = group.iter().map(|sale| sale_line(sale, index, false)).collect();
            Section::new(format!("New sales \u{b7} until {day}"), lines)
        })
        .collect();
    if !undated.is_empty() {
        undated.sort_by_key(|sale| by_price(rp_price(&sale.sale.prices)));
        let lines = undated.iter().map(|sale| sale_line(sale, index, false)).collect();
        sections.push(Section::new("New sales", lines));
    }
    sections
}

/// Renders the full announcement into embed-sized pages - every section,
/// every line: sales grouped by end date, new skins, each mythic rotation,
/// the Your Shop start - items within a section cheapest first (priceless
/// last). Nothing is dropped or cut (a page pack that would overflow
/// starts a new page instead). Empty when nothing renders.
#[must_use]
pub fn announce_pages(delta: &StoreDelta, index: &NameIndex) -> Vec<String> {
    let mut sections: Vec<Section> = Vec::new();
    if !delta.sales.is_empty() {
        sections.extend(sales_sections(&delta.sales, index));
    }
    if !delta.skins.is_empty() {
        let mut skins: Vec<&CatalogItem> = delta.skins.iter().collect();
        skins.sort_by_key(|item| by_price(rp_price(&item.prices)));
        let lines: Vec<String> = skins.into_iter().map(|item| skin_line(item, index)).collect();
        sections.push(Section::new("New in store", lines));
    }
    for rotation in &delta.rotations {
        let mut entries: Vec<&super::diff::MythicEntry> = rotation.entries.iter().collect();
        entries.sort_by_key(|entry| by_price(entry.mythic_price));
        let lines: Vec<String> = entries.into_iter().map(mythic_line).collect();
        if lines.is_empty() {
            continue;
        }
        let mut title = format!("Mythic rotation ({})", rotation.label);
        if let Some(ends) = non_empty(rotation.next_rotation.as_deref()) {
            title.push_str(&format!(" \u{b7} ends {}", date(ends)));
        }
        sections.push(Section::new(title, lines));
    }
    if let Some(start) = &delta.yourshop {
        let mut line = String::from("Your Shop started");
        if let Some(started) = non_empty(start.start.as_deref()) {
            line.push_str(&format!(" \u{b7} started {}", date(started)));
        }
        if let Some(ends) = non_empty(start.end.as_deref()) {
            line.push_str(&format!(" \u{b7} ends {}", date(ends)));
        }
        sections.push(Section::bare(line));
    }
    paginate(sections)
}

/// Rendered byte length of `**{title}**\n{lines joined by newline}` (or
/// just `**{title}**` with no lines) - the exact currency of the budget.
fn chunk_len(title: &str, lines: &[String]) -> usize {
    let base = title.len() + 4; // the two ** pairs
    if lines.is_empty() {
        return base;
    }
    base + 1 + lines.iter().map(|line| line.len()).sum::<usize>() + lines.len() - 1
}

/// Splits a section whose rendered body exceeds the page budget between
/// its lines - the title returns with " (cont.)" on every continuation -
/// and char-safely hard-cuts a single line too long for even a fresh
/// chunk. A section that fits passes through untouched.
fn explode(section: &Section) -> Vec<Section> {
    if section.byte_len() <= MAX_ANNOUNCEMENT_CHARS {
        return vec![Section::new(section.title.clone(), section.lines.clone())];
    }
    // A title longer than half a page cannot share a page with anything -
    // cut it once, up front, so the budget subtraction below can never
    // underflow. (Title text is clamped at its sources already; this is
    // the belt to those braces.)
    let base_title = cut_to_budget(&section.title, MAX_ANNOUNCEMENT_CHARS / 2);
    let fresh_budget = MAX_ANNOUNCEMENT_CHARS - base_title.len() - " (cont.)".len() - 5;
    let mut chunks: Vec<Section> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut title = base_title.clone();
    let mut used = chunk_len(&title, &current);
    for line in &section.lines {
        let line = cut_to_budget(line, fresh_budget);
        if used + 1 + line.len() > MAX_ANNOUNCEMENT_CHARS {
            chunks.push(Section::new(std::mem::take(&mut title), std::mem::take(&mut current)));
            title = format!("{base_title} (cont.)");
            used = chunk_len(&title, &current);
        }
        used += 1 + line.len();
        current.push(line);
    }
    if !current.is_empty() {
        chunks.push(Section::new(title, current));
    }
    chunks
}

/// Char-safe AND byte-bounded truncation for a pathological single line:
/// the budget counts bytes (Discord counts code points, so staying under
/// in bytes always stays under in chars too), and the ellipsis itself is
/// 3 bytes - reserved up front.
fn cut_to_budget(line: &str, budget: usize) -> String {
    if line.len() <= budget {
        return line.to_owned();
    }
    let mut cut = String::new();
    let mut taken = 0usize;
    for ch in line.chars() {
        let len = ch.len_utf8();
        if taken + len > budget.saturating_sub(3) {
            break;
        }
        taken += len;
        cut.push(ch);
    }
    cut.push('\u{2026}');
    cut
}

/// Packs sections into embed-sized pages. Nothing is dropped: whole
/// sections fill a page greedily and a taller-than-page section arrives
/// pre-split by [`explode`]. Empty input renders no pages.
pub(crate) fn paginate(sections: Vec<Section>) -> Vec<String> {
    let mut pages: Vec<String> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut used = 0usize;
    for section in &sections {
        for chunk in explode(section) {
            let text = chunk.render();
            if !current.is_empty() && used + 2 + text.len() > MAX_ANNOUNCEMENT_CHARS {
                pages.push(current.join("\n\n"));
                current = Vec::new();
                used = 0;
            }
            used += if current.is_empty() { text.len() } else { 2 + text.len() };
            current.push(text);
        }
    }
    if !current.is_empty() {
        pages.push(current.join("\n\n"));
    }
    pages
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

pub(crate) fn sale_line(sale: &super::lcu::Sale, index: &NameIndex, with_date: bool) -> String {
    let item_id = sale.item.item_id.unwrap_or(0);

    let subject = match index.catalog.get(&item_id) {
        Some(item) => match index.skin_champion(item) {
            Some(champion) => format!("{champion} \u{2014} {}", localized_name(item)),
            None => localized_name(item),
        },
        // No catalog join: the id is the only identifying data left.
        None => format!("Skin {item_id}"),
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
        line.push_str(&format!(" \u{b7} {cost} RP"));
    }
    // Announcements group by end date, so the grouped path omits the
    // per-line repetition; watch pings (one sale per line) keep it.
    if with_date {
        if let Some(ends) = sale.sale.end_date.as_deref() {
            line.push_str(&format!(" \u{b7} until {}", date(ends)));
        }
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

/// `2026-10-05T17:00:00.000+00:00` -> `2026-10-05`. The input is untrusted
/// payload text that lands inside section titles - anything implausibly
/// long degrades to a cut.
#[must_use]
pub(crate) fn date(iso: &str) -> String {
    cut_to_budget(iso.split('T').next().unwrap_or(iso), MAX_TITLE_FIELD_BYTES)
}

/// Payload date placeholders (empty strings - e.g. the featured mythic
/// shelf carries no rotation timestamps) must not render as a dangling
/// "ends"/"started".
#[must_use]
pub(crate) fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
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
        skin_sale_named(1, 1031, cost, end)
    }

    fn skin_sale_named(
        id: u64,
        item_id: u64,
        cost: u64,
        end: Option<&str>,
    ) -> super::super::lcu::Sale {
        super::super::lcu::Sale {
            id,
            item: ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(item_id),
            },
            sale: SaleInfo {
                start_date: None,
                end_date: end.map(str::to_owned),
                prices: vec![Price { cost: Some(cost), currency: Some("RP".to_owned()) }],
            },
        }
    }

    #[test]
    fn sale_line_joins_names_price_percent_and_date() {
        let line =
            sale_line(&skin_sale(607, Some("2026-10-05T17:00:00.000+00:00")), &index(), true);
        assert_eq!(line, "- Ahri — Foxfire Ahri −38% · 607 RP · until 2026-10-05");
    }

    #[test]
    fn sale_line_skips_percent_when_not_discounted() {
        let line = sale_line(&skin_sale(975, None), &index(), true);
        assert_eq!(line, "- Ahri — Foxfire Ahri · 975 RP");
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
        let line = sale_line(&sale, &NameIndex::empty(), true);
        assert_eq!(line, "- Skin 42042 · 500 RP");
    }

    #[test]
    fn announce_pages_render_all_sections() {
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
        let pages = announce_pages(&delta, &index);
        assert_eq!(pages.len(), 1, "small delta: one page, {pages:?}");
        let text = pages.first().expect("page expected");
        assert!(text.contains("**New sales**"));
        assert!(text.contains("- Ahri — Foxfire Ahri −38% · 607 RP"));
        assert!(text.contains("**New in store**"));
        assert!(text.contains("- Kayle — Viridian Kayle · 520 RP"));
        assert!(text.contains("**Mythic rotation (weekly)**"));
        assert!(text.contains("- Prestige Ocean Song Seraphine · 35 ME"));
        assert!(text.contains("**Your Shop started · started 2026-10-01 · ends 2026-10-08**"));
    }

    #[test]
    fn empty_delta_renders_no_pages() {
        assert!(announce_pages(&StoreDelta::default(), &NameIndex::empty()).is_empty());
    }

    /// The point of pagination: every sale prints, none is cut. The old
    /// per-section cap and the hidden-count footer are gone.
    #[test]
    fn announce_pages_render_every_line_without_cuts() {
        let sales: Vec<super::super::lcu::Sale> =
            (0..20).map(|id| skin_sale_named(id, 10_000 + id, 975, None)).collect();
        let delta = StoreDelta { sales, ..Default::default() };
        let pages = announce_pages(&delta, &NameIndex::empty());
        assert_eq!(pages.len(), 1);
        let text = pages.join("\n\n");
        for id in 0..20u64 {
            assert!(
                text.contains(&format!("- Skin {} \u{b7}", 10_000 + id)),
                "sale {id} missing: {text}"
            );
        }
        assert!(!text.contains('\u{2026}'), "nothing is cut: {text}");
    }

    /// Sales group under one bold title per end date (soonest first), and
    /// the per-line date repetition disappears - that is the whole budget
    /// win of grouping.
    #[test]
    fn sales_group_by_end_date() {
        let sales = vec![
            skin_sale_named(2, 20_001, 700, Some("2026-10-12T23:59:00.000Z")),
            skin_sale_named(1, 10_001, 607, Some("2026-10-05T17:00:00.000+00:00")),
            skin_sale_named(3, 30_001, 500, None),
        ];
        let delta = StoreDelta { sales, ..Default::default() };
        let text = announce_pages(&delta, &NameIndex::empty()).join("\n\n");
        let position = |needle: &str| text.find(needle).expect(needle);
        assert!(position("**New sales · until 2026-10-05**") < position("- Skin 10001"));
        assert!(position("**New sales · until 2026-10-12**") < position("- Skin 20001"));
        assert!(
            position("**New sales · until 2026-10-05**")
                < position("**New sales · until 2026-10-12**")
        );
        assert!(text.contains("**New sales**\n- Skin 30001"), "undated sales trail: {text}");
        assert!(!text.contains("RP \u{b7} until"), "dates live in the titles only: {text}");
    }

    /// Within one end-date group the cheapest sale prints first and a
    /// priceless sale (empty price list) sinks last; the same price order
    /// applies to the undated trailing group.
    #[test]
    fn sales_sort_by_price_within_a_group() {
        let priceless = super::super::lcu::Sale {
            id: 9,
            item: ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(50_001),
            },
            sale: SaleInfo {
                start_date: None,
                end_date: Some("2026-10-05T17:00:00.000+00:00".to_owned()),
                prices: Vec::new(),
            },
        };
        let sales = vec![
            skin_sale_named(2, 20_001, 700, Some("2026-10-05T17:00:00.000+00:00")),
            priceless,
            skin_sale_named(1, 10_001, 607, Some("2026-10-05T17:00:00.000+00:00")),
            skin_sale_named(3, 40_001, 800, None),
        ];
        let delta = StoreDelta { sales, ..Default::default() };
        let text = announce_pages(&delta, &NameIndex::empty()).join("\n\n");
        let position = |needle: &str| text.find(needle).expect(needle);
        // Grouped by day, cheapest first, priceless sinks to the group's end.
        assert!(position("- Skin 10001 \u{b7} 607 RP") < position("- Skin 20001 \u{b7} 700 RP"));
        assert!(position("- Skin 20001 \u{b7} 700 RP") < position("- Skin 50001"));
        assert!(
            position("**New sales \u{b7} until 2026-10-05**") < position("**New sales**\n"),
            "dated groups precede the undated one"
        );
    }

    /// The skins and mythic listings are price-ascending too, priceless
    /// entries last.
    #[test]
    fn skins_and_mythic_sort_by_price() {
        let skin = |id: u64, cost: Option<u64>| CatalogItem {
            item_id: id,
            inventory_type: Some("CHAMPION_SKIN".to_owned()),
            prices: cost
                .map(|cost| vec![Price { cost: Some(cost), currency: Some("RP".to_owned()) }])
                .unwrap_or_default(),
            ..CatalogItem::default()
        };
        let entry = |name: &str, price: Option<u64>| super::super::diff::MythicEntry {
            entry_id: None,
            name: Some(name.to_owned()),
            mythic_price: price,
        };
        let delta = StoreDelta {
            skins: vec![skin(20_002, Some(1350)), skin(10_002, Some(520)), skin(30_002, None)],
            rotations: vec![super::super::diff::RotationDelta {
                label: "weekly".to_owned(),
                rotation_start: None,
                next_rotation: None,
                entries: vec![
                    entry("Expensive", Some(100)),
                    entry("Priceless", None),
                    entry("Cheap", Some(35)),
                ],
            }],
            ..Default::default()
        };
        let text = announce_pages(&delta, &NameIndex::empty()).join("\n\n");
        let position = |needle: &str| text.find(needle).expect(needle);
        assert!(
            position("- Champion 10 — Skin 10002 · 520 RP")
                < position("- Champion 20 — Skin 20002 · 1350 RP")
        );
        assert!(
            position("- Champion 20 — Skin 20002 · 1350 RP")
                < position("- Champion 30 — Skin 30002")
        );
        assert!(position("- Cheap · 35 ME") < position("- Expensive · 100 ME"));
        assert!(position("- Expensive · 100 ME") < position("- Priceless"));
    }

    /// Payload strings interpolated into section titles are clamped: a
    /// multi-kilobyte date (no `T`, so `date` keeps it whole) must neither
    /// underflow the pagination budget nor produce an oversized page - the
    /// pre-fix failure mode this pins.
    #[test]
    fn oversized_title_fields_are_clamped() {
        assert!(date(&"x".repeat(5000)).chars().count() <= 33);

        let corrupted_end = "corrupted-without-T-".repeat(200);
        let sale = |id: u64| super::super::lcu::Sale {
            id,
            item: ItemRef {
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                item_id: Some(10_000 + id),
            },
            sale: SaleInfo {
                start_date: None,
                end_date: Some(corrupted_end.clone()),
                prices: vec![Price { cost: Some(607), currency: Some("RP".to_owned()) }],
            },
        };
        // Many lines force `explode` to run with the clamped title.
        let sales: Vec<super::super::lcu::Sale> = (0..300u64).map(sale).collect();
        let delta = StoreDelta { sales, ..Default::default() };
        let pages = announce_pages(&delta, &NameIndex::empty());
        assert!(!pages.is_empty(), "the pathological delta still renders");
        for page in &pages {
            assert!(page.len() <= MAX_ANNOUNCEMENT_CHARS, "page budget: {}", page.len());
        }
    }

    /// A budget landing mid-codepoint cuts on chars, not bytes, and still
    /// respects the byte budget (the ellipsis's 3 bytes included).
    #[test]
    fn cut_to_budget_is_char_safe_and_byte_bounded() {
        let line = "ж".repeat(100); // 2 bytes per char
        let cut = cut_to_budget(&line, 21);
        assert!(cut.len() <= 21, "byte budget respected: {}", cut.len());
        assert!(cut.chars().count() <= 10, "char-safe cut: {cut}");
        let stem = cut.trim_end_matches('\u{2026}');
        assert!(line.starts_with(stem), "the cut keeps a whole-char prefix");
    }

    /// The rotation section title carries the end date when the payload
    /// provides it - date only, no time.
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
        let text = announce_pages(&delta, &NameIndex::empty()).join("\n\n");
        assert!(text.contains("**Mythic rotation (weekly) · ends 2026-10-08**"), "text: {text}");
    }

    /// A placeholder end date (empty string - the featured mythic shelf
    /// carries one) must not render a dangling "ends".
    #[test]
    fn empty_rotation_end_omits_the_ends_suffix() {
        let delta = StoreDelta {
            rotations: vec![super::super::diff::RotationDelta {
                label: "featured".to_owned(),
                rotation_start: None,
                next_rotation: Some("   ".to_owned()),
                entries: vec![super::super::diff::MythicEntry {
                    entry_id: None,
                    name: Some("Hextech Tristana".to_owned()),
                    mythic_price: Some(125),
                }],
            }],
            ..StoreDelta::default()
        };
        let text = announce_pages(&delta, &NameIndex::empty()).join("\n\n");
        assert!(text.contains("**Mythic rotation (featured)**"), "text: {text}");
        assert!(!text.contains("ends"), "no dangling ends: {text}");
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
        let line = sale_line(&sale, &catalog_index, true);
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
        let line = sale_line(&sale, &catalog_index, false);
        assert!(line.contains("\u{2212}100%"), "deep discount clamps to ~100: {line}");
    }

    /// Oversized announcements SPLIT into pages - nothing is dropped from
    /// the tail anymore, and every page stays inside Discord's embed limit.
    #[test]
    fn oversized_announcements_split_into_pages() {
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
        let pages = announce_pages(&delta, &index());
        assert!(pages.len() > 1, "a full store must span pages: {}", pages.len());
        let text = pages.join("\n\n");
        for label in ["daily", "weekly", "biweekly", "monthly"] {
            assert!(text.contains(label), "no section is dropped: {label}");
        }
        assert!(!text.contains("hidden"), "no hidden-count footer anymore");
        for page in &pages {
            assert!(page.len() <= 3800, "page inside the budget: {}", page.len());
        }
    }

    /// A section taller than a whole page splits BETWEEN its lines (the
    /// title returns with a continuation marker), and a single line longer
    /// than the budget is char-safely hard-cut instead of overflowing.
    #[test]
    fn oversized_section_splits_and_hard_cuts_a_pathological_line() {
        let long_name = "A".repeat(300);
        let mut skins: Vec<CatalogItem> = (0..30)
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
        skins.push(CatalogItem {
            item_id: 9999,
            inventory_type: Some("CHAMPION_SKIN".to_owned()),
            prices: Vec::new(),
            localizations: BTreeMap::from([(
                "en_US".to_owned(),
                LocalizedText { name: Some("B".repeat(4000)) },
            )]),
            item_requirements: Vec::new(),
        });
        let delta = StoreDelta { skins, ..Default::default() };
        let pages = announce_pages(&delta, &NameIndex::empty());
        assert!(pages.len() > 1, "30 fat skins must span pages");
        for page in &pages {
            assert!(page.len() <= 3800, "page inside the budget: {}", page.len());
        }
        let text = pages.join("\n\n");
        assert!(text.contains("(cont.)"), "continuation title expected");
        assert!(text.contains('\u{2026}'), "the pathological line is hard-cut");
        for n in 0..30u32 {
            assert!(text.contains(&format!("{long_name} {n}")), "skin {n} missing: {}", text.len());
        }
    }
}
