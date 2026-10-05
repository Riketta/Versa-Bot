//! Per-user store watches: a user subscribes to a skin or to a champion's
//! whole skin line and gets pinged in the guild's announcement channel when
//! a watched subject hits a new sale, a Mythic Shop rotation, or a first
//! store listing (champion watches only - a catalog-resolvable skin is by
//! definition already released).
//!
//! Pure domain: types, persistence shape, name normalization, delta
//! matching, candidate search. All I/O lives in the engine (notification
//! fan-out, catalog access) and the commands (read-modify-write).

use serde::{Deserialize, Serialize};

use std::collections::HashSet;

use super::diff::StoreDelta;
use super::format::{NameIndex, fit, mythic_line, push_section, sale_line, skin_line};

/// Storage key of the guild's watch document (plugin namespace).
pub const WATCH_KEY: &str = "subscriptions";

/// Default cap on watches per user per guild (config: `watch_user_cap`).
pub const DEFAULT_USER_CAP: u32 = 20;
/// Default cap on watches per guild (config: `watch_guild_cap`).
pub const DEFAULT_GUILD_CAP: u32 = 300;

/// Candidates shown before an ambiguous name search is cut off.
const SEARCH_RESULT_CAP: usize = 10;

/// Tag budget inside the 2000-char Discord content limit (embeds never
/// notify; the tags must ride the content).
const TAG_BUDGET_CHARS: usize = 1800;

/// One watch kind as chosen in `/lol_store_watch`. `All` covers the three
/// concrete kinds; narrower subsets (sale + mythic without release) are
/// expressed as two subscriptions - the single-select dropdown keeps it
/// that way, and the caps comfortably allow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchKind {
    Sale,
    Mythic,
    Release,
    All,
}

impl WatchKind {
    /// Parses the dropdown value; the command's `kinds` choices mirror this.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "sale" => Some(Self::Sale),
            "mythic" => Some(Self::Mythic),
            "release" => Some(Self::Release),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    /// The value as shown in lists and confirmations.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Sale => "sale",
            Self::Mythic => "mythic",
            Self::Release => "release",
            Self::All => "all",
        }
    }

    /// Does this selection fire for the given concrete edge?
    #[must_use]
    pub fn covers(self, edge: Edge) -> bool {
        match self {
            Self::All => true,
            Self::Sale => edge == Edge::Sale,
            Self::Mythic => edge == Edge::Mythic,
            Self::Release => edge == Edge::Release,
        }
    }
}

/// A concrete store edge a notification can fire on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edge {
    Sale,
    Mythic,
    Release,
}

/// What a watch points at: one resolved skin, or a champion's whole skin
/// line (champion watches catch future skins by construction). Display
/// names are denormalized at subscribe time and never re-resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WatchTarget {
    Skin { item_id: u64, champion: String, skin: String },
    Champion { champion_id: u64, champion: String },
}

impl WatchTarget {
    /// Short human label for lists and confirmations.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Skin { champion, skin, .. } => format!("{champion} - {skin}"),
            Self::Champion { champion, .. } => champion.clone(),
        }
    }
}

/// One user's watch. `id` is stable per guild - the handle
/// `/lol_store_unwatch` takes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watch {
    pub id: u64,
    /// Platform user id (string - snowflakes exceed JSON numbers).
    pub user_id: String,
    pub target: WatchTarget,
    pub kinds: WatchKind,
}

/// The guild's watch document (`subscriptions` key, plugin namespace).
/// A doc that fails to deserialize is logged and treated as empty - ids may
/// restart, but the alternative (refusing mutations) bricks the guild's
/// watch commands until manual storage surgery. The unreadable raw is
/// preserved under a recovery key before the first overwrite, so the
/// corruption stays diagnosable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchDoc {
    /// Forward-compat scaffolding: no reader or migration exists yet - a
    /// bump signals a future shape migration.
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default = "default_next_id")]
    pub next_id: u64,
    #[serde(default)]
    pub subs: Vec<Watch>,
}

fn default_version() -> u32 {
    1
}

fn default_next_id() -> u64 {
    1
}

impl Default for WatchDoc {
    fn default() -> Self {
        Self { version: default_version(), next_id: default_next_id(), subs: Vec::new() }
    }
}

impl WatchDoc {
    /// Appends a watch and returns its stable id.
    pub fn insert(&mut self, user_id: String, target: WatchTarget, kinds: WatchKind) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.subs.push(Watch { id, user_id, target, kinds });
        id
    }

    /// Removes one watch owned by `user_id` and returns it.
    pub fn remove(&mut self, user_id: &str, id: u64) -> Option<Watch> {
        let index =
            self.subs.iter().position(|watch| watch.id == id && watch.user_id == user_id)?;
        Some(self.subs.remove(index))
    }

    /// Removes every watch of `user_id` and returns them.
    pub fn remove_all_of(&mut self, user_id: &str) -> Vec<Watch> {
        let (kept, removed): (Vec<_>, Vec<_>) =
            self.subs.drain(..).partition(|watch| watch.user_id != user_id);
        self.subs = kept;
        removed
    }
}

/// A store subject a watch can match against: a resolved item id, or - for
/// mythic entries whose id does not join to the catalog - a display name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Subject {
    Item(u64),
    Name(String),
}

/// Resolves one mythic entry onto a matchable subject. Id first (verified
/// against the catalog), raw display name as the fallback - the name path
/// only ever matches skin watches (see [`target_matches`]).
#[must_use]
pub(crate) fn entry_subject(entry: &super::diff::MythicEntry, index: &NameIndex) -> Subject {
    match entry.entry_id.as_deref().and_then(|id| id.parse::<u64>().ok()) {
        Some(id) if index.catalog.contains_key(&id) => Subject::Item(id),
        _ => Subject::Name(entry.name.clone().unwrap_or_default()),
    }
}

/// Does the watch target match this store subject?
#[must_use]
pub(crate) fn target_matches(target: &WatchTarget, subject: &Subject, index: &NameIndex) -> bool {
    match (target, subject) {
        (WatchTarget::Skin { item_id, .. }, Subject::Item(id)) => item_id == id,
        (WatchTarget::Skin { skin, .. }, Subject::Name(name)) => {
            let want = normalize_name(skin);
            let have = normalize_name(name);
            // Second chance on the "loose key": Riot's shoppefront names
            // append decorative parenthesized suffixes the catalog name
            // lacks ("K/DA ALL OUT Akali (BADDEST)").
            want == have || loose_name(skin) == loose_name(name)
        }
        (WatchTarget::Champion { champion_id, .. }, Subject::Item(id)) => {
            index.champion_id_of_item(*id) == Some(*champion_id)
        }
        // A bare display name carries no champion information.
        (WatchTarget::Champion { .. }, Subject::Name(_)) => false,
    }
}

/// Normalized name with parenthesized segments stripped - the second-
/// chance comparison key for name-based watch matching.
#[must_use]
fn loose_name(name: &str) -> String {
    let mut stripped = String::with_capacity(name.len());
    let mut depth = 0u32;
    for ch in name.chars() {
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            _ if depth == 0 => stripped.push(ch),
            _ => {}
        }
    }
    normalize_name(&stripped)
}

/// Lowercases, replaces every non-alphanumeric with a space, collapses
/// runs of space: `"Kai'Sa!"` and `"Kai Sa"` collide on purpose - both
/// sides of every name comparison go through this.
#[must_use]
pub fn normalize_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut last_space = false;
    for ch in name.chars() {
        if ch.is_alphanumeric() {
            out.extend(ch.to_lowercase());
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    out.trim().to_owned()
}

/// A query that names a row by id rather than by name: a bare number
/// (`41`), a bare `id 41`, or a candidate line pasted back
/// (`Gangplank (id 41)`). Candidate replies print ids, so every form the
/// reply suggests must parse back.
#[must_use]
pub fn query_id(query: &str) -> Option<u64> {
    let trimmed = query.trim();
    if let Ok(id) = trimmed.parse::<u64>() {
        return Some(id);
    }
    let stripped = trimmed.strip_suffix(')').unwrap_or(trimmed).trim_end();
    let (head, num) = stripped.rsplit_once(' ')?;
    if num.is_empty() || !num.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    // The head must end with the whole token `id` (possibly parenthesized).
    let head_lower = head.to_lowercase();
    let Some(without_marker) = head_lower.strip_suffix("id") else {
        return None;
    };
    match without_marker.chars().next_back() {
        None | Some('(') => {}
        Some(c) if c.is_whitespace() => {}
        _ => return None,
    }
    num.parse::<u64>().ok()
}

/// A skin resolved from the catalog by name search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkinHit {
    pub item_id: u64,
    /// Champion table id behind the skin - live and Classic variants
    /// share display names but not ids.
    pub champion_id: u64,
    pub champion: String,
    pub skin: String,
}

/// A champion resolved from the champion table by name search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChampionHit {
    pub champion_id: u64,
    pub champion: String,
}

/// The result of one `/lol_store_watch` name search over both namespaces.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreSearch {
    pub skins: Vec<SkinHit>,
    pub champions: Vec<ChampionHit>,
    /// Champion-table ids the current catalog can actually reach (watch
    /// matching is catalog-driven; an unreached id can never fire).
    pub store_backed: HashSet<u64>,
}

/// Name search over catalog skins: exact (normalized) matches first, then
/// substring matches, deterministic order, capped. The index carries the
/// champion-joined skins pre-sorted by name - the search is a linear
/// filter, not a per-query re-sort of the catalog.
#[must_use]
pub fn search_skins(query: &str, index: &NameIndex) -> Vec<SkinHit> {
    let needle = normalize_name(query);
    if needle.is_empty() {
        return Vec::new();
    }
    let mut exact = Vec::new();
    let mut partial = Vec::new();
    for entry in &index.skins {
        let normalized = normalize_name(&entry.skin);
        let hit = SkinHit {
            item_id: entry.item_id,
            champion_id: entry.champion_id,
            champion: entry.champion.clone(),
            skin: entry.skin.clone(),
        };
        if normalized == needle {
            exact.push(hit);
        } else if normalized.contains(&needle) {
            partial.push(hit);
        }
    }
    exact.into_iter().chain(partial).take(SEARCH_RESULT_CAP).collect()
}

/// Name search over the champion table: exact first, then substrings.
#[must_use]
pub fn search_champions<'a>(
    query: &str,
    champions: impl IntoIterator<Item = (&'a u64, &'a String)>,
) -> Vec<ChampionHit> {
    let needle = normalize_name(query);
    if needle.is_empty() {
        return Vec::new();
    }
    let mut exact = Vec::new();
    let mut partial = Vec::new();
    let mut entries: Vec<(&u64, &String)> = champions.into_iter().collect();
    entries.sort_by(|a, b| a.1.cmp(b.1));
    for (id, name) in entries {
        let normalized = normalize_name(name);
        let hit = ChampionHit { champion_id: *id, champion: name.clone() };
        if normalized == needle {
            exact.push(hit);
        } else if normalized.contains(&needle) {
            partial.push(hit);
        }
    }
    exact.into_iter().chain(partial).take(SEARCH_RESULT_CAP).collect()
}

/// One guild's watch notification: the tags that ride the message content
/// and the rendered embed body.
pub(crate) struct WatchNotification {
    pub tags: String,
    pub text: String,
    /// Distinct users pinged (for logs and the record entry).
    pub watchers: usize,
}

/// Matches a watch document against a store delta and renders the
/// notification. `None` when nothing matches (or nothing renders).
#[must_use]
pub(crate) fn build_notification(
    doc: &WatchDoc,
    delta: &StoreDelta,
    index: &NameIndex,
) -> Option<WatchNotification> {
    if doc.subs.is_empty() {
        return None;
    }
    let mut users: Vec<String> = Vec::new();
    let mut sections: Vec<String> = Vec::new();

    let mut lines = Vec::new();
    for sale in &delta.sales {
        let Some(item_id) = sale.item.item_id else { continue };
        let Some(matched) = matching_users(doc, Edge::Sale, &Subject::Item(item_id), index) else {
            continue;
        };
        lines.push(sale_line(sale, index, true));
        collect_users(&mut users, matched);
    }
    push_section(&mut sections, "On sale", &lines);

    let mut lines = Vec::new();
    for rotation in &delta.rotations {
        for entry in &rotation.entries {
            let subject = entry_subject(entry, index);
            let Some(matched) = matching_users(doc, Edge::Mythic, &subject, index) else {
                continue;
            };
            lines.push(mythic_line(entry));
            collect_users(&mut users, matched);
        }
    }
    push_section(&mut sections, "Mythic rotation", &lines);

    let mut lines = Vec::new();
    for item in &delta.skins {
        let Some(matched) = matching_users(doc, Edge::Release, &Subject::Item(item.item_id), index)
        else {
            continue;
        };
        lines.push(skin_line(item, index));
        collect_users(&mut users, matched);
    }
    push_section(&mut sections, "New in store", &lines);

    let text = fit(sections)?;
    Some(WatchNotification { tags: tag_content(&users), text, watchers: users.len() })
}

/// Users whose watches match this subject on this edge - `None` when no
/// one watches it (the common case; early-outs the rendering). Watches
/// with non-numeric user ids never participate: storage is trusted, but a
/// hand-tampered doc must not be able to inject mention syntax (e.g.
/// `<@everyone>`) into the ping content.
fn matching_users(
    doc: &WatchDoc,
    edge: Edge,
    subject: &Subject,
    index: &NameIndex,
) -> Option<Vec<String>> {
    let matched: Vec<String> = doc
        .subs
        .iter()
        .filter(|watch| {
            watch.user_id.parse::<u64>().is_ok()
                && watch.kinds.covers(edge)
                && target_matches(&watch.target, subject, index)
        })
        .map(|watch| watch.user_id.clone())
        .collect();
    if matched.is_empty() { None } else { Some(matched) }
}

fn collect_users(users: &mut Vec<String>, matched: Vec<String>) {
    for user in matched {
        if !users.contains(&user) {
            users.push(user);
        }
    }
}

/// Renders the user tags that must ride the message content, guarded
/// against the content limit (an absurd watch match crowd gets "+N more").
fn tag_content(users: &[String]) -> String {
    let mut content = String::new();
    let mut shown = 0usize;
    for user in users {
        let tag = format!("<@{user}> ");
        if content.len() + tag.len() > TAG_BUDGET_CHARS {
            break;
        }
        content.push_str(&tag);
        shown += 1;
    }
    let hidden = users.len() - shown;
    if hidden > 0 {
        content.push_str(&format!("+{hidden} more"));
    }
    content.trim_end().to_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::diff::MythicEntry;
    use super::super::format::champion_map;
    use super::super::lcu::{CatalogItem, ItemRef, LocalizedText, Price};
    use super::*;

    fn index() -> NameIndex {
        NameIndex::new(
            vec![
                CatalogItem {
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
                },
                CatalogItem {
                    item_id: 1032,
                    inventory_type: Some("CHAMPION_SKIN".to_owned()),
                    prices: vec![Price { cost: Some(975), currency: Some("RP".to_owned()) }],
                    localizations: BTreeMap::from([(
                        "en_US".to_owned(),
                        LocalizedText { name: Some("Dynasty Ahri".to_owned()) },
                    )]),
                    item_requirements: vec![ItemRef {
                        inventory_type: Some("CHAMPION".to_owned()),
                        item_id: Some(103),
                    }],
                },
            ],
            champion_map([(103, Some("Ahri".to_owned()))]),
        )
    }

    fn skin_target(item_id: u64, skin: &str) -> WatchTarget {
        WatchTarget::Skin { item_id, champion: "Ahri".to_owned(), skin: skin.to_owned() }
    }

    fn doc_of(watches: Vec<Watch>) -> WatchDoc {
        WatchDoc { version: 1, next_id: watches.len() as u64 + 1, subs: watches }
    }

    fn watch(id: u64, target: WatchTarget, kinds: WatchKind) -> Watch {
        Watch { id, user_id: "111".to_owned(), target, kinds }
    }

    #[test]
    fn normalize_strips_punctuation_and_case() {
        assert_eq!(normalize_name("Kai'Sa!"), "kai sa");
        assert_eq!(normalize_name("  Blood   Moon  Evelynn "), "blood moon evelynn");
        assert_eq!(normalize_name("Nunu & Willump"), "nunu willump");
        assert_eq!(normalize_name("!!!"), "");
    }

    #[test]
    fn query_id_parses_bare_and_pasted_forms() {
        assert_eq!(query_id("41"), Some(41));
        assert_eq!(query_id(" 41 "), Some(41));
        assert_eq!(query_id("Gangplank (id 41)"), Some(41));
        assert_eq!(query_id("id 41"), Some(41));
        assert_eq!(query_id("Blood Moon Evelynn"), None);
        assert_eq!(query_id("Gangplank (id x)"), None);
        assert_eq!(query_id("Gangplank (41"), None);
    }

    #[test]
    fn kind_coverage_matrix() {
        assert!(WatchKind::All.covers(Edge::Sale));
        assert!(WatchKind::All.covers(Edge::Mythic));
        assert!(WatchKind::All.covers(Edge::Release));
        assert!(WatchKind::Sale.covers(Edge::Sale));
        assert!(!WatchKind::Sale.covers(Edge::Mythic));
        assert!(!WatchKind::Mythic.covers(Edge::Release));
        assert!(!WatchKind::Release.covers(Edge::Sale));
    }

    #[test]
    fn doc_roundtrips_and_manages_ids() {
        let doc = doc_of(vec![watch(1, skin_target(1031, "Foxfire Ahri"), WatchKind::Sale)]);
        let raw = serde_json::to_value(&doc).expect("serialize");
        let back: WatchDoc = serde_json::from_value(raw).expect("deserialize");
        assert_eq!(back, doc);
        assert_eq!(back.next_id, 2);
        // Fresh docs (and partial docs missing new fields) start ids at 1.
        let fresh: WatchDoc = serde_json::from_value(serde_json::json!({})).expect("defaults");
        assert_eq!(fresh.next_id, 1);
        assert!(fresh.subs.is_empty());
    }

    #[test]
    fn removal_is_user_scoped() {
        let mut doc = doc_of(vec![watch(1, skin_target(1031, "Foxfire Ahri"), WatchKind::All)]);
        doc.subs.push(Watch {
            id: 2,
            user_id: "222".to_owned(),
            target: skin_target(1032, "Dynasty Ahri"),
            kinds: WatchKind::All,
        });
        assert!(doc.remove("111", 99).is_none(), "unknown id");
        assert!(doc.remove("111", 2).is_none(), "another user's watch is not removable");
        assert!(doc.remove("222", 2).is_some(), "a user removes their own watch");
        assert!(doc.remove_all_of("222").is_empty(), "already gone");
        assert!(doc.remove("111", 1).is_some());
        assert!(doc.subs.is_empty());
    }

    #[test]
    fn skin_matches_by_id_and_by_name_champion_by_join() {
        let index = index();
        assert!(target_matches(&skin_target(1031, "Foxfire Ahri"), &Subject::Item(1031), &index));
        assert!(!target_matches(&skin_target(1031, "Foxfire Ahri"), &Subject::Item(1032), &index));
        assert!(target_matches(
            &skin_target(1031, "foxfire  ahri!"),
            &Subject::Name("Foxfire Ahri".to_owned()),
            &index
        ));
        let champion = WatchTarget::Champion { champion_id: 103, champion: "Ahri".to_owned() };
        assert!(target_matches(&champion, &Subject::Item(1032), &index));
        // Base-champion items (or unknown items) join to nothing.
        assert!(!target_matches(&champion, &Subject::Item(103), &index));
        assert!(!target_matches(&champion, &Subject::Name("Foxfire Ahri".to_owned()), &index));
    }

    #[test]
    fn entry_subject_keeps_the_raw_display_name() {
        let index = index();
        let joined = MythicEntry {
            entry_id: Some("1031".to_owned()),
            name: Some("Foxfire Ahri".to_owned()),
            mythic_price: Some(100),
        };
        assert_eq!(entry_subject(&joined, &index), Subject::Item(1031));
        // Unresolvable ids fall back to the RAW display name - normalization
        // and the loose-key comparison happen in `target_matches`.
        let foreign = MythicEntry {
            entry_id: Some("999999".to_owned()),
            name: Some("Foxfire Ahri".to_owned()),
            mythic_price: None,
        };
        assert_eq!(entry_subject(&foreign, &index), Subject::Name("Foxfire Ahri".to_owned()));
        let nameless = MythicEntry { entry_id: None, name: None, mythic_price: None };
        assert_eq!(entry_subject(&nameless, &index), Subject::Name(String::new()));
    }

    /// The live shoppefront shape: GUID entry ids and fulfillment names
    /// with decorative parenthesized suffixes ("... (BADDEST)"). The name
    /// fallback must still match the catalog name the user subscribed by -
    /// and must not match the synthetic price-entry name.
    #[test]
    fn name_fallback_survives_parenthesized_shoppefront_suffixes() {
        let index = index();
        let watch = skin_target(1031, "K/DA ALL OUT Akali");
        let baddest = Subject::Name("K/DA ALL OUT Akali (BADDEST)".to_owned());
        assert!(target_matches(&watch, &baddest, &index), "loose key must match");
        let exact = Subject::Name("K/DA ALL OUT Akali".to_owned());
        assert!(target_matches(&watch, &exact, &index));
        // The StoreEntry.name fallback ("Script generated price - ME: 35")
        // is decorative junk and must never match a real skin watch.
        let junk = Subject::Name("Script generated price - ME: 35".to_owned());
        assert!(!target_matches(&watch, &junk, &index));
        // A different skin with a suffix must not cross-match.
        let other = Subject::Name("Dynasty Ahri (SPECIAL)".to_owned());
        assert!(!target_matches(&watch, &other, &index));
    }

    #[test]
    fn search_prefers_exact_matches_and_caps_results() {
        let index = index();
        let hits = search_skins("ahri", &index);
        assert_eq!(hits.len(), 2, "both Ahri skins are substring hits");
        let exact = search_skins("foxfire ahri", &index);
        assert_eq!(exact.len(), 1);
        assert_eq!(exact.first().expect("hit").item_id, 1031);
        assert!(search_skins("", &index).is_empty());
        assert!(search_skins("yuumi", &index).is_empty());

        let champions =
            champion_map([(103, Some("Ahri".to_owned())), (2, Some("Olaf".to_owned()))]);
        let hits = search_champions("aHRi", champions.iter());
        assert_eq!(hits.first().expect("hit").champion_id, 103);
        assert!(search_champions("zzz", champions.iter()).is_empty());
    }

    /// The candidate list is capped: a broad query over a big catalog
    /// replies with a bounded list, exact matches still sorted first.
    #[test]
    fn search_results_are_capped_for_huge_catalogs() {
        let mut catalog: Vec<CatalogItem> = (0..25)
            .map(|n| CatalogItem {
                item_id: 5000 + n,
                inventory_type: Some("CHAMPION_SKIN".to_owned()),
                prices: Vec::new(),
                localizations: BTreeMap::from([(
                    "en_US".to_owned(),
                    LocalizedText { name: Some(format!("Testee Skin {n:02}")) },
                )]),
                item_requirements: vec![ItemRef {
                    inventory_type: Some("CHAMPION".to_owned()),
                    item_id: Some(500),
                }],
            })
            .collect();
        // An exact match buried among the partials must surface first.
        catalog.push(CatalogItem {
            item_id: 4999,
            inventory_type: Some("CHAMPION_SKIN".to_owned()),
            prices: Vec::new(),
            localizations: BTreeMap::from([(
                "en_US".to_owned(),
                LocalizedText { name: Some("Testee".to_owned()) },
            )]),
            item_requirements: vec![ItemRef {
                inventory_type: Some("CHAMPION".to_owned()),
                item_id: Some(500),
            }],
        });
        let index = NameIndex::new(catalog, champion_map([(500, Some("Testee".to_owned()))]));
        let hits = search_skins("testee", &index);
        assert_eq!(hits.len(), super::SEARCH_RESULT_CAP, "capped at the limit");
        assert_eq!(
            hits.first().expect("hit").skin,
            "Testee",
            "the exact match sorts first despite the suffix numbering"
        );
    }

    #[test]
    fn notification_renders_sections_and_dedupes_users() {
        let doc = doc_of(vec![
            watch(1, skin_target(1031, "Foxfire Ahri"), WatchKind::Sale),
            Watch {
                id: 2,
                user_id: "222".to_owned(),
                target: skin_target(1031, "Foxfire Ahri"),
                kinds: WatchKind::Sale,
            },
        ]);
        let delta = StoreDelta {
            sales: vec![super::super::lcu::Sale {
                id: 7,
                item: ItemRef {
                    inventory_type: Some("CHAMPION_SKIN".to_owned()),
                    item_id: Some(1031),
                },
                sale: Default::default(),
            }],
            ..StoreDelta::default()
        };
        let notification = build_notification(&doc, &delta, &index()).expect("should notify");
        assert_eq!(notification.watchers, 2);
        assert!(notification.tags.contains("<@111>"));
        assert!(notification.tags.contains("<@222>"));
        assert!(notification.text.contains("Foxfire Ahri"));
    }

    #[test]
    fn non_matching_and_wrong_kind_deltas_stay_silent() {
        let doc = doc_of(vec![watch(1, skin_target(1031, "Foxfire Ahri"), WatchKind::Mythic)]);
        let sale_delta = StoreDelta {
            sales: vec![super::super::lcu::Sale {
                id: 7,
                item: ItemRef {
                    inventory_type: Some("CHAMPION_SKIN".to_owned()),
                    item_id: Some(1031),
                },
                sale: Default::default(),
            }],
            ..StoreDelta::default()
        };
        assert!(build_notification(&doc, &sale_delta, &index()).is_none());
        assert!(build_notification(&WatchDoc::default(), &sale_delta, &index()).is_none());
    }

    /// A tampered watch doc (non-numeric user id) must never inject mention
    /// syntax into the ping content; valid watches keep working.
    #[test]
    fn non_numeric_user_ids_never_reach_the_tag_content() {
        let doc = doc_of(vec![
            Watch {
                id: 1,
                user_id: "everyone".to_owned(),
                target: skin_target(1031, "Foxfire Ahri"),
                kinds: WatchKind::All,
            },
            Watch {
                id: 2,
                user_id: "222".to_owned(),
                target: skin_target(1031, "Foxfire Ahri"),
                kinds: WatchKind::All,
            },
        ]);
        let delta = sale_delta_for(1031);
        let notification = build_notification(&doc, &delta, &index()).expect("valid watch fires");
        assert!(notification.tags.contains("<@222>"));
        assert!(!notification.tags.contains("everyone"), "tags: {}", notification.tags);
        assert_eq!(notification.watchers, 1);
    }

    /// The "+N more" budget path: many watchers must keep the content under
    /// the Discord limit and record what was cut.
    #[test]
    fn tag_content_stays_within_the_budget() {
        let users: Vec<String> = (0..300).map(|n| format!("1{n:017}")).collect();
        let tags = super::tag_content(&users);
        assert!(tags.len() <= super::TAG_BUDGET_CHARS + 32, "content over budget: {}", tags.len());
        assert!(tags.contains('+'), "hidden count expected: {tags}");
        assert!(tags.ends_with("more"));
        assert!(tags.starts_with("<@1"), "rendered tags are well-formed mentions");
    }

    fn sale_delta_for(item_id: u64) -> StoreDelta {
        StoreDelta {
            sales: vec![super::super::lcu::Sale {
                id: 7,
                item: ItemRef {
                    inventory_type: Some("CHAMPION_SKIN".to_owned()),
                    item_id: Some(item_id),
                },
                sale: Default::default(),
            }],
            ..StoreDelta::default()
        }
    }
}
