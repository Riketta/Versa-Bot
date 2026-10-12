use std::collections::HashMap;

use crate::kernel::models::InvitePayload;
use crate::kernel::spi_ports::DirectoryInvite;

/// Guild storage key (inside the tracker namespace) holding the serialized
/// invite map - one JSON object keyed by invite code.
pub const INVITES_KEY: &str = "invites";

/// One observed invite, persisted per guild. Identifiers are stored as
/// strings - snowflakes exceed JSON number precision. The document is
/// mutable tracker state (unlike chat records): `last_seen_uses` moves with
/// every observation, and `deleted` invites stay recorded so their audit
/// trail and creator attribution survive the delete event.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InviteDoc {
    pub code: String,
    #[serde(default)]
    pub creator_id: Option<String>,
    #[serde(default)]
    pub creator_name: Option<String>,
    #[serde(default)]
    pub max_uses: Option<u64>,
    /// Last observed cumulative use count - the join-attribution baseline.
    /// `None` = never observed (created outside an observed lifecycle).
    #[serde(default)]
    pub last_seen_uses: Option<u64>,
    #[serde(default)]
    pub deleted: bool,
}

impl InviteDoc {
    /// A fresh record from a directory listing - only what the listing
    /// itself knows (the creator may be enriched later, `uses` stays
    /// unobserved until the first diff).
    #[must_use]
    pub fn from_directory(invite: &DirectoryInvite) -> Self {
        Self {
            code: invite.code.clone(),
            creator_id: invite.creator_id.map(|user| user.get().to_string()),
            creator_name: invite.creator_name.clone(),
            max_uses: invite.max_uses,
            last_seen_uses: None,
            deleted: false,
        }
    }

    /// A fresh record from a create event: the platform reports a zeroed
    /// use counter - the baseline the first attribution diff diffs from.
    #[must_use]
    pub fn from_create(invite: &InvitePayload) -> Self {
        Self {
            code: invite.code.clone(),
            creator_id: invite.creator_id.map(|user| user.get().to_string()),
            creator_name: invite.creator_name.clone(),
            max_uses: invite.max_uses,
            last_seen_uses: Some(0),
            deleted: false,
        }
    }

    /// `true` when the doc describes an invite one use short of its limit:
    /// the next join through it consumes it, which also deletes it from
    /// the directory - the vanished-code attribution signal.
    #[must_use]
    pub fn one_use_from_limit(&self) -> bool {
        matches!(
            (self.last_seen_uses, self.max_uses),
            (Some(seen), Some(max)) if max >= 1 && seen == max - 1
        )
    }

    /// Creator as the audit line renders it: name when known, else the
    /// bare id, else "unknown creator".
    #[must_use]
    pub fn creator_display(&self) -> String {
        if let Some(name) = &self.creator_name {
            return name.clone();
        }
        if let Some(id) = &self.creator_id {
            return format!("user {id}");
        }
        "unknown creator".to_owned()
    }
}

/// The invite a joining member came through, per the uses-count diff: the
/// one code whose cumulative uses grew by exactly one since the last
/// observation. Codes unknown to storage (created outside an observed
/// lifecycle - before the feature or while the bot was offline) count as
/// candidates once they carry uses, and a stored code that vanished from
/// the listing while one use short of its limit was just consumed by this
/// join (the platform deletes such invites on the spot). Multiple
/// candidates (offline catch-up, concurrent joins) and none stay
/// unattributed - the diff refuses to guess.
#[must_use]
pub fn attribute_join(
    stored: &HashMap<String, InviteDoc>,
    fetched: &[DirectoryInvite],
) -> Option<String> {
    let mut candidates: Vec<&str> = Vec::new();
    for invite in fetched {
        let Some(uses) = invite.uses else { continue };
        if uses == 0 {
            continue;
        }
        match stored.get(&invite.code).and_then(|doc| doc.last_seen_uses) {
            // Observed growth by exactly one - this code was just used.
            Some(seen) if uses == seen + 1 => candidates.push(&invite.code),
            // Observed and unchanged, or jumped by more than one (unobserved
            // offline joins) - the latter must NOT attribute, or every join
            // after downtime would pin to it.
            Some(_) => {}
            // Unknown to storage: created outside an observed lifecycle.
            None => candidates.push(&invite.code),
        }
    }
    // A consumed code is absent from the listing, but its doc survives:
    // observed one use short of the limit right as a join lands means the
    // join consumed it.
    for (code, doc) in stored {
        if !fetched.iter().any(|invite| invite.code == *code) && doc.one_use_from_limit() {
            candidates.push(code);
        }
    }
    if candidates.len() == 1 { candidates.pop().map(str::to_owned) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::models::UserId;

    fn directory(code: &str, uses: u64) -> DirectoryInvite {
        DirectoryInvite {
            code: code.to_owned(),
            channel_id: None,
            creator_id: Some(UserId(7)),
            creator_name: None,
            uses: Some(uses),
            max_uses: None,
        }
    }

    fn observed(code: &str, uses: u64) -> (String, InviteDoc) {
        let mut doc = InviteDoc::from_directory(&directory(code, uses));
        doc.last_seen_uses = Some(uses);
        (code.to_owned(), doc)
    }

    #[test]
    fn one_code_growing_by_one_attributes() {
        let stored = HashMap::from([observed("abc", 4), observed("old", 9)]);
        let fetched = vec![directory("abc", 5), directory("old", 9)];

        assert_eq!(attribute_join(&stored, &fetched).as_deref(), Some("abc"));
    }

    #[test]
    fn unknown_code_with_uses_attributes() {
        let stored = HashMap::from([observed("old", 9)]);
        let fetched = vec![directory("brand-new", 1), directory("old", 9)];

        assert_eq!(attribute_join(&stored, &fetched).as_deref(), Some("brand-new"));
    }

    #[test]
    fn jumps_of_more_than_one_stay_unattributed() {
        // The bot was offline: two joins happened through "abc" unseen.
        let stored = HashMap::from([observed("abc", 4)]);
        let fetched = vec![directory("abc", 6)];

        assert_eq!(attribute_join(&stored, &fetched), None);
    }

    #[test]
    fn two_growing_codes_stay_unattributed() {
        let stored = HashMap::from([observed("a", 1), observed("b", 1)]);
        let fetched = vec![directory("a", 2), directory("b", 2)];

        assert_eq!(attribute_join(&stored, &fetched), None);
    }

    #[test]
    fn no_growth_and_unchanged_use_never_attributes() {
        let stored = HashMap::from([observed("a", 1)]);
        assert_eq!(attribute_join(&stored, &[directory("a", 1)]), None);
        assert_eq!(attribute_join(&stored, &[directory("a", 0)]), None);
        assert_eq!(attribute_join(&stored, &[]), None);
    }

    #[test]
    fn unobserved_uses_in_the_directory_are_skipped() {
        let mut unknown = directory("x", 3);
        unknown.uses = None;

        assert_eq!(attribute_join(&HashMap::new(), &[unknown]), None);
    }

    #[test]
    fn consumed_vanished_single_use_attributes() {
        // The join consumed the last permitted use; the platform deleted
        // the invite, so the listing no longer contains it.
        let mut single = observed("single", 0);
        single.1.max_uses = Some(1);
        let stored = HashMap::from([single, observed("old", 9)]);
        let fetched = vec![directory("old", 9)];

        assert_eq!(attribute_join(&stored, &fetched).as_deref(), Some("single"));
    }

    #[test]
    fn vanished_short_of_its_limit_never_attributes() {
        // Gone because it was revoked or expired by age - no consumption
        // signal, no attribution.
        let mut partial = observed("partial", 2);
        partial.1.max_uses = Some(10);
        let stored = HashMap::from([partial]);

        assert_eq!(attribute_join(&stored, &[]), None);
    }

    #[test]
    fn vanished_and_growing_codes_stay_unattributed() {
        let mut single = observed("single", 0);
        single.1.max_uses = Some(1);
        let stored = HashMap::from([single, observed("live", 4)]);
        let fetched = vec![directory("live", 5)];

        assert_eq!(attribute_join(&stored, &fetched), None);
    }

    #[test]
    fn two_vanished_candidates_stay_unattributed() {
        let mut first = observed("a", 0);
        first.1.max_uses = Some(1);
        let mut second = observed("b", 3);
        second.1.max_uses = Some(4);
        let stored = HashMap::from([first, second]);

        assert_eq!(attribute_join(&stored, &[]), None);
    }

    #[test]
    fn unlimited_vanished_code_is_not_a_consumption_candidate() {
        // max_uses 0 = unlimited: using it never deletes it, so its
        // absence means revocation, not consumption.
        let mut unlimited = observed("unl", 7);
        unlimited.1.max_uses = Some(0);
        let stored = HashMap::from([unlimited]);

        assert_eq!(attribute_join(&stored, &[]), None);
    }
}
