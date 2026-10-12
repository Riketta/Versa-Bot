use std::collections::HashMap;

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
    pub channel_id: Option<String>,
    #[serde(default)]
    pub creator_id: Option<String>,
    #[serde(default)]
    pub creator_name: Option<String>,
    #[serde(default)]
    pub max_uses: Option<u64>,
    #[serde(default)]
    pub temporary: Option<bool>,
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
            channel_id: invite.channel_id.map(|channel| channel.get().to_string()),
            creator_id: invite.creator_id.map(|user| user.get().to_string()),
            creator_name: invite.creator_name.clone(),
            max_uses: invite.max_uses,
            temporary: None,
            last_seen_uses: None,
            deleted: false,
        }
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
/// candidates once they carry uses. Multiple candidates (offline catch-up,
/// concurrent joins) and none stay unattributed - the diff refuses to
/// guess.
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
}
