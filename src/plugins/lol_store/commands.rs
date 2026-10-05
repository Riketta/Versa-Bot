//! The plugin's slash commands. Config commands run inside the pipeline and
//! use the event-scoped guild storage; status/dump commands read the
//! engine's in-memory state, which is why they hold an `Arc` to it.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;

use crate::common::command_reply;
use crate::kernel::{
    models::{Embed, OutboundMessage, RequestContext},
    plugin_ports::{CommandArgs, CommandHandler},
    services::KernelServices,
};

use super::engine::{CONFIG_KEY, GuildConfig, NAMESPACE, StoreEngine};
use super::watch::{
    ChampionHit, SkinHit, WATCH_KEY, WatchDoc, WatchKind, WatchTarget, normalize_name, query_id,
};

/// Serializes the guild-config read-modify-write across ALL config commands
/// (and guilds - admin-frequency operations, contention is negligible).
/// Without it, two concurrent commands can interleave and drop one field's
/// change: last document write wins.
static CONFIG_WRITE: AsyncMutex<()> = AsyncMutex::const_new(());

/// Read-modify-write of the guild config document, so enable/disable and
/// assign/unassign never clobber each other's fields (serialized by
/// [`CONFIG_WRITE`]). A doc that fails to deserialize is logged and treated
/// as defaults for this operation - the corruption predates the command.
async fn update_config(
    services: &KernelServices,
    change: impl FnOnce(&mut GuildConfig),
) -> anyhow::Result<GuildConfig> {
    let _guard = CONFIG_WRITE.lock().await;
    let storage = services
        .guild_storage
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("guild storage unavailable outside a guild"))?;
    let mut config = match storage.get(NAMESPACE, CONFIG_KEY).await? {
        Some(raw) => match serde_json::from_value::<GuildConfig>(raw) {
            Ok(config) => config,
            Err(err) => {
                tracing::warn!(%err, "store tracker config unreadable - resetting to defaults");
                GuildConfig::default()
            }
        },
        None => GuildConfig::default(),
    };
    change(&mut config);
    storage.set(NAMESPACE, CONFIG_KEY, serde_json::to_value(&config)?).await?;
    Ok(config)
}

async fn reply_guild_only(services: &KernelServices) -> anyhow::Result<()> {
    services.chat_output.send(command_reply("This command only works inside a server.")).await?;
    Ok(())
}

/// Storage key where an unreadable watch document is preserved before the
/// first overwrite - the fresh doc repairs the commands, the copy keeps the
/// corruption recoverable.
pub(crate) const WATCH_RECOVERY_KEY: &str = "subscriptions.unreadable";

/// `/lol_store_enable`: guild-level switch (admin).
pub struct EnableHandler;

#[async_trait]
impl CommandHandler for EnableHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        if services.guild_storage.is_none() {
            return reply_guild_only(services).await;
        }
        let config = update_config(services, |config| config.enabled = true).await?;
        services
            .chat_output
            .send(command_reply(match config.channel_id {
                Some(_) => {
                    "LoL store tracking enabled. Updates will be posted to the assigned channel."
                }
                None => {
                    "LoL store tracking enabled. Assign an announcement channel with \
                     `/lol_store_assign`."
                }
            }))
            .await?;
        Ok(())
    }
}

/// `/lol_store_disable`: guild-level switch (admin).
pub struct DisableHandler;

#[async_trait]
impl CommandHandler for DisableHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        if services.guild_storage.is_none() {
            return reply_guild_only(services).await;
        }
        update_config(services, |config| config.enabled = false).await?;
        services
            .chat_output
            .send(command_reply(
                "LoL store tracking disabled. The assigned channel is kept for re-enabling.",
            ))
            .await?;
        Ok(())
    }
}

/// `/lol_store_assign`: the current channel becomes the announcement channel
/// (moderator). Assigning does not enable - the two switches stay orthogonal.
pub struct AssignHandler;

#[async_trait]
impl CommandHandler for AssignHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        if services.guild_storage.is_none() {
            return reply_guild_only(services).await;
        }
        let channel = event.origin.channel_id.get().to_string();
        update_config(services, |config| config.channel_id = Some(channel)).await?;
        services
            .chat_output
            .send(command_reply(
                "LoL store events will be posted in this channel (tracking must also be \
                 enabled with `/lol_store_enable`).",
            ))
            .await?;
        Ok(())
    }
}

/// `/lol_store_unassign`: clears the announcement channel (moderator).
pub struct UnassignHandler;

#[async_trait]
impl CommandHandler for UnassignHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        if services.guild_storage.is_none() {
            return reply_guild_only(services).await;
        }
        update_config(services, |config| config.channel_id = None).await?;
        services
            .chat_output
            .send(command_reply("LoL store events will no longer be posted in this guild."))
            .await?;
        Ok(())
    }
}

/// `/lol_store_role`: binds the role tagged on store announcements - the
/// per-guild "subscription": members join the role to opt into pings
/// (moderator). Run with the role argument to set, without it to clear.
pub struct RoleHandler;

#[async_trait]
impl CommandHandler for RoleHandler {
    async fn invoke(
        &self,
        _event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        if services.guild_storage.is_none() {
            return reply_guild_only(services).await;
        }
        let role = args.get("role").map(str::to_owned);
        match role {
            Some(role) => {
                if role.parse::<u64>().is_err() {
                    services
                        .chat_output
                        .send(command_reply(
                            "The role argument must be a role - pick one from the list.",
                        ))
                        .await?;
                    return Ok(());
                }
                update_config(services, |config| config.role_id = Some(role)).await?;
                services
                    .chat_output
                    .send(command_reply(
                        "Store announcements will now tag that role (members opt in by \
                         joining it). Make sure the role is mentionable.",
                    ))
                    .await?;
            }
            None => {
                update_config(services, |config| config.role_id = None).await?;
                services
                    .chat_output
                    .send(command_reply("Store announcements will no longer tag a role."))
                    .await?;
            }
        }
        Ok(())
    }
}

/// `/lol_client_status`: watcher health, counts and settings (moderator,
/// ephemeral).
pub struct ClientStatusHandler<B: crate::kernel::plugin_ports::EventBusPort> {
    pub engine: Arc<StoreEngine<B>>,
}

#[async_trait]
impl<B: crate::kernel::plugin_ports::EventBusPort> CommandHandler for ClientStatusHandler<B> {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        // Same self-guard as every other handler: defense in depth next to
        // the descriptor's `guild_only`.
        if services.guild_storage.is_none() {
            return reply_guild_only(services).await;
        }
        services.chat_output.send(command_reply(self.engine.status_text())).await?;
        Ok(())
    }
}

/// Serializes watch-document read-modify-writes (subscribe/unsubscribe).
/// User-frequency operations, but the same last-write-wins hazard applies.
static WATCH_WRITE: AsyncMutex<()> = AsyncMutex::const_new(());

/// Loads the guild's watch document alongside its raw form when that form
/// fails to deserialize: mutations move the unreadable raw aside (see
/// [`quarantine_unreadable`]) instead of silently destroying it, while
/// read-only paths never persist anything.
async fn load_watch_doc(
    storage: &dyn crate::kernel::spi_ports::GuildStorage,
) -> anyhow::Result<(WatchDoc, Option<serde_json::Value>)> {
    match storage.get(NAMESPACE, WATCH_KEY).await? {
        Some(raw) => match serde_json::from_value::<WatchDoc>(raw.clone()) {
            Ok(doc) => Ok((doc, None)),
            Err(err) => {
                tracing::warn!(%err, "watch doc unreadable - treating as empty");
                Ok((WatchDoc::default(), Some(raw)))
            }
        },
        None => Ok((WatchDoc::default(), None)),
    }
}

/// Preserves an unreadable watch document under a recovery key before the
/// caller's fresh document overwrites it. Idempotent: a newer unreadable
/// doc replaces the stashed one.
async fn quarantine_unreadable(
    storage: &dyn crate::kernel::spi_ports::GuildStorage,
    raw: &serde_json::Value,
) -> anyhow::Result<()> {
    tracing::warn!("moving the unreadable watch doc aside before overwrite");
    storage.set(NAMESPACE, WATCH_RECOVERY_KEY, raw.clone()).await?;
    Ok(())
}

/// One `/lol_store_watch` name-resolution outcome.
enum Resolved {
    Hit(WatchTarget),
    Nothing,
    /// Suggestion lines; `by_id` marks lists whose rows carry an
    /// `(id N)` suffix - the reply then offers picking one with it.
    Candidates {
        lines: Vec<String>,
        by_id: bool,
    },
}

/// The validated `target` dropdown value.
enum TargetKind {
    Skin,
    Champion,
}

/// Distinct normalized keys in first-appearance order, each represented by
/// the lowest-id hit among its duplicates. Live LoL data can list one name
/// under several ids (Evelynn ships twice in the champion table); without
/// the collapse the resolver dead-ends: it asks for an exact name that
/// cannot disambiguate identical rows.
fn collapse<'a, T>(
    hits: impl IntoIterator<Item = &'a T>,
    key: impl Fn(&T) -> String,
    id: impl Fn(&T) -> u64,
) -> Vec<&'a T> {
    let mut best: Vec<(String, &T)> = Vec::new();
    for hit in hits {
        let key = key(hit);
        match best.iter_mut().find(|(known, _)| *known == key) {
            Some(entry) => {
                if id(entry.1) > id(hit) {
                    entry.1 = hit;
                }
            }
            None => best.push((key, hit)),
        }
    }
    best.into_iter().map(|(_, hit)| hit).collect()
}

/// Normalized (champion id, skin name) identity: live and Classic
/// variants share display names but not champion ids - only rows under
/// ONE champion id are true duplicates.
fn skin_key(hit: &SkinHit) -> String {
    format!("{}|{}", hit.champion_id, normalize_name(&hit.skin))
}

/// Skin candidate lines. Live and Classic variants can render one
/// identical line - such collisions get item-id suffixes and the reply
/// offers picking by id.
fn skin_candidates(hits: &[&SkinHit]) -> Resolved {
    let line = |hit: &SkinHit| format!("{} - {}", hit.champion, hit.skin);
    let distinct = hits.iter().map(|hit| line(hit)).collect::<HashSet<_>>().len();
    if distinct < hits.len() {
        candidate_lines(
            hits.iter().map(|hit| format!("{} (item {})", line(hit), hit.item_id)).collect(),
            true,
        )
    } else {
        candidate_lines(hits.iter().map(|hit| line(hit)).collect(), false)
    }
}

fn resolve_skin(query: &str, hits: &[SkinHit]) -> Resolved {
    // A bare number (or a pasted `... (item N)` line) selects by item id
    // directly - the only way to pick between same-named variant skins.
    if let Some(id) = query_id(query) {
        if let Some(hit) = hits.iter().find(|hit| hit.item_id == id) {
            return Resolved::Hit(WatchTarget::Skin {
                item_id: hit.item_id,
                champion: hit.champion.clone(),
                skin: hit.skin.clone(),
            });
        }
    }
    let needle = normalize_name(query);
    let exacts: Vec<&SkinHit> =
        hits.iter().filter(|hit| normalize_name(&hit.skin) == needle).collect();
    let collapsed = collapse(exacts.clone(), skin_key, |hit| hit.item_id);
    if exacts.len() > collapsed.len() {
        tracing::debug!(
            item_ids = %exacts
                .iter()
                .map(|hit| hit.item_id.to_string())
                .collect::<Vec<_>>()
                .join(","),
            "skin name spans several catalog item ids - watch bound to the lowest"
        );
    }
    match collapsed.as_slice() {
        [hit] => Resolved::Hit(WatchTarget::Skin {
            item_id: hit.item_id,
            champion: hit.champion.clone(),
            skin: hit.skin.clone(),
        }),
        [] => match collapse(hits.iter(), skin_key, |hit| hit.item_id).as_slice() {
            [] => Resolved::Nothing,
            [hit] => Resolved::Hit(WatchTarget::Skin {
                item_id: hit.item_id,
                champion: hit.champion.clone(),
                skin: hit.skin.clone(),
            }),
            many => skin_candidates(many),
        },
        many => skin_candidates(many),
    }
}

/// Breadcrumb payload: the competing ids, joined - shapes, not contents.
fn ids_of<'a>(hits: impl IntoIterator<Item = &'a ChampionHit>) -> String {
    hits.into_iter().map(|hit| hit.champion_id.to_string()).collect::<Vec<_>>().join(",")
}

fn resolve_champion(query: &str, hits: &[ChampionHit], store_backed: &HashSet<u64>) -> Resolved {
    // A bare number (or a pasted `... (id N)` line) selects by champion
    // id directly - the only way to pick between same-named rows that
    // are both real (live + Classic variants share one display name).
    if let Some(id) = query_id(query) {
        if let Some(hit) = hits.iter().find(|hit| hit.champion_id == id) {
            return Resolved::Hit(WatchTarget::Champion {
                champion_id: hit.champion_id,
                champion: hit.champion.clone(),
            });
        }
    }
    let needle = normalize_name(query);
    let exacts: Vec<&ChampionHit> =
        hits.iter().filter(|hit| normalize_name(&hit.champion) == needle).collect();
    // The table lists some champions under several ids (live + Classic
    // variants share one display name). Only ids the store catalog can
    // reach can ever fire, so bind to a backed id when exactly one
    // exists; several backed ids are genuinely different targets and
    // stay a disambiguation list.
    let backed: Vec<&ChampionHit> =
        exacts.iter().filter(|hit| store_backed.contains(&hit.champion_id)).copied().collect();
    match backed.as_slice() {
        [hit] => {
            if exacts.len() > 1 {
                tracing::debug!(
                    champion_ids = %ids_of(exacts.iter().copied()),
                    bound = hit.champion_id,
                    "champion name spans several table ids - watch bound to the store-backed id"
                );
            }
            Resolved::Hit(WatchTarget::Champion {
                champion_id: hit.champion_id,
                champion: hit.champion.clone(),
            })
        }
        // No store-backed id (catalog empty or not yet polled): lowest
        // id is the release-ordered canonical row.
        [] => {
            if let Some(hit) = exacts.iter().min_by_key(|hit| hit.champion_id) {
                if exacts.len() > 1 {
                    tracing::debug!(
                        champion_ids = %ids_of(exacts.iter().copied()),
                        bound = hit.champion_id,
                        "champion name spans several table ids, none store-backed - \
                         watch bound to the lowest"
                    );
                }
                return Resolved::Hit(WatchTarget::Champion {
                    champion_id: hit.champion_id,
                    champion: hit.champion.clone(),
                });
            }
            let by_name = |hit: &ChampionHit| normalize_name(&hit.champion);
            match collapse(hits.iter(), by_name, |hit| hit.champion_id).as_slice() {
                [] => Resolved::Nothing,
                [hit] => Resolved::Hit(WatchTarget::Champion {
                    champion_id: hit.champion_id,
                    champion: hit.champion.clone(),
                }),
                many => {
                    candidate_lines(many.iter().map(|hit| hit.champion.clone()).collect(), false)
                }
            }
        }
        // Reachable only with two or more backed ids: genuinely different
        // targets sharing one display name.
        many => {
            tracing::debug!(
                champion_ids = %ids_of(exacts.iter().copied()),
                "champion name spans several store-backed ids - asking the user"
            );
            candidate_lines(
                many.iter()
                    .map(|hit| format!("{} (id {})", hit.champion, hit.champion_id))
                    .collect(),
                true,
            )
        }
    }
}

/// Suggestion lines are backticked: the reply renders as markdown, and the
/// quotes keep stray whitespace in live names visible instead of silent.
fn candidate_lines(lines: Vec<String>, by_id: bool) -> Resolved {
    let lines = lines.into_iter().map(|line| format!("`{line}`")).collect();
    Resolved::Candidates { lines, by_id }
}

/// `/lol_store_watch`: subscribe to a skin or a champion's whole skin line
/// (user, ephemeral). Watch matching runs on the full delta, independent
/// of the guild's announce flags.
pub struct WatchHandler<B: crate::kernel::plugin_ports::EventBusPort> {
    pub engine: Arc<StoreEngine<B>>,
}

#[async_trait]
impl<B: crate::kernel::plugin_ports::EventBusPort> CommandHandler for WatchHandler<B> {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = services.guild_storage.as_ref() else {
            return reply_guild_only(services).await;
        };
        let user_id = event.origin.user_id.get().to_string();

        // Validate all arguments before touching the store snapshot - an
        // invalid target must not pay for the name search.
        let kinds = match args.get("kinds") {
            Some(raw) => match WatchKind::parse(raw) {
                Some(kinds) => kinds,
                None => {
                    services
                        .chat_output
                        .send(command_reply("kinds must be one of: sale, mythic, release, all."))
                        .await?;
                    return Ok(());
                }
            },
            None => WatchKind::All,
        };
        let target_kind = match args.get("target") {
            Some("skin") => TargetKind::Skin,
            Some("champion") => TargetKind::Champion,
            _ => {
                services
                    .chat_output
                    .send(command_reply("target must be `skin` or `champion`."))
                    .await?;
                return Ok(());
            }
        };
        let Some(query) = args.get("name").map(str::trim).filter(|name| !name.is_empty()) else {
            services
                .chat_output
                .send(command_reply("Give a name to watch - a skin or a champion."))
                .await?;
            return Ok(());
        };

        // Resolve the name against the last good store snapshot.
        let Some(search) = self.engine.search_store(query).await else {
            services
                .chat_output
                .send(command_reply(
                    "No store data yet - the League client has not been reachable since \
                     startup (see `/lol_client_status`).",
                ))
                .await?;
            return Ok(());
        };
        let resolved = match target_kind {
            TargetKind::Skin => resolve_skin(query, &search.skins),
            TargetKind::Champion => {
                resolve_champion(query, &search.champions, &search.store_backed)
            }
        };
        let target = match resolved {
            Resolved::Hit(target) => target,
            Resolved::Nothing => {
                services
                    .chat_output
                    .send(command_reply(format!(
                        "Nothing matching \"{query}\" in the store catalog - check the \
                         spelling, or try again once the League client is online."
                    )))
                    .await?;
                return Ok(());
            }
            Resolved::Candidates { lines, by_id } => {
                let hint = if by_id { "the exact name or a listed id" } else { "the exact name" };
                services
                    .chat_output
                    .send(command_reply(format!(
                        "Several matches - run `/lol_store_watch` again with {hint}:\n{}",
                        lines.join("\n")
                    )))
                    .await?;
                return Ok(());
            }
        };

        // Read-modify-write: caps are enforced between load and save.
        let _guard = WATCH_WRITE.lock().await;
        let (mut doc, unreadable) = load_watch_doc(storage.as_ref()).await?;
        let settings = self.engine.settings();
        let user_cap = usize::try_from(settings.watch_user_cap).unwrap_or(usize::MAX);
        let guild_cap = usize::try_from(settings.watch_guild_cap).unwrap_or(usize::MAX);
        let user_watches = doc.subs.iter().filter(|watch| watch.user_id == user_id).count();
        if user_watches >= user_cap {
            services
                .chat_output
                .send(command_reply(format!(
                    "Watch limit reached ({user_watches}/{} per user). Remove one with \
                     `/lol_store_unwatch` first.",
                    settings.watch_user_cap
                )))
                .await?;
            return Ok(());
        }
        if doc.subs.len() >= guild_cap {
            services
                .chat_output
                .send(command_reply(format!(
                    "This guild reached its watch limit ({}). Ask a moderator to prune \
                     stale watches.",
                    settings.watch_guild_cap
                )))
                .await?;
            return Ok(());
        }
        let id = doc.insert(user_id, target.clone(), kinds);
        if let Some(raw) = &unreadable {
            quarantine_unreadable(storage.as_ref(), raw).await?;
        }
        storage.set(NAMESPACE, WATCH_KEY, serde_json::to_value(&doc)?).await?;
        drop(_guard);

        // "all" is a category bundle - say so; the watchlist keeps the
        // short form.
        let kinds_text = match kinds {
            WatchKind::All => "all categories".to_owned(),
            _ => kinds.label().to_owned(),
        };
        let mut reply_text = format!("Watch #{id} added: {} ({}).", target.label(), kinds_text);
        if let Some(status) = self.engine.watch_status(&target).await {
            for line in status {
                reply_text.push_str(&format!("\n- {line}"));
            }
        }
        services.chat_output.send(command_reply(&reply_text)).await?;
        Ok(())
    }
}

/// `/lol_store_unwatch`: remove one own watch by id, or every watch with
/// `all` (user, ephemeral).
pub struct UnwatchHandler;

#[async_trait]
impl CommandHandler for UnwatchHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = services.guild_storage.as_ref() else {
            return reply_guild_only(services).await;
        };
        let user_id = event.origin.user_id.get().to_string();
        let Some(what) = args.get("what").map(str::trim).filter(|what| !what.is_empty()) else {
            services
                .chat_output
                .send(command_reply(
                    "Give a watch id from `/lol_store_watchlist`, or `all` to clear yours.",
                ))
                .await?;
            return Ok(());
        };

        let _guard = WATCH_WRITE.lock().await;
        let (mut doc, unreadable) = load_watch_doc(storage.as_ref()).await?;
        let (changed, reply_text) = if what.eq_ignore_ascii_case("all") {
            let removed = doc.remove_all_of(&user_id);
            let text = if removed.is_empty() {
                "You have no watches to remove.".to_owned()
            } else {
                let mut lines: Vec<String> = vec![format!("Removed {} watch(es):", removed.len())];
                lines.extend(removed.iter().map(|watch| {
                    format!("- #{} {} ({})", watch.id, watch.target.label(), watch.kinds.label())
                }));
                lines.join("\n")
            };
            (!removed.is_empty(), text)
        } else {
            match what.parse::<u64>() {
                Ok(id) => match doc.remove(&user_id, id) {
                    Some(watch) => {
                        (true, format!("Watch #{id} removed: {}.", watch.target.label()))
                    }
                    None => {
                        (false, format!("No watch #{id} of yours - see `/lol_store_watchlist`."))
                    }
                },
                Err(_) => {
                    (false, "Give a watch id from `/lol_store_watchlist`, or `all`.".to_owned())
                }
            }
        };
        if changed {
            if let Some(raw) = &unreadable {
                quarantine_unreadable(storage.as_ref(), raw).await?;
            }
            storage.set(NAMESPACE, WATCH_KEY, serde_json::to_value(&doc)?).await?;
        }
        drop(_guard);
        services.chat_output.send(command_reply(reply_text)).await?;
        Ok(())
    }
}

/// `/lol_store_watchlist`: the invoking user's own watches (private,
/// ephemeral).
pub struct WatchlistHandler;

#[async_trait]
impl CommandHandler for WatchlistHandler {
    async fn invoke(
        &self,
        event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        let Some(storage) = services.guild_storage.as_ref() else {
            return reply_guild_only(services).await;
        };
        let user_id = event.origin.user_id.get().to_string();
        let (doc, _) = load_watch_doc(storage.as_ref()).await?;
        let mut mine: Vec<_> =
            doc.subs.into_iter().filter(|watch| watch.user_id == user_id).collect();
        mine.sort_by_key(|watch| watch.id);
        let reply_text = if mine.is_empty() {
            "No watches yet - add one with `/lol_store_watch`.".to_owned()
        } else {
            let lines: Vec<String> = mine
                .iter()
                .map(|watch| {
                    format!("#{} {} ({})", watch.id, watch.target.label(), watch.kinds.label())
                })
                .collect();
            format!("Your store watches:\n{}", lines.join("\n"))
        };
        services.chat_output.send(command_reply(reply_text)).await?;
        Ok(())
    }
}

/// `/lol_store_dump`: force-post the latest store update summary into the
/// invoking channel (moderator, public - a dump is meant to be seen). With
/// no update announced yet (fresh launch), the current store is dumped
/// instead; with no snapshot either, an ephemeral placeholder explains it.
pub struct DumpHandler<B: crate::kernel::plugin_ports::EventBusPort> {
    pub engine: Arc<StoreEngine<B>>,
}

#[async_trait]
impl<B: crate::kernel::plugin_ports::EventBusPort> CommandHandler for DumpHandler<B> {
    async fn invoke(
        &self,
        _event: &RequestContext,
        _args: &CommandArgs,
        services: &KernelServices,
    ) -> anyhow::Result<()> {
        // Same self-guard as every other handler: defense in depth next to
        // the descriptor's `guild_only`.
        if services.guild_storage.is_none() {
            return reply_guild_only(services).await;
        }
        let mut pages = self.engine.last_announcement_pages();
        let mut title = "LoL Store - latest update";
        if pages.is_empty() {
            pages = self.engine.current_store_pages().await;
            title = "LoL Store - current state";
        }
        if pages.is_empty() {
            services
                .chat_output
                .send(command_reply(
                    "No store update has been announced yet, and no store snapshot is \
                     available yet (the client may not have been polled).",
                ))
                .await?;
            return Ok(());
        }
        let total = pages.len();
        for (n, page) in pages.iter().enumerate() {
            let embed_title = if total == 1 {
                title.to_owned()
            } else {
                format!("{title} ({}/{})", n + 1, total)
            };
            services
                .chat_output
                .send(OutboundMessage::embed(Embed {
                    title: embed_title,
                    description: page.clone(),
                }))
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::super::watch::{ChampionHit, SkinHit};
    use super::{Resolved, WatchTarget, resolve_champion, resolve_skin};

    fn champion(id: u64, name: &str) -> ChampionHit {
        ChampionHit { champion_id: id, champion: name.to_owned() }
    }

    fn skin(champion_id: u64, item_id: u64, champion: &str, name: &str) -> SkinHit {
        SkinHit { champion_id, item_id, champion: champion.to_owned(), skin: name.to_owned() }
    }

    #[test]
    fn duplicate_champion_rows_resolve_instead_of_dead_ending() {
        let ascending = vec![champion(28, "Evelynn"), champion(60028, "Evelynn")];
        let descending: Vec<_> = ascending.iter().rev().cloned().collect();
        for hits in [ascending, descending] {
            match resolve_champion("Evelynn", &hits, &HashSet::new()) {
                Resolved::Hit(WatchTarget::Champion { champion_id, champion }) => {
                    assert_eq!(champion_id, 28);
                    assert_eq!(champion, "Evelynn");
                }
                _ => panic!("one name under several unbacked ids must resolve, not ask again"),
            }
        }
    }

    #[test]
    fn store_backed_id_wins_over_the_lowest() {
        let hits = vec![champion(28, "Evelynn"), champion(60028, "Evelynn")];
        let backed = HashSet::from([60028]);
        match resolve_champion("Evelynn", &hits, &backed) {
            Resolved::Hit(WatchTarget::Champion { champion_id, champion }) => {
                assert_eq!(champion_id, 60028);
                assert_eq!(champion, "Evelynn");
            }
            _ => panic!("the store-backed id must win over the id-order assumption"),
        }
    }

    #[test]
    fn several_store_backed_ids_stay_candidates_with_labels() {
        let hits = vec![champion(28, "Evelynn"), champion(60028, "Evelynn")];
        let backed = HashSet::from([28, 60028]);
        match resolve_champion("Evelynn", &hits, &backed) {
            Resolved::Candidates { lines, by_id } => {
                assert!(by_id);
                assert_eq!(lines, vec!["`Evelynn (id 28)`", "`Evelynn (id 60028)`"]);
            }
            _ => panic!("several store-backed ids are genuinely different targets"),
        }
    }

    #[test]
    fn a_numeric_name_selects_by_id() {
        let hits = vec![champion(28, "Evelynn"), champion(60028, "Evelynn")];
        match resolve_champion("60028", &hits, &HashSet::new()) {
            Resolved::Hit(WatchTarget::Champion { champion_id, .. }) => {
                assert_eq!(champion_id, 60028);
            }
            _ => panic!("a listed id must be selectable by typing it"),
        }
    }

    #[test]
    fn partial_champion_duplicates_list_one_candidate_per_name() {
        let hits = vec![champion(60028, "Evelynn"), champion(28, "Evelynn"), champion(120, "Kayn")];
        match resolve_champion("yn", &hits, &HashSet::new()) {
            Resolved::Candidates { lines, by_id } => {
                assert!(!by_id);
                assert_eq!(lines, vec!["`Evelynn`", "`Kayn`"]);
            }
            _ => panic!("several distinct names must stay candidates"),
        }
    }

    #[test]
    fn duplicate_skin_rows_collapse_to_the_lowest_item_id() {
        let hits = vec![
            skin(103, 103_002, "Ahri", "Foxfire Ahri"),
            skin(103, 103_001, "Ahri", "Foxfire Ahri"),
        ];
        match resolve_skin("Foxfire Ahri", &hits) {
            Resolved::Hit(WatchTarget::Skin { item_id, champion, skin }) => {
                assert_eq!(item_id, 103_001);
                assert_eq!(champion, "Ahri");
                assert_eq!(skin, "Foxfire Ahri");
            }
            _ => panic!("one distinct skin must resolve, not ask again"),
        }
    }

    #[test]
    fn variant_skins_with_one_name_stay_labeled_candidates() {
        let hits = vec![
            skin(28, 103_001, "Evelynn", "Blood Moon Evelynn"),
            skin(60028, 60_028_001, "Evelynn", "Blood Moon Evelynn"),
        ];
        match resolve_skin("Blood Moon Evelynn", &hits) {
            Resolved::Candidates { lines, by_id } => {
                assert!(by_id);
                assert_eq!(
                    lines,
                    vec![
                        "`Evelynn - Blood Moon Evelynn (item 103001)`",
                        "`Evelynn - Blood Moon Evelynn (item 60028001)`"
                    ]
                );
            }
            _ => panic!("variants must not collapse into a silent pick"),
        }
    }

    #[test]
    fn a_numeric_name_selects_a_skin_by_item_id() {
        let hits = vec![
            skin(28, 103_001, "Evelynn", "Blood Moon Evelynn"),
            skin(60028, 60_028_001, "Evelynn", "Blood Moon Evelynn"),
        ];
        match resolve_skin("60028001", &hits) {
            Resolved::Hit(WatchTarget::Skin { item_id, .. }) => {
                assert_eq!(item_id, 60_028_001);
            }
            _ => panic!("a listed item id must be selectable by typing it"),
        }
    }

    #[test]
    fn a_pasted_candidate_line_selects_by_id() {
        let hits = vec![champion(41, "Gangplank"), champion(60041, "Gangplank")];
        let backed = HashSet::from([41, 60041]);
        for query in ["Gangplank (id 41)", "id 41", "41"] {
            match resolve_champion(query, &hits, &backed) {
                Resolved::Hit(WatchTarget::Champion { champion_id, champion }) => {
                    assert_eq!(champion_id, 41);
                    assert_eq!(champion, "Gangplank");
                }
                _ => panic!("query {query:?} must select the listed id"),
            }
        }
    }

    #[test]
    fn same_skin_name_across_champions_stays_a_candidate_list() {
        let hits = vec![skin(103, 1, "Ahri", "Fire"), skin(1, 2, "Annie", "Fire")];
        match resolve_skin("fire", &hits) {
            Resolved::Candidates { lines, by_id } => {
                assert!(!by_id);
                assert_eq!(lines, vec!["`Ahri - Fire`", "`Annie - Fire`"]);
            }
            _ => panic!("skins of different champions must stay candidates"),
        }
    }

    #[test]
    fn nothing_matches_without_any_hit() {
        assert!(matches!(resolve_champion("missing", &[], &HashSet::new()), Resolved::Nothing));
        assert!(matches!(resolve_skin("missing", &[]), Resolved::Nothing));
    }
}
