//! The plugin's slash commands. Config commands run inside the pipeline and
//! use the event-scoped guild storage; status/dump commands read the
//! engine's in-memory state, which is why they hold an `Arc` to it.

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
    ChampionHit, SkinHit, WATCH_KEY, WatchDoc, WatchKind, WatchTarget, normalize_name,
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
        services.chat_output.send(command_reply(self.engine.status_text())).await?;
        Ok(())
    }
}

/// Serializes watch-document read-modify-writes (subscribe/unsubscribe).
/// User-frequency operations, but the same last-write-wins hazard applies.
static WATCH_WRITE: AsyncMutex<()> = AsyncMutex::const_new(());

/// Loads the guild's watch document; an unreadable doc is logged and treated
/// as empty (ids may restart - mutations only ever save on real change, so
/// a reset is never persisted by a read-only command path).
async fn load_watch_doc(
    storage: &dyn crate::kernel::spi_ports::GuildStorage,
) -> anyhow::Result<WatchDoc> {
    match storage.get(NAMESPACE, WATCH_KEY).await? {
        Some(raw) => match serde_json::from_value::<WatchDoc>(raw) {
            Ok(doc) => Ok(doc),
            Err(err) => {
                tracing::warn!(%err, "watch doc unreadable - treating as empty");
                Ok(WatchDoc::default())
            }
        },
        None => Ok(WatchDoc::default()),
    }
}

/// One `/lol_store_watch` name-resolution outcome.
enum Resolved {
    Hit(WatchTarget),
    Nothing,
    Candidates(Vec<String>),
}

/// The validated `target` dropdown value.
enum TargetKind {
    Skin,
    Champion,
}

fn resolve_skin(query: &str, hits: &[SkinHit]) -> Resolved {
    let needle = normalize_name(query);
    let exacts: Vec<&SkinHit> =
        hits.iter().filter(|hit| normalize_name(&hit.skin) == needle).collect();
    match exacts.len() {
        1 => Resolved::Hit(WatchTarget::Skin {
            item_id: exacts.first().expect("len == 1 checked").item_id,
            champion: exacts.first().expect("len == 1 checked").champion.clone(),
            skin: exacts.first().expect("len == 1 checked").skin.clone(),
        }),
        0 => match hits {
            [] => Resolved::Nothing,
            [hit] => Resolved::Hit(WatchTarget::Skin {
                item_id: hit.item_id,
                champion: hit.champion.clone(),
                skin: hit.skin.clone(),
            }),
            _ => candidate_lines(hits.iter().map(|hit| format!("{} - {}", hit.champion, hit.skin))),
        },
        _ => candidate_lines(exacts.iter().map(|hit| format!("{} - {}", hit.champion, hit.skin))),
    }
}

fn resolve_champion(query: &str, hits: &[ChampionHit]) -> Resolved {
    let needle = normalize_name(query);
    let exacts: Vec<&ChampionHit> =
        hits.iter().filter(|hit| normalize_name(&hit.champion) == needle).collect();
    match exacts.len() {
        1 => Resolved::Hit(WatchTarget::Champion {
            champion_id: exacts.first().expect("len == 1 checked").champion_id,
            champion: exacts.first().expect("len == 1 checked").champion.clone(),
        }),
        0 => match hits {
            [] => Resolved::Nothing,
            [hit] => Resolved::Hit(WatchTarget::Champion {
                champion_id: hit.champion_id,
                champion: hit.champion.clone(),
            }),
            _ => candidate_lines(hits.iter().map(|hit| hit.champion.clone())),
        },
        _ => candidate_lines(exacts.iter().map(|hit| hit.champion.clone())),
    }
}

fn candidate_lines(lines: impl Iterator<Item = String>) -> Resolved {
    Resolved::Candidates(lines.collect())
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
            TargetKind::Champion => resolve_champion(query, &search.champions),
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
            Resolved::Candidates(lines) => {
                services
                    .chat_output
                    .send(command_reply(format!(
                        "Several matches - run `/lol_store_watch` again with the exact name:\n{}",
                        lines.join("\n")
                    )))
                    .await?;
                return Ok(());
            }
        };

        // Read-modify-write: caps are enforced between load and save.
        let _guard = WATCH_WRITE.lock().await;
        let mut doc = load_watch_doc(storage.as_ref()).await?;
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
        storage.set(NAMESPACE, WATCH_KEY, serde_json::to_value(&doc)?).await?;
        drop(_guard);

        let mut reply_text = format!("Watch #{id} added: {} ({}).", target.label(), kinds.label());
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
        let mut doc = load_watch_doc(storage.as_ref()).await?;
        let (changed, reply_text) = if what.eq_ignore_ascii_case("all") {
            let removed = doc.remove_all_of(&user_id);
            let text = if removed == 0 {
                "You have no watches to remove.".to_owned()
            } else {
                format!("Removed {removed} watch(es).")
            };
            (removed > 0, text)
        } else {
            match what.parse::<u64>() {
                Ok(id) if doc.remove(&user_id, id) => (true, format!("Watch #{id} removed.")),
                Ok(id) => (false, format!("No watch #{id} of yours - see `/lol_store_watchlist`.")),
                Err(_) => {
                    (false, "Give a watch id from `/lol_store_watchlist`, or `all`.".to_owned())
                }
            }
        };
        if changed {
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
        let mut mine: Vec<_> = load_watch_doc(storage.as_ref())
            .await?
            .subs
            .into_iter()
            .filter(|watch| watch.user_id == user_id)
            .collect();
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
        let message = if let Some(text) =
            self.engine.last_announcement_text().filter(|text| !text.is_empty())
        {
            OutboundMessage::embed(Embed {
                title: "LoL Store - latest update".to_owned(),
                description: text,
            })
        } else if let Some(text) = self.engine.current_store_text().await {
            OutboundMessage::embed(Embed {
                title: "LoL Store - current state".to_owned(),
                description: text,
            })
        } else {
            command_reply(
                "No store update has been announced yet, and no store snapshot is \
                     available yet (the client may not have been polled).",
            )
        };
        services.chat_output.send(message).await?;
        Ok(())
    }
}
