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
                     /lol_store_assign."
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
                 enabled with /lol_store_enable).",
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

/// `/lol_store_dump`: force-post the latest store update summary into the
/// invoking channel (moderator, public - a dump is meant to be seen).
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
        let message = match self.engine.last_announcement_text() {
            Some(text) if !text.is_empty() => OutboundMessage::embed(Embed {
                title: "LoL Store - latest update".to_owned(),
                description: text,
            }),
            _ => command_reply("No store update has been announced yet."),
        };
        services.chat_output.send(message).await?;
        Ok(())
    }
}
