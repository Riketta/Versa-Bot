use async_trait::async_trait;

#[async_trait]
pub trait PlatformQueryPort: Send + Sync {
    // async fn get_guild_member(
    //     &self,
    //     guild_id: crate::kernel::models::GuildId,
    //     user_id: crate::kernel::models::UserId,
    // ) -> Result<Option<crate::kernel::models::MemberPayload>, crate::kernel::models::OutboundError>;
    // async fn get_roles(&self, guild_id: crate::kernel::models::GuildId) ->
    //     Result<Vec<String>, crate::kernel::models::OutboundError>;
    // async fn get_channels(&self, guild_id: crate::kernel::models::GuildId) ->
    //     Result<Vec<crate::kernel::models::ChannelId>, crate::kernel::models::OutboundError>;
    // async fn resolve_user(&self, user_id: crate::kernel::models::UserId) ->
    //     Result<Option<String>, crate::kernel::models::OutboundError>;
}
