use async_trait::async_trait;

#[async_trait]
pub trait PresencePort: Send + Sync {
    // async fn set_status(&self, status: crate::kernel::models::OutboundMessage) ->
    //     Result<(), crate::kernel::models::OutboundError>;
    // async fn set_activity(&self, activity: Option<crate::kernel::models::OutboundMessage>) ->
    //     Result<(), crate::kernel::models::OutboundError>;
}
