mod nickname_adapter;
mod presence_adapter;
mod serenity_command_registrar;
mod serenity_outbound;
mod sqlx_storage;

pub use nickname_adapter::SerenityNickname;
pub use presence_adapter::{GatewayContext, SerenityPresence};
pub use serenity_command_registrar::DiscordCommandRegistrar;
pub use serenity_outbound::SerenityChatOutputFactory;
pub use sqlx_storage::SqlxStorage;

// The mention-tag wire codec is shared with the gateway adapter, which
// normalizes inbound tags onto its inverse shape.
pub(crate) use serenity_outbound::{MentionKind, parse_mention_tag};
