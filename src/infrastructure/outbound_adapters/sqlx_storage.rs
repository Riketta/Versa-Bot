use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

use crate::kernel::{
    models::{GuildId, Platform, StorageError},
    spi_ports::{GuildStorage, StoragePort},
};

/// Connection to one of the supported engines. Kept as an explicit enum
/// instead of `AnyPool`: every query carries its dialect visibly, so guild
/// scoping stays auditable per engine.
enum Db {
    Sqlite(sqlx::SqlitePool),
    Postgres(PgPool),
}

/// Plain-SQL storage adapter. One `guild_documents` table, one query shape
/// per dialect; the scoping columns are part of the primary key, so cross-
/// guild leakage has nowhere to hide.
pub struct SqlxStorage {
    db: Arc<Db>,
}

impl SqlxStorage {
    /// Connects by URL scheme (`sqlite://...` or `postgres://...`), creates
    /// the SQLite file if missing, and runs pending migrations.
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        if url.starts_with("sqlite") {
            let in_memory = url.contains(":memory:");
            let options = SqliteConnectOptions::from_str(url)
                .map_err(|err| StorageError::Database(err.to_string()))?
                .create_if_missing(true);
            let options =
                if in_memory { options } else { options.journal_mode(SqliteJournalMode::Wal) };
            // An in-memory database lives per connection - keep a single one.
            let pool = SqlitePoolOptions::new()
                .max_connections(if in_memory { 1 } else { 5 })
                .connect_with(options)
                .await
                .map_err(|err| StorageError::Database(err.to_string()))?;
            // sqlx 0.9 has no `migrate!` facade macro - migrations load from
            // the runtime path (CWD-relative: project root in dev, workdir in
            // the container image).
            let migrator = sqlx::migrate::Migrator::new(Path::new("./migrations"))
                .await
                .map_err(|err| StorageError::Database(err.to_string()))?;
            migrator.run(&pool).await.map_err(|err| StorageError::Database(err.to_string()))?;
            Ok(Self { db: Arc::new(Db::Sqlite(pool)) })
        } else {
            let pool = PgPool::connect(url)
                .await
                .map_err(|err| StorageError::Database(err.to_string()))?;
            let migrator = sqlx::migrate::Migrator::new(Path::new("./migrations"))
                .await
                .map_err(|err| StorageError::Database(err.to_string()))?;
            migrator.run(&pool).await.map_err(|err| StorageError::Database(err.to_string()))?;
            Ok(Self { db: Arc::new(Db::Postgres(pool)) })
        }
    }
}

impl StoragePort for SqlxStorage {
    fn guild_scoped(&self, platform: Platform, guild_id: GuildId) -> Arc<dyn GuildStorage> {
        Arc::new(ScopedGuildStorage {
            db: Arc::clone(&self.db),
            platform: platform.as_str().to_owned(),
            // Discord snowflakes fit i64; engines store integers natively.
            guild_id: guild_id.get() as i64,
        })
    }
}

struct ScopedGuildStorage {
    db: Arc<Db>,
    platform: String,
    guild_id: i64,
}

#[async_trait]
impl GuildStorage for ScopedGuildStorage {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>, StorageError> {
        let raw: Option<String> = match &*self.db {
            Db::Sqlite(pool) => {
                sqlx::query_scalar(
                    "SELECT value FROM guild_documents \
                     WHERE platform = ? AND guild_id = ? AND namespace = ? AND key = ?",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(key)
                .fetch_optional(pool)
                .await
            }
            Db::Postgres(pool) => {
                sqlx::query_scalar(
                    "SELECT value FROM guild_documents \
                     WHERE platform = $1 AND guild_id = $2 AND namespace = $3 AND key = $4",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(key)
                .fetch_optional(pool)
                .await
            }
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;

        raw.map(|json| {
            serde_json::from_str(&json).map_err(|err| StorageError::Serialization(err.to_string()))
        })
        .transpose()
    }

    async fn set(&self, namespace: &str, key: &str, value: Value) -> Result<(), StorageError> {
        let json = serde_json::to_string(&value)
            .map_err(|err| StorageError::Serialization(err.to_string()))?;

        match &*self.db {
            Db::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO guild_documents (platform, guild_id, namespace, key, value) \
                     VALUES (?, ?, ?, ?, ?) \
                     ON CONFLICT (platform, guild_id, namespace, key) DO UPDATE SET value = excluded.value",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(key)
                .bind(&json)
                .execute(pool)
                .await
                .map(|_| ())
            }
            Db::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO guild_documents (platform, guild_id, namespace, key, value) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (platform, guild_id, namespace, key) DO UPDATE SET value = excluded.value",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(key)
                .bind(&json)
                .execute(pool)
                .await
                .map(|_| ())
            }
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;
        Ok(())
    }

    async fn delete(&self, namespace: &str, key: &str) -> Result<(), StorageError> {
        match &*self.db {
            Db::Sqlite(pool) => sqlx::query(
                "DELETE FROM guild_documents \
                     WHERE platform = ? AND guild_id = ? AND namespace = ? AND key = ?",
            )
            .bind(&self.platform)
            .bind(self.guild_id)
            .bind(namespace)
            .bind(key)
            .execute(pool)
            .await
            .map(|_| ()),
            Db::Postgres(pool) => sqlx::query(
                "DELETE FROM guild_documents \
                     WHERE platform = $1 AND guild_id = $2 AND namespace = $3 AND key = $4",
            )
            .bind(&self.platform)
            .bind(self.guild_id)
            .bind(namespace)
            .bind(key)
            .execute(pool)
            .await
            .map(|_| ()),
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;
        Ok(())
    }

    async fn list_keys(&self, namespace: &str) -> Result<Vec<String>, StorageError> {
        let keys: Vec<String> = match &*self.db {
            Db::Sqlite(pool) => {
                sqlx::query_scalar(
                    "SELECT key FROM guild_documents \
                     WHERE platform = ? AND guild_id = ? AND namespace = ? ORDER BY key",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .fetch_all(pool)
                .await
            }
            Db::Postgres(pool) => {
                sqlx::query_scalar(
                    "SELECT key FROM guild_documents \
                     WHERE platform = $1 AND guild_id = $2 AND namespace = $3 ORDER BY key",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .fetch_all(pool)
                .await
            }
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn sqlite_storage() -> SqlxStorage {
        SqlxStorage::connect("sqlite::memory:")
            .await
            .expect("in-memory storage expected to connect")
    }

    #[tokio::test]
    async fn roundtrip_within_guild() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        assert_eq!(guild.get("command", "prefix").await.unwrap(), None);
        guild.set("command", "prefix", Value::String("!".to_owned())).await.unwrap();
        assert_eq!(
            guild.get("command", "prefix").await.unwrap(),
            Some(Value::String("!".to_owned()))
        );
        assert_eq!(guild.list_keys("command").await.unwrap(), ["prefix"]);
    }

    #[tokio::test]
    async fn guilds_are_isolated_by_construction() {
        let storage = sqlite_storage().await;
        let first = storage.guild_scoped(Platform::Discord, GuildId(1));
        let second = storage.guild_scoped(Platform::Discord, GuildId(2));

        first.set("command", "prefix", Value::String("!".to_owned())).await.unwrap();

        assert_eq!(second.get("command", "prefix").await.unwrap(), None);
        second.set("command", "prefix", Value::String("?".to_owned())).await.unwrap();
        assert_eq!(
            first.get("command", "prefix").await.unwrap(),
            Some(Value::String("!".to_owned()))
        );
    }

    #[tokio::test]
    async fn delete_and_namespaces_do_not_mix() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        guild.set("command", "prefix", Value::String("!".to_owned())).await.unwrap();
        guild.set("greeter", "greeting", Value::String("hi".to_owned())).await.unwrap();
        guild.delete("command", "prefix").await.unwrap();

        assert_eq!(guild.get("command", "prefix").await.unwrap(), None);
        assert_eq!(guild.list_keys("command").await.unwrap(), Vec::<String>::new());
        assert_eq!(guild.list_keys("greeter").await.unwrap(), ["greeting"]);
    }
}
