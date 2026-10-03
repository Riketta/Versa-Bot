use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::migrate::{Migration, MigrationType, Migrator};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{SqlSafeStr, sqlite::SqlitePool};

use crate::kernel::{
    models::{GuildId, Platform, StorageError},
    spi_ports::{GUILD_SETTINGS, GuildStorage, StoragePort, StoredRecord},
};

/// Connection to one of the supported engines. Kept as an explicit enum
/// instead of `AnyPool`: every query carries its dialect visibly, so guild
/// scoping stays auditable per engine.
enum Db {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

/// Compile-time-embedded migrations: sqlx 0.9 dropped the `migrate!` macro,
/// so the set is assembled by hand from `include_str!` sources. The binary
/// stays CWD-independent (it used to break when launched outside the project
/// root), and `Migration::new` computes the same checksums the directory
/// loader did, so already-migrated databases validate cleanly.
fn embedded_migrations() -> Vec<Migration> {
    vec![
        migration(
            1,
            "guild_documents",
            include_str!("../../../migrations/0001_guild_documents.sql"),
        ),
        migration(2, "guild_records", include_str!("../../../migrations/0002_guild_records.sql")),
    ]
}

/// The `embedded_migrations` entry: mirrors the directory loader's parse of
/// `000<version>_<description>.sql` (plain, transactional migrations).
fn migration(version: i64, description: &'static str, sql: &'static str) -> Migration {
    Migration::new(
        version,
        Cow::Borrowed(description),
        MigrationType::Simple,
        sql.into_sql_str(),
        false,
    )
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
            // Migrations are embedded at compile time - no runtime path.
            Migrator::with_migrations(embedded_migrations())
                .run(&pool)
                .await
                .map_err(|err| StorageError::Database(err.to_string()))?;
            Ok(Self { db: Arc::new(Db::Sqlite(pool)) })
        } else {
            let pool = PgPool::connect(url)
                .await
                .map_err(|err| StorageError::Database(err.to_string()))?;
            Migrator::with_migrations(embedded_migrations())
                .run(&pool)
                .await
                .map_err(|err| StorageError::Database(err.to_string()))?;
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
        // The `guild` namespace is reserved for guild settings (kernel
        // policy, adapter-enforced): plugins must not be able to corrupt
        // them, so writes from any scoped handle are rejected. Reads stay
        // permitted.
        if namespace == GUILD_SETTINGS {
            // Warn, not error: this is policy enforcement, not a storage
            // failure - a plugin bug must not raise a Sentry Issue per
            // rejected write. The warn still ships as a log item.
            tracing::warn!(
                namespace = GUILD_SETTINGS,
                key,
                "rejected write to the reserved guild namespace"
            );
            return Err(StorageError::Forbidden("the 'guild' namespace is reserved".to_owned()));
        }

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
        if namespace == GUILD_SETTINGS {
            // Same level as `set`: policy enforcement, not a failure.
            tracing::warn!(
                namespace = GUILD_SETTINGS,
                key,
                "rejected delete in the reserved guild namespace"
            );
            return Err(StorageError::Forbidden("the 'guild' namespace is reserved".to_owned()));
        }

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

    async fn append(&self, namespace: &str, payload: Value) -> Result<u64, StorageError> {
        // Same reserved-namespace policy as document writes: plugins record
        // into their own namespaces, never into guild settings.
        if namespace == GUILD_SETTINGS {
            // Same level as `set`/`delete`: policy enforcement, not a failure.
            tracing::warn!(
                namespace = GUILD_SETTINGS,
                "rejected append to the reserved guild namespace"
            );
            return Err(StorageError::Forbidden("the 'guild' namespace is reserved".to_owned()));
        }

        let json = serde_json::to_string(&payload)
            .map_err(|err| StorageError::Serialization(err.to_string()))?;

        // One statement assigns the next guild-scoped sequence (MAX+1 over
        // the scope's rows) and returns it - no read-write race window.
        let seq: i64 = match &*self.db {
            Db::Sqlite(pool) => {
                sqlx::query_scalar(
                    "INSERT INTO guild_records (platform, guild_id, namespace, seq, value) \
                     SELECT ?, ?, ?, COALESCE(MAX(seq), 0) + 1, ? \
                     FROM guild_records \
                     WHERE platform = ? AND guild_id = ? AND namespace = ? \
                     RETURNING seq",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(&json)
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .fetch_one(pool)
                .await
            }
            Db::Postgres(pool) => {
                sqlx::query_scalar(
                    "INSERT INTO guild_records (platform, guild_id, namespace, seq, value) \
                     SELECT $1, $2, $3, COALESCE(MAX(seq), 0) + 1, $4 \
                     FROM guild_records \
                     WHERE platform = $5 AND guild_id = $6 AND namespace = $7 \
                     RETURNING seq",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(&json)
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .fetch_one(pool)
                .await
            }
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;

        Ok(u64::try_from(seq).map_err(|err| StorageError::Database(err.to_string()))?)
    }

    async fn list_after(
        &self,
        namespace: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let after =
            i64::try_from(after_seq).map_err(|err| StorageError::Database(err.to_string()))?;
        let rows: Vec<(i64, String)> = match &*self.db {
            Db::Sqlite(pool) => {
                sqlx::query_as(
                    "SELECT seq, value FROM guild_records \
                     WHERE platform = ? AND guild_id = ? AND namespace = ? AND seq > ? \
                     ORDER BY seq LIMIT ?",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(after)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await
            }
            Db::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT seq, value FROM guild_records \
                     WHERE platform = $1 AND guild_id = $2 AND namespace = $3 AND seq > $4 \
                     ORDER BY seq LIMIT $5",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(after)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await
            }
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;

        rows.into_iter()
            .map(|(seq, json)| {
                Ok(StoredRecord {
                    seq: u64::try_from(seq)
                        .map_err(|err| StorageError::Database(err.to_string()))?,
                    payload: serde_json::from_str(&json)
                        .map_err(|err| StorageError::Serialization(err.to_string()))?,
                })
            })
            .collect()
    }

    async fn list_last(
        &self,
        namespace: &str,
        limit: u32,
    ) -> Result<Vec<StoredRecord>, StorageError> {
        let rows: Vec<(i64, String)> = match &*self.db {
            Db::Sqlite(pool) => {
                sqlx::query_as(
                    "SELECT seq, value FROM guild_records \
                     WHERE platform = ? AND guild_id = ? AND namespace = ? \
                     ORDER BY seq DESC LIMIT ?",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await
            }
            Db::Postgres(pool) => {
                sqlx::query_as(
                    "SELECT seq, value FROM guild_records \
                     WHERE platform = $1 AND guild_id = $2 AND namespace = $3 \
                     ORDER BY seq DESC LIMIT $4",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(i64::from(limit))
                .fetch_all(pool)
                .await
            }
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;

        // The query returns newest-first; callers expect ascending order.
        let mut records: Vec<StoredRecord> = rows
            .into_iter()
            .map(|(seq, json)| {
                Ok(StoredRecord {
                    seq: u64::try_from(seq)
                        .map_err(|err| StorageError::Database(err.to_string()))?,
                    payload: serde_json::from_str(&json)
                        .map_err(|err| StorageError::Serialization(err.to_string()))?,
                })
            })
            .collect::<Result<_, _>>()?;
        records.reverse();
        Ok(records)
    }

    async fn count_after(&self, namespace: &str, after_seq: u64) -> Result<u64, StorageError> {
        let after =
            i64::try_from(after_seq).map_err(|err| StorageError::Database(err.to_string()))?;
        let count: i64 = match &*self.db {
            Db::Sqlite(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM guild_records \
                     WHERE platform = ? AND guild_id = ? AND namespace = ? AND seq > ?",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(after)
                .fetch_one(pool)
                .await
            }
            Db::Postgres(pool) => {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM guild_records \
                     WHERE platform = $1 AND guild_id = $2 AND namespace = $3 AND seq > $4",
                )
                .bind(&self.platform)
                .bind(self.guild_id)
                .bind(namespace)
                .bind(after)
                .fetch_one(pool)
                .await
            }
        }
        .map_err(|err| StorageError::Database(err.to_string()))?;

        Ok(u64::try_from(count).map_err(|err| StorageError::Database(err.to_string()))?)
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
    async fn set_overwrites_the_previous_value() {
        // The upsert path is what the LLM plugin's one-atomic-write state
        // commit rides: the same key must replace, not duplicate.
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        guild.set("llm", "state", Value::String("first".to_owned())).await.unwrap();
        guild.set("llm", "state", Value::String("second".to_owned())).await.unwrap();

        assert_eq!(
            guild.get("llm", "state").await.unwrap(),
            Some(Value::String("second".to_owned()))
        );
        assert_eq!(guild.list_keys("llm").await.unwrap(), ["state"]);
    }

    /// The reserved namespace is write-only guarded: kernel-side reads of
    /// guild settings must stay permitted (the config manager depends on it).
    #[tokio::test]
    async fn reserved_guild_namespace_permits_reads() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        let seeded = guild.get(GUILD_SETTINGS, "language").await.unwrap();
        assert_eq!(seeded, None);
        assert!(guild.list_keys(GUILD_SETTINGS).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn reserved_guild_namespace_rejects_writes() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        let set = guild.set(GUILD_SETTINGS, "language", Value::String("en".to_owned())).await;
        assert!(matches!(set, Err(StorageError::Forbidden(_))));
        let delete = guild.delete(GUILD_SETTINGS, "language").await;
        assert!(matches!(delete, Err(StorageError::Forbidden(_))));
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

    #[tokio::test]
    async fn records_append_in_order_with_increasing_seq() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        let first = guild.append("llm", Value::String("a".to_owned())).await.unwrap();
        let second = guild.append("llm", Value::String("b".to_owned())).await.unwrap();
        let third = guild.append("llm", Value::String("c".to_owned())).await.unwrap();
        assert!(first < second && second < third);

        let records = guild.list_after("llm", 0, 100).await.unwrap();
        assert_eq!(records.len(), 3);
        let head = records.first().expect("three records expected");
        assert_eq!(head.seq, first);
        assert_eq!(head.payload, Value::String("a".to_owned()));
        let tail = records.get(2).expect("three records expected");
        assert_eq!(tail.seq, third);
        assert_eq!(tail.payload, Value::String("c".to_owned()));
    }

    #[tokio::test]
    async fn list_after_and_count_after_window_records() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        let first = guild.append("llm", Value::from(1)).await.unwrap();
        guild.append("llm", Value::from(2)).await.unwrap();
        guild.append("llm", Value::from(3)).await.unwrap();

        let window = guild.list_after("llm", first, 100).await.unwrap();
        assert_eq!(window.len(), 2);
        assert_eq!(window.first().map(|record| record.payload.clone()), Some(Value::from(2)));
        assert_eq!(guild.count_after("llm", first).await.unwrap(), 2);
        assert_eq!(guild.count_after("llm", 0).await.unwrap(), 3);

        // Ascending order + LIMIT keeps the earliest records after the
        // cursor - consumers page forward by cursor (e.g. compaction walks
        // the oldest chunk first).
        let limited = guild.list_after("llm", 0, 2).await.unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(limited.first().map(|record| record.payload.clone()), Some(Value::from(1)));
    }

    #[tokio::test]
    async fn list_last_returns_the_newest_tail_in_ascending_order() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        for value in 1..=5 {
            guild.append("llm", Value::from(value)).await.unwrap();
        }

        let tail = guild.list_last("llm", 3).await.unwrap();
        let seqs: Vec<u64> = tail.iter().map(|record| record.seq).collect();
        let values: Vec<i64> = tail.iter().filter_map(|record| record.payload.as_i64()).collect();
        assert_eq!(values, [3, 4, 5]);
        assert!(
            seqs.windows(2).all(|pair| pair
                .first()
                .is_some_and(|head| pair.get(1).is_some_and(|next| head < next))),
            "ascending expected"
        );

        // More capacity than records: everything comes back, oldest first.
        let all = guild.list_last("llm", 100).await.unwrap();
        assert_eq!(
            all.iter().filter_map(|record| record.payload.as_i64()).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5]
        );

        // Namespaces and guilds do not mix (same partitioning as list_after).
        let other = storage.guild_scoped(Platform::Discord, GuildId(2));
        other.append("llm", Value::from(99)).await.unwrap();
        assert_eq!(guild.list_last("llm", 100).await.unwrap().len(), 5);
        assert_eq!(other.list_last("llm", 100).await.unwrap().len(), 1);
        assert_eq!(guild.list_last("tracker", 100).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn migrations_reopen_keeps_data_and_reapply_is_idempotent() {
        // A FILE-backed database (in-memory cannot re-open): connect and
        // migrate, write, drop the pool, connect again - the migration set
        // re-applies as no-ops (checksummed) and the data survives. This is
        // the realistic upgrade-path regression: an existing database must
        // never be clobbered by a re-open.
        let path = std::env::temp_dir().join(format!(
            "versabot-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ));
        let url = format!("sqlite://{}", path.display());
        {
            let storage = SqlxStorage::connect(&url).await.expect("connect expected");
            let guild = storage.guild_scoped(Platform::Discord, GuildId(1));
            guild.set("llm", "k", Value::String("v".to_owned())).await.unwrap();
            guild.append("llm", Value::from(7)).await.unwrap();
        }

        let reopened = SqlxStorage::connect(&url).await.expect("reopen expected");
        let guild = reopened.guild_scoped(Platform::Discord, GuildId(1));
        assert_eq!(guild.get("llm", "k").await.unwrap(), Some(Value::String("v".to_owned())));
        assert_eq!(guild.list_last("llm", 10).await.unwrap().len(), 1);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn records_do_not_mix_across_namespaces_and_guilds() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));
        let other_guild = storage.guild_scoped(Platform::Discord, GuildId(2));

        guild.append("llm", Value::String("one".to_owned())).await.unwrap();
        guild.append("tracker", Value::String("two".to_owned())).await.unwrap();
        other_guild.append("llm", Value::String("three".to_owned())).await.unwrap();

        assert_eq!(guild.list_after("llm", 0, 100).await.unwrap().len(), 1);
        assert_eq!(guild.list_after("tracker", 0, 100).await.unwrap().len(), 1);
        assert_eq!(other_guild.list_after("llm", 0, 100).await.unwrap().len(), 1);
        assert_eq!(other_guild.count_after("tracker", 0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn reserved_guild_namespace_rejects_appends() {
        let storage = sqlite_storage().await;
        let guild = storage.guild_scoped(Platform::Discord, GuildId(1));

        let append = guild.append(GUILD_SETTINGS, Value::String("x".to_owned())).await;
        assert!(matches!(append, Err(StorageError::Forbidden(_))));
    }
}
