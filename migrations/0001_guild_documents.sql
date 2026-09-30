-- Engine-neutral: BIGINT/TEXT are valid in both SQLite and PostgreSQL.
-- Scoping columns (platform, guild_id) are part of the primary key, so a
-- row without guild context cannot exist.
CREATE TABLE IF NOT EXISTS guild_documents (
    platform  TEXT   NOT NULL,
    guild_id  BIGINT NOT NULL,
    namespace TEXT   NOT NULL,
    key       TEXT   NOT NULL,
    value     TEXT   NOT NULL,
    PRIMARY KEY (platform, guild_id, namespace, key)
);
