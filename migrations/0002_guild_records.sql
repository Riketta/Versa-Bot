-- Append-only per-plugin record log (e.g. LLM conversation history) for
-- high-volume ordered data that outgrows key-value documents.
-- Engine-neutral: BIGINT/TEXT are valid in both SQLite and PostgreSQL.
-- seq is assigned at insert time as MAX(seq)+1 within the
-- (platform, guild_id, namespace) scope, so records stay ordered per guild.
-- Scoping columns are part of the primary key - same guarantee as
-- guild_documents: a row without guild context cannot exist.
CREATE TABLE IF NOT EXISTS guild_records (
    platform  TEXT   NOT NULL,
    guild_id  BIGINT NOT NULL,
    namespace TEXT   NOT NULL,
    seq       BIGINT NOT NULL,
    value     TEXT   NOT NULL,
    PRIMARY KEY (platform, guild_id, namespace, seq)
);
