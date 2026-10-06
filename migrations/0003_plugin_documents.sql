-- Plugin-global documents: operator-side aggregates and settings that
-- deliberately cross guild boundaries (token counters, plugin-wide state).
-- User content stays in guild_documents, whose rows cannot exist without
-- guild context. The `platform` column keeps deployments separated the same
-- way guild_documents does. Engine-neutral: TEXT/TEXT are valid in both
-- SQLite and PostgreSQL.
CREATE TABLE IF NOT EXISTS plugin_documents (
    platform  TEXT NOT NULL,
    namespace TEXT NOT NULL,
    key       TEXT NOT NULL,
    value     TEXT NOT NULL,
    PRIMARY KEY (platform, namespace, key)
);
