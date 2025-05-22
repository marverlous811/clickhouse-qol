-- up migration
CREATE TABLE IF NOT EXISTS users (
    id UInt32,
    name String,
    email String,
    created_at DateTime64(3, 'UTC')
) ENGINE = MergeTree 
ORDER BY id;