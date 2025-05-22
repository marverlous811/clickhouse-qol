use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::ClickhouseToolError;

use super::ClickhouseMigration;

const MIGRATION_TABLE: &str = "s_migrations";

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct ClickhouseMigrationRow {
    pub version: u64,
    pub name: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub timestamp: OffsetDateTime,
}

pub async fn create_migrations_table(client: &Client) -> Result<(), ClickhouseToolError> {
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} (
                version UInt64,
                name String,
                timestamp DateTime
            ) ENGINE = MergeTree()
            ORDER BY (version, timestamp)",
        MIGRATION_TABLE
    );
    client.query(&query).execute().await?;

    Ok(())
}

pub async fn get_migrations_in_database(client: &Client) -> Result<Vec<ClickhouseMigrationRow>, ClickhouseToolError> {
    let query = format!("SELECT * FROM {} ORDER BY version DESC", MIGRATION_TABLE);
    let migrations = client.query(&query).fetch_all::<ClickhouseMigrationRow>().await?;

    Ok(migrations)
}

pub async fn apply_migration(client: &Client, migration: &ClickhouseMigration) -> Result<(), ClickhouseToolError> {
    let up_query = migration.get_up_query().await?;
    client.query(&up_query).execute().await?;

    let insert_query = format!(
        "INSERT INTO {} (version, name, timestamp) VALUES ({}, '{}', now())",
        MIGRATION_TABLE, migration.version, migration.name
    );
    client.query(&insert_query).execute().await?;

    Ok(())
}

pub async fn rollback_migration(client: &Client, migration: &ClickhouseMigration) -> Result<(), ClickhouseToolError> {
    let down_query = migration.get_down_query().await?;
    client.query(&down_query).execute().await?;

    let delete_query = format!("DELETE FROM {} WHERE version = {}", MIGRATION_TABLE, migration.version);
    client.query(&delete_query).execute().await?;

    Ok(())
}
