use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use url::Url;
use urlencoding::decode;

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

pub async fn get_migrations_in_database(
    client: &Client,
) -> Result<Vec<ClickhouseMigrationRow>, ClickhouseToolError> {
    let query = format!("SELECT * FROM {} ORDER BY version DESC", MIGRATION_TABLE);
    let migrations = client
        .query(&query)
        .fetch_all::<ClickhouseMigrationRow>()
        .await?;

    Ok(migrations)
}

pub async fn apply_migration(
    client: &Client,
    migration: &ClickhouseMigration,
) -> Result<(), ClickhouseToolError> {
    let up_query = migration.get_up_query().await?;
    client.query(&up_query).execute().await?;

    let insert_query = format!(
        "INSERT INTO {} (version, name, timestamp) VALUES ({}, '{}', now())",
        MIGRATION_TABLE, migration.version, migration.name
    );
    client.query(&insert_query).execute().await?;

    Ok(())
}

pub async fn rollback_migration(
    client: &Client,
    migration: &ClickhouseMigration,
) -> Result<(), ClickhouseToolError> {
    let down_query = migration.get_down_query().await?;
    client.query(&down_query).execute().await?;

    let delete_query = format!(
        "DELETE FROM {} WHERE version = {}",
        MIGRATION_TABLE, migration.version
    );
    client.query(&delete_query).execute().await?;

    Ok(())
}

pub async fn init_client(db_uri: &str) -> Result<Client, ClickhouseToolError> {
    let endpoint: Url = db_uri
        .parse()
        .map_err(|_| ClickhouseToolError::InvaidArgs("Invalid URL".to_string()))?;

    let url = format!(
        "{}://{}:{}",
        endpoint.scheme(),
        endpoint.host().expect("clickhouse must have host"),
        endpoint.port().expect("clickhouse must have port")
    );
    let user = endpoint.username();
    let password = decode(endpoint.password().unwrap_or(""))
        .map_err(|_| ClickhouseToolError::InvaidArgs("Invalid password".to_string()))?;
    let database = endpoint
        .path()
        .split('/')
        .nth(1)
        .expect("clickhouse ep must have database");

    let mut client = Client::default().with_url(&url);
    if !user.is_empty() {
        client = client.with_user(user);
    }

    if !password.is_empty() {
        client = client.with_password(password);
    }

    if !database.is_empty() {
        client = client.with_database(database);
    }

    client.query("SELECT 1").execute().await?;
    Ok(client)
}
