use clickhouse_qol::migrations::ClickhouseMigrator;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let migrator =
        ClickhouseMigrator::new("http://admin:123456@localhost:8123/default", "./examples/migrations").await?;
    migrator.init().await?;
    // migrator.create_migration("create_users").await?;
    migrator.down(None).await?;
    // migrator.up(None).await?;
    Ok(())
}
