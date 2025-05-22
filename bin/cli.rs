use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Parser, Debug)]
pub struct CreateMigrationArgs {
    #[arg(long)]
    pub name: String,
}

#[derive(Parser, Debug)]
pub struct ExecuteMigrationArgs {
    #[arg(long)]
    pub step: Option<usize>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Up(ExecuteMigrationArgs),
    Donw(ExecuteMigrationArgs),
    Reset,
    Create(CreateMigrationArgs),
}

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(long)]
    pub database_endpoint: String,

    #[arg(long)]
    pub migration_directory: String,

    #[command(subcommand)]
    pub command: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .init();

    let migrator = clickhouse_tools::migrations::ClickhouseMigrator::new(
        &args.database_endpoint,
        &args.migration_directory,
    )
    .await
    .map_err(|e| {
        log::error!("Failed to create migrator: {}", e);
        e
    })?;

    match args.command {
        Command::Up(args) => {
            if let Err(e) = migrator.up(args.step).await {
                log::error!("Failed to apply migrations: {}", e);
            }
        }
        Command::Donw(args) => {
            if let Err(e) = migrator.down(args.step).await {
                log::error!("Failed to rollback migrations: {}", e);
            }
        }
        Command::Reset => {
            if let Err(e) = migrator.down(None).await {
                log::error!("Failed to reset migrations: {}", e);
            }
        }
        Command::Create(args) => {
            if let Err(e) = migrator.create_migration(&args.name).await {
                log::error!("Failed to create migration: {}", e);
            }
        }
    }

    Ok(())
}
