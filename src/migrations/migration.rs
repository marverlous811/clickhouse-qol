use crate::error::ClickhouseToolError;

#[async_trait::async_trait]
pub trait ClickhouseMigrationTrait: Send + Sync {
    async fn up(&self) -> Result<(), ClickhouseToolError>;
    async fn down(&self) -> Result<(), ClickhouseToolError>;
}

pub trait ClickhouseMigratorTrait {
    fn migrations(&self) -> Vec<Box<dyn ClickhouseMigrationTrait>>;
}
