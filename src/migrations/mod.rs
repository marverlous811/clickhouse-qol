use std::{cmp::max, collections::HashMap, path::PathBuf};

use clickhouse_operations::{create_migrations_table, get_migrations_in_database};

use crate::error::ClickhouseToolError;

mod clickhouse_operations;

#[derive(Debug, Clone)]
pub struct ClickhouseMigration {
    pub version: u64,
    pub name: String,
    pub path: PathBuf,
}

impl ClickhouseMigration {
    pub fn from_file(path: String) -> Result<Self, ClickhouseToolError> {
        let path = PathBuf::from(path);
        let name = path
            .clone()
            .file_name()
            .ok_or_else(|| {
                ClickhouseToolError::InvaidArgs("Invalid migration file name".to_string())
            })?
            .to_os_string()
            .into_string()
            .map_err(|_| {
                ClickhouseToolError::InvaidArgs("Invalid migration file name".to_string())
            })?;

        let (version_str, name) = name.split_once('_').ok_or_else(|| {
            ClickhouseToolError::InvaidArgs("Invalid migration file name".to_string())
        })?;

        let version = version_str.parse::<u64>().map_err(|_| {
            ClickhouseToolError::InvaidArgs("Invalid migration file name".to_string())
        })?;

        Ok(Self {
            version,
            name: name.to_string(),
            path,
        })
    }

    pub async fn get_up_query(&self) -> Result<String, ClickhouseToolError> {
        tokio::fs::read_to_string(&self.path.join("up.sql"))
            .await
            .map_err(|_| {
                ClickhouseToolError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to read migration file",
                ))
            })
    }

    pub async fn get_down_query(&self) -> Result<String, ClickhouseToolError> {
        tokio::fs::read_to_string(&self.path.join("down.sql"))
            .await
            .map_err(|_| {
                ClickhouseToolError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to read migration file",
                ))
            })
    }
}

#[cfg(test)]
mod test {
    use super::ClickhouseMigration;

    #[test]
    fn test_clickhouse_migration() {
        let migration = ClickhouseMigration::from_file("20230101_001_create_users".to_string());

        assert!(migration.is_ok());
        let migration = migration.unwrap();
        assert_eq!(migration.version, 20230101);
        assert_eq!(migration.name, "001_create_users");
    }

    #[test]
    fn test_clickhouse_migration_in_directory() {
        let migration = ClickhouseMigration::from_file(
            "./clickhouse/migrations/20230101_001_create_users".to_string(),
        );

        assert!(migration.is_ok());
        let migration = migration.unwrap();
        assert_eq!(migration.version, 20230101);
        assert_eq!(migration.name, "001_create_users");
    }
}

pub struct ClickhouseMigrator {
    client: clickhouse::Client,
    directory: String,
}

impl ClickhouseMigrator {
    pub async fn new(uri: &str, directory: &str) -> Result<Self, ClickhouseToolError> {
        let client = clickhouse_operations::init_client(uri).await?;
        Ok(Self {
            client,
            directory: directory.to_string(),
        })
    }

    pub async fn init(&self) -> Result<(), ClickhouseToolError> {
        create_migrations_table(&self.client).await
    }

    pub async fn create_migration(&self, name: &str) -> Result<(), ClickhouseToolError> {
        let now = chrono::Local::now().format("%Y%m%d%H%M%S");
        let migration_name = format!("{}_{}", now, name);
        let migration_path = format!("{}/{}", self.directory, migration_name);
        let up_file_path = format!("{}/up.sql", migration_path);
        let down_file_path = format!("{}/down.sql", migration_path);
        tokio::fs::create_dir_all(&migration_path).await?;
        tokio::fs::write(&up_file_path, "-- up migration").await?;
        tokio::fs::write(&down_file_path, "-- down migration").await?;
        Ok(())
    }

    pub async fn up(&self, step: Option<usize>) -> Result<(), ClickhouseToolError> {
        let migration = self.get_migrations().await?;
        let migration_exist = get_migrations_in_database(&self.client).await?;
        let migration = if migration_exist.is_empty() {
            migration
        } else {
            let last_migration =
                migration_exist
                    .first()
                    .ok_or(ClickhouseToolError::InternalError(
                        "No migration found".to_string(),
                    ))?;
            migration
                .into_iter()
                .filter(|m| m.version > last_migration.version)
                .map(|m| m.to_owned())
                .collect::<Vec<ClickhouseMigration>>()
        };

        let step = max(migration.len(), step.unwrap_or(migration.len()));
        let migration = migration[..step].to_vec();
        for m in migration {
            clickhouse_operations::apply_migration(&self.client, &m).await?;
        }

        Ok(())
    }

    pub async fn down(&self, step: Option<usize>) -> Result<(), ClickhouseToolError> {
        let migration = self.get_migrations().await?;
        let migration_map: HashMap<u64, ClickhouseMigration> =
            migration.iter().map(|m| (m.version, m.clone())).collect();
        let migration_exist = get_migrations_in_database(&self.client).await?;
        if migration_exist.is_empty() {
            return Ok(());
        } else {
            let mut exist_step = step.unwrap_or(migration_exist.len());
            for m in migration_exist.iter() {
                if exist_step == 0 {
                    break;
                }
                if let Some(migration) = migration_map.get(&m.version) {
                    clickhouse_operations::rollback_migration(&self.client, migration).await?;
                }
                exist_step -= 1;
            }
            Ok(())
        }
    }

    pub async fn get_migrations(&self) -> Result<Vec<ClickhouseMigration>, ClickhouseToolError> {
        let mut migrations = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.directory).await?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| ClickhouseToolError::IoError(e))?
        {
            if entry.file_type().await?.is_dir() {
                let migration =
                    ClickhouseMigration::from_file(entry.path().to_string_lossy().to_string())?;
                migrations.push(migration);
            }
        }

        migrations.sort_by(|a, b| a.version.cmp(&b.version));
        Ok(migrations)
    }
}
