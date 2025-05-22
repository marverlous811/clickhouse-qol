use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClickhouseToolError {
    #[error("Clickhouse client error: {0}")]
    ClientError(#[from] clickhouse::error::Error),

    #[error("Invalid Args error: {0}")]
    InvaidArgs(String),

    #[error("Io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    InternalError(String),
}
