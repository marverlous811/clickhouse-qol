use clickhouse::Client;
use error::ClickhouseToolError;
use url::Url;
use urlencoding::decode;

pub mod error;
pub mod migrations;
pub mod worker;

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
