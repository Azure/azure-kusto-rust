#![cfg(feature = "mock_transport_framework")]
use azure_core::auth::{TokenCredential, TokenResponse};
use azure_core::Error as CoreError;
use azure_identity::token_credentials::{ClientSecretCredential, TokenCredentialOptions};
use azure_kusto_data::client::{KustoClient, KustoClientOptions};
use chrono::Utc;
use dotenv::dotenv;
use oauth2::AccessToken;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;

pub struct DummyCredential {}

#[async_trait::async_trait]
impl TokenCredential for DummyCredential {
    async fn get_token(&self, _resource: &str) -> Result<TokenResponse, CoreError> {
        Ok(TokenResponse::new(
            AccessToken::new("some dummy token".to_string()),
            Utc::now(),
        ))
    }
}

pub async fn create_kusto_client(
    transaction_name: &str,
) -> Result<(KustoClient, String), Box<dyn Error + Send + Sync>> {
    let db_path = Path::new(&workspace_root().unwrap())
        .join(format!("test/transactions/{}/_db", transaction_name));

    let (service_url, credential, database): (String, Arc<dyn TokenCredential>, String) =
        if std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
            == Ok(azure_core::mock::TESTING_MODE_RECORD)
        {
            dotenv().ok();

            let client_id =
                std::env::var("AZURE_CLIENT_ID").expect("Set env variable AZURE_CLIENT_ID first!");
            let client_secret = std::env::var("AZURE_CLIENT_SECRET")
                .expect("Set env variable AZURE_CLIENT_SECRET first!");
            let tenant_id =
                std::env::var("AZURE_TENANT_ID").expect("Set env variable AZURE_TENANT_ID first!");
            let service_url = std::env::var("KUSTO_SERVICE_URL")
                .expect("Set env variable KUSTO_SERVICE_URL first!");
            let database =
                std::env::var("KUSTO_DATABASE").expect("Set env variable KUSTO_DATABASE first!");

            // Wee need to persist the database name as well, since it may change per recording run depending on who
            // records it, is part of the request, and as such validated against.
            std::fs::write(db_path, &database).unwrap();

            let credential = Arc::new(ClientSecretCredential::new(
                tenant_id.to_string(),
                client_id.to_string(),
                client_secret.to_string(),
                TokenCredentialOptions::default(),
            ));
            (service_url, credential, database)
        } else {
            let credential = Arc::new(DummyCredential {});
            let database = String::from_utf8_lossy(&std::fs::read(db_path).unwrap()).to_string();
            (String::new(), credential, database)
        };

    let options = KustoClientOptions::new_with_transaction_name(transaction_name.to_string());

    Ok((
        KustoClient::new_with_options(service_url, credential, options).unwrap(),
        database,
    ))
}

/// Run cargo to get the root of the workspace
fn workspace_root() -> Result<String, Box<dyn std::error::Error>> {
    let output = std::process::Command::new("cargo")
        .arg("metadata")
        .arg("--no-deps")
        .output()?;
    let output = String::from_utf8_lossy(&output.stdout);

    let key = "workspace_root\":\"";
    let index = output
        .find(key)
        .ok_or_else(|| format!("workspace_root key not found in metadata"))?;
    let value = &output[index + key.len()..];
    let end = value
        .find("\"")
        .ok_or_else(|| format!("workspace_root value was malformed"))?;
    Ok(value[..end].into())
}
