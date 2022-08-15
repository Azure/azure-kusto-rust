#![cfg(feature = "mock_transport_framework")]
use azure_core::auth::{AccessToken, TokenCredential, TokenResponse};
use azure_core::error::Error as CoreError;
use azure_kusto_data::prelude::*;
use dotenv::dotenv;
use std::path::Path;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

pub struct DummyCredential {}

#[async_trait::async_trait]
impl TokenCredential for DummyCredential {
    async fn get_token(&self, _resource: &str) -> Result<TokenResponse, CoreError> {
        Ok(TokenResponse::new(
            AccessToken::new("some dummy token".to_string()),
            OffsetDateTime::now() + Duration::days(365),
        ))
    }
}

#[must_use]
pub fn create_kusto_client(transaction_name: &str) -> (KustoClient, String) {
    let transaction_path = Path::new(&workspace_root().expect("Failed to get workspace root"))
        .join(format!("test/transactions/{}", transaction_name));
    std::fs::create_dir_all(&transaction_path).expect("Failed to create transaction directory");
    let db_path = transaction_path.join("_db");

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
            std::fs::write(db_path, &database).expect("Failed to write database name to file");

            let credential = Arc::new(ClientSecretCredential::new(
                tenant_id,
                client_id,
                client_secret,
                TokenCredentialOptions::default(),
            ));
            (service_url, credential, database)
        } else {
            let credential = Arc::new(DummyCredential {});
            let database = String::from_utf8_lossy(
                &std::fs::read(&db_path)
                    .unwrap_or_else(|_| panic!("Could not read db path {}", db_path.display())),
            )
            .to_string();
            (String::new(), credential, database)
        };

    let options = KustoClientOptions::new_with_transaction_name(transaction_name.to_string());

    (
        KustoClient::new(
            ConnectionString::with_token_credential(service_url, credential),
            options,
        )
        .expect("Failed to create KustoClient"),
        database,
    )
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
        .ok_or_else(|| "workspace_root key not found in metadata".to_string())?;
    let value = &output[index + key.len()..];
    let end = value
        .find('\"')
        .ok_or_else(|| "workspace_root value was malformed".to_string())?;
    Ok(value[..end].into())
}
