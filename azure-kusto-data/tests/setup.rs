#![cfg(feature = "mock_transport_framework")]
use azure_core::auth::{TokenCredential, TokenResponse};
use azure_core::Error as CoreError;
use azure_identity::token_credentials::{ClientSecretCredential, TokenCredentialOptions};
use azure_kusto_data::client::{KustoClient, KustoClientOptions};
use chrono::Utc;
use oauth2::AccessToken;
use std::error::Error;
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
    let client_id = (std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
        == Ok(azure_core::mock::TESTING_MODE_RECORD))
    .then(get_client_id)
    .unwrap_or_else(String::new);

    let client_secret = (std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
        == Ok(azure_core::mock::TESTING_MODE_RECORD))
    .then(get_client_secret)
    .unwrap_or_else(String::new);

    let tenant_id = (std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
        == Ok(azure_core::mock::TESTING_MODE_RECORD))
    .then(get_tenant_id)
    .unwrap_or_else(String::new);

    let service_url = (std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
        == Ok(azure_core::mock::TESTING_MODE_RECORD))
    .then(get_service_url)
    .unwrap_or_else(String::new);

    let database = (std::env::var(azure_core::mock::TESTING_MODE_KEY).as_deref()
        == Ok(azure_core::mock::TESTING_MODE_RECORD))
    .then(get_database)
    .unwrap_or_else(String::new);

    let options = KustoClientOptions::new_with_transaction_name(transaction_name.to_string());

    let credential: Arc<dyn TokenCredential> = if std::env::var(azure_core::mock::TESTING_MODE_KEY)
        .as_deref()
        == Ok(azure_core::mock::TESTING_MODE_RECORD)
    {
        Arc::new(ClientSecretCredential::new(
            tenant_id.to_string(),
            client_id.to_string(),
            client_secret.to_string(),
            TokenCredentialOptions::default(),
        ))
    } else {
        Arc::new(DummyCredential {})
    };

    Ok((
        KustoClient::new_with_options(service_url, credential, options).unwrap(),
        database,
    ))
}

fn get_service_url() -> String {
    std::env::var("KUSTO_SERVICE_URL").expect("Set env variable KUSTO_SERVICE_URL first!")
}

fn get_database() -> String {
    std::env::var("KUSTO_DATABASE").expect("Set env variable KUSTO_DATABASE first!")
}

fn get_client_id() -> String {
    std::env::var("AZURE_CLIENT_ID").expect("Set env variable AZURE_CLIENT_ID first!")
}

fn get_client_secret() -> String {
    std::env::var("AZURE_CLIENT_SECRET").expect("Set env variable AZURE_CLIENT_SECRET first!")
}

fn get_tenant_id() -> String {
    std::env::var("AZURE_TENANT_ID").expect("Set env variable AZURE_TENANT_ID first!")
}
