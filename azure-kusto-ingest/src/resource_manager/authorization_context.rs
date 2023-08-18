use std::sync::Arc;

use anyhow::Result;
use azure_kusto_data::prelude::KustoClient;
use tokio::sync::RwLock;

use super::cache::{Cached, Refreshing};
use super::RESOURCE_REFRESH_PERIOD;

pub type KustoIdentityToken = String;
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    client: KustoClient,
    auth_context_cache: Refreshing<Option<KustoIdentityToken>>,
}

impl AuthorizationContext {
    pub fn new(client: KustoClient) -> Self {
        Self {
            client,
            auth_context_cache: Arc::new(RwLock::new(Cached::new(None, RESOURCE_REFRESH_PERIOD))),
        }
    }

    // Logic to get the Kusto identity token from Kusto management endpoint - handle validation here
    async fn execute_kql_mgmt_query(client: KustoClient) -> Result<KustoIdentityToken> {
        let results = client
            .execute_command("NetDefaultDB", ".get kusto identity token", None)
            .await?;
        // TODO: any other checks, plus error handling
        let table = results.tables.first().unwrap();

        println!("table: {:#?}", table);

        // TODO: any other checks, plus error handling
        let kusto_identity_token = table
            .rows
            .first()
            .unwrap()
            .first()
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        if kusto_identity_token.chars().all(char::is_whitespace) {
            return Err(anyhow::anyhow!("Kusto identity token is empty"));
        }

        println!("kusto_identity_token: {:#?}", kusto_identity_token);

        Ok(kusto_identity_token)
    }

    // handle caching here
    pub async fn get(&self) -> Result<KustoIdentityToken> {
        let auth_context_cache = self.auth_context_cache.read().await;
        if !auth_context_cache.is_expired() {
            if let Some(inner_value) = auth_context_cache.get() {
                return Ok(inner_value.clone());
            }
        }
        // otherwise, drop the read lock and get a write lock to refresh the token
        drop(auth_context_cache);
        let mut auth_context_cache = self.auth_context_cache.write().await;

        // check again in case another thread refreshed the token while we were
        // waiting on the write lock
        if let Some(inner_value) = auth_context_cache.get() {
            return Ok(inner_value.clone());
        }

        let token = Self::execute_kql_mgmt_query(self.client.clone()).await?;
        auth_context_cache.update(Some(token.clone()));

        Ok(token)
    }
}
