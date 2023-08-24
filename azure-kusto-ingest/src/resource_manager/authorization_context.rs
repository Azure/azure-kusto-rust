use std::sync::Arc;

use anyhow::Result;
use azure_kusto_data::prelude::KustoClient;
use tokio::sync::RwLock;

use super::cache::{Cached, Refreshing};
use super::RESOURCE_REFRESH_PERIOD;

pub type KustoIdentityToken = String;

/// Logic to obtain a Kusto identity token from the management endpoint. This auth token is a temporary token
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

    /// Executes a KQL query to get the Kusto identity token from the management endpoint
    async fn execute_kql_mgmt_query(client: KustoClient) -> Result<KustoIdentityToken> {
        let results = client
            .execute_command("NetDefaultDB", ".get kusto identity token", None)
            .await?;

        let table = match &results.tables[..] {
            [a] => a,
            _ => {
                return Err(anyhow::anyhow!(
                    "Kusto Expected 1 table in results, found {}",
                    results.tables.len()
                ))
            }
        };

        // TODO: add more validation here
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

        Ok(kusto_identity_token)
    }

    /// Fetches the latest Kusto identity token, either retrieving from cache if valid, or by executing a KQL query
    pub async fn get(&self) -> Result<KustoIdentityToken> {
        // First, attempt to get the return the token from the cache
        let auth_context_cache = self.auth_context_cache.read().await;
        if !auth_context_cache.is_expired() {
            if let Some(inner_value) = auth_context_cache.get() {
                return Ok(inner_value.clone());
            }
        }
        // Drop the read lock and get a write lock to refresh the token
        drop(auth_context_cache);
        let mut auth_context_cache = self.auth_context_cache.write().await;

        // Again attempt to return from cache, check is done in case another thread
        // refreshed the token while we were waiting on the write lock
        if let Some(inner_value) = auth_context_cache.get() {
            return Ok(inner_value.clone());
        }

        let token = Self::execute_kql_mgmt_query(self.client.clone()).await?;
        auth_context_cache.update(Some(token.clone()));

        Ok(token)
    }
}
