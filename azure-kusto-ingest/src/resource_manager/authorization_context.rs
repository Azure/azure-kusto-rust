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
    /// A client against a Kusto ingestion cluster
    client: KustoClient,
    /// Cache of the Kusto identity token
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
    async fn query_kusto_identity_token(&self) -> Result<KustoIdentityToken> {
        let results = self
            .client
            .execute_command("NetDefaultDB", ".get kusto identity token", None)
            .await?;

        // Check that there is only 1 table in the results returned by the query
        let table = match &results.tables[..] {
            [a] => a,
            _ => {
                return Err(anyhow::anyhow!(
                    "Kusto Expected 1 table in results, found {}",
                    results.tables.len()
                ))
            }
        };

        // Check that a column in this table actually exists called `AuthorizationContext`
        let index = table
            .columns
            .iter()
            .position(|c| c.column_name == "AuthorizationContext")
            .ok_or(anyhow::anyhow!(
                "AuthorizationContext column is missing in the table"
            ))?;

        // Check that there is only 1 row in the table, and that the value in the first row at the given index is not empty
        let kusto_identity_token = match &table.rows[..] {
            [row] => row.get(index).ok_or(anyhow::anyhow!(
                "Kusto response did not contain a value in the first row at position {}",
                index
            ))?,
            _ => {
                return Err(anyhow::anyhow!(
                    "Kusto Expected 1 row in results, found {}",
                    table.rows.len()
                ))
            }
        };

        // Convert the JSON string into a Rust string
        let kusto_identity_token = kusto_identity_token.as_str().ok_or(anyhow::anyhow!(
            "Kusto response did not contain a string value"
        ))?;

        if kusto_identity_token.chars().all(char::is_whitespace) {
            return Err(anyhow::anyhow!("Kusto identity token is empty"));
        }

        Ok(kusto_identity_token.to_string())
    }

    /// Fetches the latest Kusto identity token, either retrieving from cache if valid, or by executing a KQL query
    pub async fn get(&self) -> Result<KustoIdentityToken> {
        // Attempt to get the token from the cache
        let auth_context_cache = self.auth_context_cache.read().await;
        if !auth_context_cache.is_expired() {
            if let Some(token) = auth_context_cache.get() {
                return Ok(token.clone());
            }
        }
        // Drop the read lock and get a write lock to refresh the token
        drop(auth_context_cache);
        let mut auth_context_cache = self.auth_context_cache.write().await;

        // Again attempt to return from cache, check is done in case another thread
        // refreshed the token while we were waiting on the write lock
        if !auth_context_cache.is_expired() {
            if let Some(token) = auth_context_cache.get() {
                return Ok(token.clone());
            }
        }

        // Fetch new token from Kusto, update the cache, and return the token
        let token = self.query_kusto_identity_token().await?;
        auth_context_cache.update(Some(token.clone()));

        Ok(token)
    }
}
