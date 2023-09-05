use std::sync::Arc;

use anyhow::Result;
use azure_kusto_data::prelude::KustoClient;
use tokio::sync::RwLock;

use super::cache::{Cached, ThreadSafeCachedValue};
use super::utils::get_column_index;
use super::RESOURCE_REFRESH_PERIOD;

pub(crate) type KustoIdentityToken = String;

/// Logic to obtain a Kusto identity token from the management endpoint. This auth token is a temporary token
#[derive(Debug, Clone)]
pub(crate) struct AuthorizationContext {
    /// A client against a Kusto ingestion cluster
    client: KustoClient,
    /// Cache of the Kusto identity token
    token_cache: ThreadSafeCachedValue<Option<KustoIdentityToken>>,
}

impl AuthorizationContext {
    pub fn new(client: KustoClient) -> Self {
        Self {
            client,
            token_cache: Arc::new(RwLock::new(Cached::new(None, RESOURCE_REFRESH_PERIOD))),
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
        let index = get_column_index(table, "AuthorizationContext")?;

        // Check that there is only 1 row in the table, and that the value in the first row at the given index is not empty
        let token = match &table.rows[..] {
            [row] => row.get(index).ok_or(anyhow::anyhow!(
                "Kusto response did not contain a value in the first row at position {}",
                index
            ))?,
            _ => {
                return Err(anyhow::anyhow!(
                    "Kusto expected 1 row in results, found {}",
                    table.rows.len()
                ))
            }
        };

        // Convert the JSON string into a Rust string
        let token = token.as_str().ok_or(anyhow::anyhow!(
            "Kusto response did not contain a string value: {:?}",
            token
        ))?;

        if token.chars().all(char::is_whitespace) {
            return Err(anyhow::anyhow!("Kusto identity token is empty"));
        }

        Ok(token.to_string())
    }

    /// Fetches the latest Kusto identity token, either retrieving from cache if valid, or by executing a KQL query
    pub(crate) async fn get(&self) -> Result<KustoIdentityToken> {
        // Attempt to get the token from the cache
        let token_cache = self.token_cache.read().await;
        if !token_cache.is_expired() {
            if let Some(token) = token_cache.get() {
                return Ok(token.clone());
            }
        }
        // Drop the read lock and get a write lock to refresh the token
        drop(token_cache);
        let mut token_cache = self.token_cache.write().await;

        // Again attempt to return from cache, check is done in case another thread
        // refreshed the token while we were waiting on the write lock
        if !token_cache.is_expired() {
            if let Some(token) = token_cache.get() {
                return Ok(token.clone());
            }
        }

        // Fetch new token from Kusto, update the cache, and return the token
        let token = self.query_kusto_identity_token().await?;
        token_cache.update(Some(token.clone()));

        Ok(token)
    }
}
