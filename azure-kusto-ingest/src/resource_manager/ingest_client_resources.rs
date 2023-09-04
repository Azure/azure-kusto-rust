use std::sync::Arc;

use crate::client_options::QueuedIngestClientOptions;

use super::{
    cache::{Cached, Refreshing},
    resource_uri::{ClientFromResourceUri, ResourceUri},
    RESOURCE_REFRESH_PERIOD,
};
use anyhow::Result;
use azure_core::ClientOptions;
use azure_kusto_data::{models::TableV1, prelude::KustoClient};
use azure_storage_blobs::prelude::ContainerClient;
use azure_storage_queues::QueueClient;
use tokio::sync::RwLock;

/// Helper to get a column index from a table
// TODO: this could be moved upstream into Kusto Data - would likely result in a change to the API of this function to return an Option<usize>
fn get_column_index(table: &TableV1, column_name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|c| c.column_name == column_name)
        .ok_or(anyhow::anyhow!(
            "{} column is missing in the table",
            column_name
        ))
}

/// Helper to get a resource URI from a table, erroring if there are no resources of the given name
fn get_resource_by_name(table: &TableV1, resource_name: String) -> Result<Vec<ResourceUri>> {
    let storage_root_index = get_column_index(table, "StorageRoot")?;
    let resource_type_name_index = get_column_index(table, "ResourceTypeName")?;

    let resource_uris: Vec<Result<ResourceUri>> = table
        .rows
        .iter()
        .filter(|r| r[resource_type_name_index] == resource_name)
        .map(|r| {
            ResourceUri::try_from(r[storage_root_index].as_str().ok_or(anyhow::anyhow!(
                "Response returned from Kusto could not be parsed as a string"
            ))?)
        })
        .collect();

    if resource_uris.is_empty() {
        return Err(anyhow::anyhow!(
            "No {} resources found in the table",
            resource_name
        ));
    }

    resource_uris.into_iter().collect()
}

/// Helper to turn a vector of resource URIs into a vector of Azure clients of type T with the provided [ClientOptions]
fn create_clients_vec<T>(resource_uris: &[ResourceUri], client_options: &ClientOptions) -> Vec<T>
where
    T: ClientFromResourceUri,
{
    resource_uris
        .iter()
        .map(|uri| T::create_client(uri.clone(), client_options.clone()))
        .collect()
}

/// Storage of the clients required for ingestion
#[derive(Debug, Clone)]
pub struct InnerIngestClientResources {
    pub ingestion_queues: Vec<QueueClient>,
    pub temp_storage_containers: Vec<ContainerClient>,
}

impl TryFrom<(&TableV1, &QueuedIngestClientOptions)> for InnerIngestClientResources {
    type Error = anyhow::Error;

    /// Attempts to create a new InnerIngestClientResources from the given [TableV1] and [QueuedIngestClientOptions]
    fn try_from((table, client_options): (&TableV1, &QueuedIngestClientOptions)) -> Result<Self> {
        let secured_ready_for_aggregation_queues =
            get_resource_by_name(table, "SecuredReadyForAggregationQueue".to_string())?;
        let temp_storage = get_resource_by_name(table, "TempStorage".to_string())?;

        Ok(Self {
            ingestion_queues: create_clients_vec(
                &secured_ready_for_aggregation_queues,
                &client_options.queue_service,
            ),
            temp_storage_containers: create_clients_vec(
                &temp_storage,
                &client_options.blob_service,
            ),
        })
    }
}

pub struct IngestClientResources {
    client: KustoClient,
    resources: Refreshing<Option<InnerIngestClientResources>>,
    client_options: QueuedIngestClientOptions,
}

impl IngestClientResources {
    pub fn new(client: KustoClient, client_options: QueuedIngestClientOptions) -> Self {
        Self {
            client,
            resources: Arc::new(RwLock::new(Cached::new(None, RESOURCE_REFRESH_PERIOD))),
            client_options,
        }
    }

    /// Executes a KQL management query that retrieves resource URIs for the various Azure resources used for ingestion
    async fn query_ingestion_resources(&self) -> Result<InnerIngestClientResources> {
        let results = self
            .client
            .execute_command("NetDefaultDB", ".get ingestion resources", None)
            .await?;

        let new_resources = results.tables.first().ok_or(anyhow::anyhow!(
            "Kusto expected a table containing ingestion resource results, found no tables",
        ))?;

        InnerIngestClientResources::try_from((new_resources, &self.client_options))
    }

    /// Gets the latest resources either from cache, or fetching from Kusto and updating the cached resources
    pub async fn get(&self) -> Result<InnerIngestClientResources> {
        let resources = self.resources.read().await;
        if !resources.is_expired() {
            if let Some(inner_value) = resources.get() {
                return Ok(inner_value.clone());
            }
        }

        // otherwise, drop the read lock and get a write lock to refresh the kusto response
        drop(resources);
        let mut resources = self.resources.write().await;

        // check again in case another thread refreshed while we were waiting on the write lock
        if !resources.is_expired() {
            if let Some(inner_value) = resources.get() {
                return Ok(inner_value.clone());
            }
        }

        let new_resources = self.query_ingestion_resources().await?;
        resources.update(Some(new_resources.clone()));

        Ok(new_resources)
    }
}
