use std::sync::Arc;

use crate::client_options::QueuedIngestClientOptions;

use super::{
    cache::{Cached, ThreadSafeCachedValue},
    resource_uri::{ClientFromResourceUri, ResourceUri},
    utils, RESOURCE_REFRESH_PERIOD,
};
use azure_core::ClientOptions;
use azure_kusto_data::{models::TableV1, prelude::KustoClient};
use azure_storage_blobs::prelude::ContainerClient;
use azure_storage_queues::QueueClient;
use serde_json::Value;
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
pub enum IngestionResourceError {
    #[error("{column_name} column is missing in the table")]
    ColumnNotFoundError { column_name: String },

    #[error("Response returned from Kusto could not be parsed as a string: {0}")]
    ParseAsStringError(Value),

    #[error("No {0} resources found in the table")]
    NoResourcesFound(String),

    #[error(transparent)]
    KustoError(#[from] azure_kusto_data::error::Error),

    #[error(transparent)]
    ResourceUriError(#[from] super::resource_uri::ResourceUriError),

    #[error("Kusto expected a table containing ingestion resource results, found no tables")]
    NoTablesFound,
}

type Result<T> = std::result::Result<T, IngestionResourceError>;

fn get_column_index(table: &TableV1, column_name: &str) -> Result<usize> {
    utils::get_column_index(table, column_name).ok_or(IngestionResourceError::ColumnNotFoundError {
        column_name: column_name.to_string(),
    })
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
            let x = r[storage_root_index].as_str().ok_or(
                IngestionResourceError::ParseAsStringError(r[storage_root_index].clone()),
            )?;
            ResourceUri::try_from(x).map_err(IngestionResourceError::ResourceUriError)
        })
        .collect();

    if resource_uris.is_empty() {
        return Err(IngestionResourceError::NoResourcesFound(resource_name));
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
    type Error = IngestionResourceError;

    /// Attempts to create a new InnerIngestClientResources from the given [TableV1] and [QueuedIngestClientOptions]
    fn try_from(
        (table, client_options): (&TableV1, &QueuedIngestClientOptions),
    ) -> std::result::Result<Self, Self::Error> {
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
    resources: ThreadSafeCachedValue<Option<InnerIngestClientResources>>,
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

        let new_resources = results
            .tables
            .first()
            .ok_or(IngestionResourceError::NoTablesFound)?;

        InnerIngestClientResources::try_from((new_resources, &self.client_options))
    }

    /// Gets the latest resources either from cache, or fetching from Kusto and updating the cached resources
    pub async fn get(&self) -> Result<InnerIngestClientResources> {
        // first, try to get the resources from the cache by obtaining a read lock
        {
            let resources = self.resources.read().await;
            if !resources.is_expired() {
                if let Some(inner_value) = resources.get() {
                    return Ok(inner_value.clone());
                }
            }
        }

        // obtain a write lock to refresh the kusto response
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
