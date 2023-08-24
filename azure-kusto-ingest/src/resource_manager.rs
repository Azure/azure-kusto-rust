use std::{sync::Arc, time::Duration};

pub mod authorization_context;
pub mod cache;
pub mod ingest_client_resources;
pub mod resource_uri;

use anyhow::{Ok, Result};
use azure_core::ClientOptions;
use azure_kusto_data::prelude::KustoClient;
use tokio::sync::RwLock;

use azure_data_tables::prelude::TableClient;
use azure_storage_blobs::prelude::ContainerClient;
use azure_storage_queues::QueueClient;

use crate::queued_ingest::QueuedIngestClientOptions;

use self::{
    authorization_context::AuthorizationContext,
    cache::{Cached, Refreshing},
    resource_uri::{ClientFromResourceUri, ResourceUri},
};

use self::ingest_client_resources::RawIngestClientResources;

pub(crate) const RESOURCE_REFRESH_PERIOD: Duration = Duration::from_secs(60 * 60);

pub struct InnerIngestClientResources {
    kusto_response: Option<RawIngestClientResources>,
    secured_ready_for_aggregation_queues: Vec<QueueClient>,
    temp_storage: Vec<ContainerClient>,
    ingestions_status_tables: Vec<TableClient>,
    successful_ingestions_queues: Vec<QueueClient>,
    failed_ingestions_queues: Vec<QueueClient>,
}

impl InnerIngestClientResources {
    pub fn new() -> Self {
        Self {
            kusto_response: None,
            secured_ready_for_aggregation_queues: Vec::new(),
            temp_storage: Vec::new(),
            ingestions_status_tables: Vec::new(),
            successful_ingestions_queues: Vec::new(),
            failed_ingestions_queues: Vec::new(),
        }
    }
}

pub struct IngestClientResources {
    client: KustoClient,
    resources: Refreshing<InnerIngestClientResources>,
    client_options: QueuedIngestClientOptions,
}

impl IngestClientResources {
    pub fn new(client: KustoClient, client_options: QueuedIngestClientOptions) -> Self {
        Self {
            client,
            resources: Arc::new(RwLock::new(Cached::new(
                InnerIngestClientResources::new(),
                RESOURCE_REFRESH_PERIOD,
            ))),
            client_options,
        }
    }

    // TODO: Logic to get the Kusto identity token from Kusto management endpoint - handle any validation of the response from the query here
    /// Executes a KQL management query that retrieves resource URIs for the various Azure resources used for ingestion
    async fn execute_kql_mgmt_query(client: KustoClient) -> Result<RawIngestClientResources> {
        let results = client
            .execute_command("NetDefaultDB", ".get ingestion resources", None)
            .await?;

        let table = match results.tables.first() {
            Some(a) => a,
            None => {
                return Err(anyhow::anyhow!(
                    "Kusto expected a table containing ingestion resource results, found no tables",
                ))
            }
        };

        RawIngestClientResources::try_from(table)
    }

    fn create_clients_vec<T>(resource_uris: &[ResourceUri], client_options: ClientOptions) -> Vec<T>
    where
        T: ClientFromResourceUri,
    {
        resource_uris
            .iter()
            .map(|uri| T::create_client(uri.clone(), client_options.clone()))
            .collect()
    }

    fn update_clients_vec<T>(
        current_resources: Vec<T>,
        resource_uris: Vec<ResourceUri>,
        client_options: ClientOptions,
    ) -> Vec<T>
    where
        T: ClientFromResourceUri,
    {
        if !current_resources.is_empty() {
            Self::create_clients_vec(&resource_uris, client_options)
        } else {
            current_resources
        }
    }

    // 1. Get the kusto response
    // 2. Update the kusto response, and the dependent resources if they are not empty, do this by a hashmap on the URI returned
    // 3. Update the time
    // 4. Return the kusto response
    // As such, at any one time it is guaranteed that anything that has been queried before will be available and up to date
    // Anything that has not been queried before will be available to create, but not as Azure clients until explicitly queried
    ///
    async fn update_from_kusto(&self) -> Result<RawIngestClientResources> {
        let resources = self.resources.read().await;
        if !resources.is_expired() {
            if let Some(ref inner_value) = resources.get().kusto_response {
                return Ok(inner_value.clone());
            }
        }
        // otherwise, drop the read lock and get a write lock to refresh the kusto response
        drop(resources);
        let mut resources = self.resources.write().await;

        // check again in case another thread refreshed the while we were waiting on the write lock
        if let Some(inner_value) = &resources.get().kusto_response {
            return Ok(inner_value.clone());
        }

        let raw_ingest_client_resources = Self::execute_kql_mgmt_query(self.client.clone()).await?;
        let mut_resources = resources.get_mut();

        mut_resources.kusto_response = Some(raw_ingest_client_resources.clone());

        mut_resources.secured_ready_for_aggregation_queues = Self::update_clients_vec(
            mut_resources.secured_ready_for_aggregation_queues.clone(),
            raw_ingest_client_resources
                .secured_ready_for_aggregation_queues
                .clone(),
            self.client_options.queue_service.clone(),
        );
        mut_resources.temp_storage = Self::update_clients_vec(
            mut_resources.temp_storage.clone(),
            raw_ingest_client_resources.temp_storage.clone(),
            self.client_options.blob_service.clone(),
        );
        mut_resources.ingestions_status_tables = Self::update_clients_vec(
            mut_resources.ingestions_status_tables.clone(),
            raw_ingest_client_resources.ingestions_status_tables.clone(),
            self.client_options.table_service.clone(),
        );
        mut_resources.successful_ingestions_queues = Self::update_clients_vec(
            mut_resources.successful_ingestions_queues.clone(),
            raw_ingest_client_resources
                .successful_ingestions_queues
                .clone(),
            self.client_options.queue_service.clone(),
        );
        mut_resources.failed_ingestions_queues = Self::update_clients_vec(
            mut_resources.failed_ingestions_queues.clone(),
            raw_ingest_client_resources.failed_ingestions_queues.clone(),
            self.client_options.queue_service.clone(),
        );
        Ok(raw_ingest_client_resources)
    }

    // Logic here
    // Get a read lock, try and return the secured ready for aggregation queues
    // If they are not empty, return them
    // Otherwise, drop the read lock and get a write lock
    // Check again if they are empty, if not return them assuming something has changed in between
    // Otherwise, get the kusto response, create the queues
    // Store the queues, and also return them
    pub async fn get_clients<T, F, Fx, Fy>(
        &self,
        field_fn: F,
        create_client_vec_fn: Fx,
        set_value: Fy,
        client_options: ClientOptions,
    ) -> Result<Vec<T>>
    where
        F: Fn(&InnerIngestClientResources) -> &Vec<T>,
        Fx: Fn(&RawIngestClientResources) -> &Vec<ResourceUri>,
        Fy: Fn(&mut InnerIngestClientResources, &Vec<T>),
        T: ClientFromResourceUri + Clone,
    {
        let resources = self.resources.read().await;
        if !resources.is_expired() {
            let vecs = field_fn(resources.get());
            if !vecs.is_empty() {
                return Ok(vecs.clone());
            }
        }

        drop(resources);

        let raw_ingest_client_resources = self.update_from_kusto().await?;

        let mut resources = self.resources.write().await;
        let vecs = field_fn(resources.get_mut());
        if !vecs.is_empty() {
            return Ok(vecs.clone());
        }

        // First time, so create the resources outside
        let mut_resources = resources.get_mut();
        let new_resources = Self::create_clients_vec(
            create_client_vec_fn(&raw_ingest_client_resources),
            client_options,
        );
        set_value(mut_resources, &new_resources);

        Ok(new_resources)
    }

    pub async fn get_secured_ready_for_aggregation_queues(&self) -> Result<Vec<QueueClient>> {
        self.get_clients(
            |resources| &resources.secured_ready_for_aggregation_queues,
            |resources| &resources.secured_ready_for_aggregation_queues,
            |mut_resources, new_resources| {
                mut_resources.secured_ready_for_aggregation_queues = new_resources.clone()
            },
            self.client_options.queue_service.clone(),
        )
        .await
    }

    // pub async fn get_temp_storage(&self) -> Result<Vec<ContainerClient>> {
    //     self.get_clients(
    //         |resources| &resources.temp_storage,
    //         |resources| &resources.temp_storage,
    //         |mut_resources, new_resources| mut_resources.temp_storage = new_resources.clone(),
    //         self.client_options.blob_service.clone(),
    //     )
    //     .await
    // }

    // pub async fn get_ingestions_status_tables(&self) -> Result<Vec<TableClient>> {
    //     self.get_clients(
    //         |resources| &resources.ingestions_status_tables,
    //         |resources| &resources.ingestions_status_tables,
    //         |mut_resources, new_resources| {
    //             mut_resources.ingestions_status_tables = new_resources.clone()
    //         },
    //         self.client_options.table_service.clone(),
    //     )
    //     .await
    // }

    // pub async fn get_successful_ingestions_queues(&self) -> Result<Vec<QueueClient>> {
    //     self.get_clients(
    //         |resources| &resources.successful_ingestions_queues,
    //         |resources| &resources.successful_ingestions_queues,
    //         |mut_resources, new_resources| {
    //             mut_resources.successful_ingestions_queues = new_resources.clone()
    //         },
    //         self.client_options.queue_service.clone(),
    //     )
    //     .await
    // }

    // pub async fn get_failed_ingestions_queues(&self) -> Result<Vec<QueueClient>> {
    //     self.get_clients(
    //         |resources| &resources.failed_ingestions_queues,
    //         |resources| &resources.failed_ingestions_queues,
    //         |mut_resources, new_resources| {
    //             mut_resources.failed_ingestions_queues = new_resources.clone()
    //         },
    //         self.client_options.queue_service.clone(),
    //     )
    //     .await
    // }
}

pub type KustoIdentityToken = String;

/// ResourceManager is a struct that keeps track of all the resources required for ingestion using the queued flavour
pub struct ResourceManager {
    ingest_client_resources: Arc<IngestClientResources>,
    authorization_context: Arc<AuthorizationContext>,
}

impl ResourceManager {
    /// Creates a new ResourceManager from the given [KustoClient] and the [QueuedIngestClientOptions] as provided by the user
    pub fn new(client: KustoClient, client_options: QueuedIngestClientOptions) -> Self {
        Self {
            ingest_client_resources: Arc::new(IngestClientResources::new(
                client.clone(),
                client_options,
            )),
            authorization_context: Arc::new(AuthorizationContext::new(client)),
        }
    }

    pub async fn secured_ready_for_aggregation_queues(&self) -> Result<Vec<QueueClient>> {
        self.ingest_client_resources
            .get_secured_ready_for_aggregation_queues()
            .await
    }

    // pub async fn temp_storage(&self) -> Result<Vec<ContainerClient>> {
    //     self.ingest_client_resources.get_temp_storage().await
    // }

    // pub async fn ingestions_status_tables(&self) -> Result<Vec<TableClient>> {
    //     self.ingest_client_resources
    //         .get_ingestions_status_tables()
    //         .await
    // }

    // pub async fn successful_ingestions_queues(&self) -> Result<Vec<QueueClient>> {
    //     self.ingest_client_resources
    //         .get_successful_ingestions_queues()
    //         .await
    // }

    // pub async fn failed_ingestions_queues(&self) -> Result<Vec<QueueClient>> {
    //     self.ingest_client_resources
    //         .get_failed_ingestions_queues()
    //         .await
    // }

    pub async fn authorization_context(&self) -> Result<KustoIdentityToken> {
        self.authorization_context.get().await
    }
}
