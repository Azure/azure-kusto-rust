use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub mod authorization_context;
pub mod cache;
pub mod ingest_client_resources;
pub mod resource_uri;

use anyhow::{Ok, Result};
use azure_kusto_data::prelude::KustoClient;
use tokio::sync::RwLock;

use azure_storage_queues::QueueClient;

use self::{
    authorization_context::AuthorizationContext,
    cache::{Cached, Refreshing},
};

use self::ingest_client_resources::RawIngestClientResources;

pub(crate) const RESOURCE_REFRESH_PERIOD: Duration = Duration::from_secs(60 * 60);

pub struct IngestClientResources {
    client: KustoClient,
    kusto_response: Refreshing<Option<RawIngestClientResources>>,
    secured_ready_for_aggregation_queues: Refreshing<Vec<QueueClient>>,
    // secured_ready_for_aggregation_queues: Vec<ResourceUri>,
    // failed_ingestions_queues: Vec<ResourceUri>,
    // successful_ingestions_queues: Vec<ResourceUri>,
    // temp_storage: Vec<ResourceUri>,
    // ingestions_status_tables: Vec<ResourceUri>,
}

impl IngestClientResources {
    pub fn new(client: KustoClient) -> Self {
        Self {
            client,
            kusto_response: Arc::new(RwLock::new(Cached::new(None, RESOURCE_REFRESH_PERIOD))),
            secured_ready_for_aggregation_queues: Arc::new(RwLock::new(Cached::new(
                Vec::new(),
                RESOURCE_REFRESH_PERIOD,
            ))),
            // secured_ready_for_aggregation_queues: Vec::new(),
            // failed_ingestions_queues: Vec::new(),
            // successful_ingestions_queues: Vec::new(),
            // temp_storage: Vec::new(),
            // ingestions_status_tables: Vec::new(),
            // last_update: None,
        }
    }

    // TODO: Logic to get the Kusto identity token from Kusto management endpoint - handle validation here
    async fn execute_kql_mgmt_query(client: KustoClient) -> Result<RawIngestClientResources> {
        let results = client
            .execute_command("NetDefaultDB", ".get ingestion resources", None)
            .await?;
        let table = results.tables.first().unwrap();

        println!("table: {:#?}", table);
        RawIngestClientResources::try_from(table)
    }

    async fn get(&self) -> Result<(RawIngestClientResources, Instant)> {
        let kusto_response = self.kusto_response.read().await;
        if !kusto_response.is_expired() {
            if let Some(inner_value) = kusto_response.get() {
                return Ok((
                    inner_value.clone(),
                    kusto_response.get_last_updated().clone(),
                ));
            }
        }
        // otherwise, drop the read lock and get a write lock to refresh the token
        drop(kusto_response);
        let mut kusto_response = self.kusto_response.write().await;

        // check again in case another thread refreshed the token while we were
        // waiting on the write lock
        if let Some(inner_value) = kusto_response.get() {
            return Ok((
                inner_value.clone(),
                kusto_response.get_last_updated().clone(),
            ));
        }

        let raw_ingest_client_resources = Self::execute_kql_mgmt_query(self.client.clone()).await?;
        let last_updated = Instant::now();
        kusto_response.update_with_time(
            Some(raw_ingest_client_resources.clone()),
            last_updated.clone(),
        );

        Ok((raw_ingest_client_resources, last_updated))
    }

    pub async fn get_ingestion_queues(&self) -> Result<Vec<QueueClient>> {
        let secured_ready_for_aggregation_queues =
            self.secured_ready_for_aggregation_queues.read().await;

        if !secured_ready_for_aggregation_queues.is_expired() {
            let vecs = secured_ready_for_aggregation_queues.get();
            if !vecs.is_empty() {
                return Ok(vecs.clone());
            }
        }

        drop(secured_ready_for_aggregation_queues);
        let mut secured_ready_for_aggregation_queues =
            self.secured_ready_for_aggregation_queues.write().await;

        let vecs = secured_ready_for_aggregation_queues.get();
        if !vecs.is_empty() {
            return Ok(vecs.clone());
        }

        let (raw_ingest_client_resources, last_updated) = self.get().await?;
        let queue_uris = raw_ingest_client_resources.secured_ready_for_aggregation_queues;
        let queue_clients: Vec<QueueClient> =
            queue_uris.iter().map(|q| QueueClient::from(q)).collect();

        secured_ready_for_aggregation_queues.update_with_time(queue_clients.clone(), last_updated);

        Ok(queue_clients)
    }
}

pub type KustoIdentityToken = String;

pub struct ResourceManager {
    ingest_client_resources: Arc<IngestClientResources>,
    authorization_context: Arc<AuthorizationContext>,
}

impl ResourceManager {
    pub fn new(client: KustoClient) -> Self {
        Self {
            ingest_client_resources: Arc::new(IngestClientResources::new(client.clone())),
            authorization_context: Arc::new(AuthorizationContext::new(client)),
        }
    }

    pub async fn secured_ready_for_aggregation_queues(&self) -> Result<Vec<QueueClient>> {
        self.ingest_client_resources.get_ingestion_queues().await
    }

    // pub async fn failed_ingestions_queues(&mut self) -> Result<Vec<QueueClient>> {
    //     // TODO: proper refresh and caching logic so we don't need to generate new clients every time
    //     self.ingest_client_resources
    //         .get_ingest_client_resources()
    //         .await?;

    //     let queue_uris = self
    //         .ingest_client_resources
    //         .failed_ingestions_queues
    //         .clone();

    //     Ok(queue_uris.iter().map(|q| QueueClient::from(q)).collect())
    // }

    // pub async fn successful_ingestions_queues(&mut self) -> Result<Vec<QueueClient>> {
    //     // TODO: proper refresh and caching logic so we don't need to generate new clients every time
    //     self.ingest_client_resources
    //         .get_ingest_client_resources()
    //         .await?;

    //     let queue_uris = self
    //         .ingest_client_resources
    //         .successful_ingestions_queues
    //         .clone();

    //     Ok(queue_uris.iter().map(|q| QueueClient::from(q)).collect())
    // }

    // pub async fn temp_storage(&mut self) -> Result<Vec<ContainerClient>> {
    //     // TODO: proper refresh and caching logic so we don't need to generate new clients every time
    //     self.ingest_client_resources
    //         .get_ingest_client_resources()
    //         .await?;

    //     let container_uris = self.ingest_client_resources.temp_storage.clone();

    //     Ok(container_uris
    //         .iter()
    //         .map(|c| ContainerClient::from(c))
    //         .collect())
    // }

    // pub async fn ingestions_status_tables(
    //     &mut self,
    //     client: KustoClient,
    // ) -> Result<Vec<ResourceUri>> {
    //     unimplemented!()
    // }

    // pub fn retrieve_service_type(self) -> ServiceType {
    //     unimplemented!()
    // }

    pub async fn authorization_context(&self) -> Result<KustoIdentityToken> {
        self.authorization_context.get().await
    }
}
