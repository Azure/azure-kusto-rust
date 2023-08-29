use std::{sync::Arc, time::Duration};

pub mod authorization_context;
pub mod cache;
pub mod ingest_client_resources;
pub mod resource_uri;

use anyhow::Result;
use azure_kusto_data::prelude::KustoClient;

use azure_storage_queues::QueueClient;

use crate::client_options::QueuedIngestClientOptions;

use self::{
    authorization_context::{AuthorizationContext, KustoIdentityToken},
    ingest_client_resources::IngestClientResources,
};

pub const RESOURCE_REFRESH_PERIOD: Duration = Duration::from_secs(60 * 60);

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
        Ok(self
            .ingest_client_resources
            .get()
            .await?
            .secured_ready_for_aggregation_queues)
    }

    // pub async fn temp_storage(&self) -> Result<Vec<ContainerClient>> {
    //     Ok(self.ingest_client_resources.get().await?.temp_storage)
    // }

    pub async fn authorization_context(&self) -> Result<KustoIdentityToken> {
        self.authorization_context.get().await
    }
}
