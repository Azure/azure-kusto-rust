use std::{sync::Arc, time::Duration};

pub mod authorization_context;
pub mod cache;
pub mod ingest_client_resources;
pub mod resource_uri;
pub mod utils;

use azure_kusto_data::prelude::KustoClient;

use azure_storage_queues::QueueClient;

use crate::client_options::QueuedIngestClientOptions;

use self::{
    authorization_context::{AuthorizationContext, KustoIdentityToken},
    ingest_client_resources::IngestClientResources,
};

use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};

pub const RESOURCE_REFRESH_PERIOD: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, thiserror::Error)]
pub enum ResourceManagerError {
    #[error("Failed to obtain ingestion resources: {0}")]
    IngestClientResourcesError(#[from] ingest_client_resources::IngestionResourceError),

    #[error("Failed to obtain authorization token: {0}")]
    AuthorizationContextError(#[from] authorization_context::KustoIdentityTokenError),

    #[error("Failed to select a resource - no resources found")]
    NoResourcesFound,
}

type Result<T> = std::result::Result<T, ResourceManagerError>;

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

    /// Returns the latest [QueueClient]s ready for posting ingestion messages to
    async fn ingestion_queues(&self) -> Result<Vec<QueueClient>> {
        Ok(self.ingest_client_resources.get().await?.ingestion_queues)
    }

    /// Returns a [QueueClient] to ingest to.
    /// This is a random selection from the list of ingestion queues
    pub async fn ingestion_queue(&self) -> Result<QueueClient> {
        let ingestion_queues = self.ingestion_queues().await?;
        let selected_queue = select_random_resource(ingestion_queues)?;
        Ok(selected_queue.clone())
    }

    /// Returns the latest [KustoIdentityToken] to be added as an authorization context to ingestion messages
    pub async fn authorization_context(&self) -> Result<KustoIdentityToken> {
        self.authorization_context
            .get()
            .await
            .map_err(ResourceManagerError::AuthorizationContextError)
    }
}
/// Selects a random resource from the given list of resources
fn select_random_resource<T: Clone>(resources: Vec<T>) -> Result<T> {
    let mut rng: StdRng = SeedableRng::from_entropy();
    resources
        .choose(&mut rng)
        .ok_or(ResourceManagerError::NoResourcesFound)
        .cloned()
}

#[cfg(test)]
mod select_random_resource_tests {
    use super::*;

    #[test]
    fn single_resource() {
        const VALUE: i32 = 1;
        let resources = vec![VALUE];
        let selected_resource = select_random_resource(resources).unwrap();
        assert!(selected_resource == VALUE)
    }

    #[test]
    fn multiple_resources() {
        let resources = vec![1, 2, 3, 4, 5];
        let selected_resource = select_random_resource(resources.clone()).unwrap();
        assert!(resources.contains(&selected_resource));
    }

    #[test]
    fn no_resources() {
        let resources: Vec<i32> = vec![];
        let selected_resource = select_random_resource(resources);
        assert!(selected_resource.is_err());
        assert!(matches!(
            selected_resource.unwrap_err(),
            ResourceManagerError::NoResourcesFound
        ))
    }
}
