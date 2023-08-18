use std::time::{Duration, Instant};

use anyhow::{Ok, Result};
use azure_kusto_data::{models::TableV1, prelude::KustoClient};
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{ClientBuilder, ContainerClient};
use url::Url;

use azure_storage_queues::{QueueClient, QueueServiceClientBuilder};

#[derive(Debug, Clone)]
pub struct ResourceUri {
    uri: String,
    // parsed_uri: Url,
    service_uri: String,
    object_name: String,
    sas_token: StorageCredentials,
}

impl ResourceUri {
    pub fn new(uri: String) -> Self {
        println!("uri: {:#?}", uri);
        let parsed_uri = Url::parse(&uri).unwrap();
        println!("parsed_uri: {:#?}", parsed_uri);

        let service_uri = parsed_uri.scheme().to_string()
            + "://"
            + parsed_uri.host_str().expect("We should get result here");
        let object_name = parsed_uri
            .path()
            .trim_start()
            .trim_start_matches("/")
            .to_string();
        let sas_token = parsed_uri
            .query()
            .expect("Returned URI should contain SAS token as query")
            .to_string();
        let sas_token = StorageCredentials::sas_token(sas_token).unwrap();

        Self {
            uri,
            // parsed_uri,
            service_uri,
            object_name,
            sas_token,
        }
    }

    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }

    pub fn service_uri(&self) -> &str {
        self.service_uri.as_str()
    }

    pub fn object_name(&self) -> &str {
        self.object_name.as_str()
    }

    pub fn sas_token(&self) -> &StorageCredentials {
        &self.sas_token
    }
}

impl From<&ResourceUri> for QueueClient {
    fn from(resource_uri: &ResourceUri) -> Self {
        let queue_service =
            QueueServiceClientBuilder::with_location(azure_storage::CloudLocation::Custom {
                uri: resource_uri.service_uri().to_string(),
                credentials: resource_uri.sas_token().clone(),
            })
            .build();

        queue_service.queue_client(resource_uri.object_name())
    }
}

impl From<&ResourceUri> for ContainerClient {
    fn from(resource_uri: &ResourceUri) -> Self {
        ClientBuilder::with_location(azure_storage::CloudLocation::Custom {
            uri: resource_uri.service_uri().to_string(),
            credentials: resource_uri.sas_token().clone(),
        })
        .container_client(resource_uri.object_name())
    }
}

fn get_resource_by_name(table: &TableV1, resource_name: String) -> Vec<ResourceUri> {
    let storage_root_index = table
        .columns
        .iter()
        .position(|c| c.column_name == "StorageRoot")
        .unwrap();
    let resource_type_name_index = table
        .columns
        .iter()
        .position(|c| c.column_name == "ResourceTypeName")
        .unwrap();

    println!("table: {:#?}", table);
    let resource_uris: Vec<ResourceUri> = table
        .rows
        .iter()
        .filter(|r| r[resource_type_name_index] == resource_name)
        .map(|r| {
            ResourceUri::new(
                r[storage_root_index]
                    .as_str()
                    .expect("We should get result here")
                    .to_string(),
            )
        })
        .collect();

    resource_uris
}

pub struct IngestClientResources {
    client: KustoClient,
    secured_ready_for_aggregation_queues: Vec<ResourceUri>,
    failed_ingestions_queues: Vec<ResourceUri>,
    successful_ingestions_queues: Vec<ResourceUri>,
    temp_storage: Vec<ResourceUri>,
    ingestions_status_tables: Vec<ResourceUri>,
    last_update: Option<Instant>,
    refresh_period: Duration,
}

impl IngestClientResources {
    pub fn new(client: KustoClient, refresh_period: Duration) -> Self {
        Self {
            client,
            secured_ready_for_aggregation_queues: Vec::new(),
            failed_ingestions_queues: Vec::new(),
            successful_ingestions_queues: Vec::new(),
            temp_storage: Vec::new(),
            ingestions_status_tables: Vec::new(),
            last_update: None,
            refresh_period,
        }
    }

    fn is_not_applicable(&self) -> bool {
        self.secured_ready_for_aggregation_queues.is_empty()
            || self.failed_ingestions_queues.is_empty()
            || self.successful_ingestions_queues.is_empty()
            || self.temp_storage.is_empty()
            || self.ingestions_status_tables.is_empty()
    }

    // TODO: figure out refresh logic
    // async fn refresh(&mut self) {
    //     self.get_ingest_client_resources().await
    //     // let interval = tokio::time::interval(self.refresh_period);
    //     // loop {
    //     //     match self.get_ingest_client_resources(self.client.clone()).await {
    //     //         Ok(_) => todo!(),
    //     //         Err(e) => println!("Error: {}", e),
    //     //     };

    //     //     interval.tick().await;
    //     // }

    //     // if self.last_update.is_none()
    //     //     || self.last_update.unwrap().elapsed() > self.refresh_period
    //     //     || self.is_not_applicable()
    //     // {
    //     //     self.get_ingest_client_resources(client).await?;
    //     //     self.last_update = Some(Instant::now());
    //     // }
    //     // Ok(())
    // }

    // async fn refresh(&mut self, client: KustoClient) -> Result<()> {
    //     if self.last_update.is_none()
    //         || self.last_update.unwrap().elapsed() > self.refresh_period
    //         || self.is_not_applicable()
    //     {
    //         self.get_ingest_client_resources(client).await?;
    //         self.last_update = Some(Instant::now());
    //     }
    //     Ok(())
    // }

    async fn get_ingest_client_resources(&mut self) -> Result<()> {
        let results = self
            .client
            .execute_command("NetDefaultDB", ".get ingestion resources", None)
            .await?;
        let table = results.tables.first().unwrap();

        self.secured_ready_for_aggregation_queues =
            get_resource_by_name(table, "SecuredReadyForAggregationQueue".to_string());
        self.failed_ingestions_queues =
            get_resource_by_name(table, "FailedIngestionsQueue".to_string());
        self.successful_ingestions_queues =
            get_resource_by_name(table, "SuccessfulIngestionsQueue".to_string());
        self.temp_storage = get_resource_by_name(table, "TempStorage".to_string());
        self.ingestions_status_tables =
            get_resource_by_name(table, "IngestionsStatusTable".to_string());

        Ok(())
    }
}

pub type KustoIdentityToken = String;
#[derive(Debug, Clone)]
pub struct AuthorizationContext {
    client: KustoClient,
    pub kusto_identity_token: KustoIdentityToken,
    last_update: Option<Instant>,
    refresh_period: Duration,
}

impl AuthorizationContext {
    pub fn new(client: KustoClient, refresh_period: Duration) -> Self {
        Self {
            client,
            kusto_identity_token: String::new(),
            last_update: None,
            refresh_period,
        }
    }

    // TODO: figure out refresh logic
    // Make this spawn a tokio task to refresh the token based on elapsed time
    async fn refresh(&mut self, client: KustoClient) -> Result<()> {
        if self.last_update.is_none()
            || self.kusto_identity_token.chars().all(char::is_whitespace)
            || self.last_update.unwrap().elapsed() > self.refresh_period
        {
            self.get_authorization_context(client).await?;
            self.last_update = Some(Instant::now());
        }
        Ok(())
    }

    async fn get_authorization_context(&mut self, client: KustoClient) -> Result<()> {
        let results = client
            .execute_command("NetDefaultDB", ".get kusto identity token", None)
            .await?;
        let table = results.tables.first().unwrap();

        println!("table: {:#?}", table);

        self.kusto_identity_token = table
            .rows
            .first()
            .unwrap()
            .first()
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        Ok(())
    }

    pub async fn kusto_identity_token(
        &mut self,
        client: KustoClient,
    ) -> Result<KustoIdentityToken> {
        self.refresh(client).await?;
        Ok(self.kusto_identity_token.clone())
    }
}

pub struct ResourceManager {
    // client: KustoClient,
    pub ingest_client_resources: IngestClientResources,
    pub authorization_context: AuthorizationContext,
}

impl ResourceManager {
    pub fn new(client: KustoClient, refresh_period: Duration) -> Self {
        Self {
            ingest_client_resources: IngestClientResources::new(client.clone(), refresh_period),
            authorization_context: AuthorizationContext::new(client, refresh_period),
        }
    }

    // pub async fn secured_ready_for_aggregation_queues(&mut self) -> Result<Vec<ResourceUri>> {
    pub async fn secured_ready_for_aggregation_queues(&mut self) -> Result<Vec<QueueClient>> {
        // TODO: proper refresh and caching logic so we don't need to generate new clients every time
        self.ingest_client_resources
            .get_ingest_client_resources()
            .await?;

        // We should return Azure SDK QueueClient's here.
        // Although it's recommended to share the same transport, we can't as the storage credentials (SAS tokens) differ per queue.
        // So the best we can do is store the individual QueueClient's so multiple requests

        let queue_uris = self
            .ingest_client_resources
            .secured_ready_for_aggregation_queues
            .clone();

        Ok(queue_uris.iter().map(|q| QueueClient::from(q)).collect())
    }

    pub async fn failed_ingestions_queues(&mut self) -> Result<Vec<QueueClient>> {
        // TODO: proper refresh and caching logic so we don't need to generate new clients every time
        self.ingest_client_resources
            .get_ingest_client_resources()
            .await?;

        let queue_uris = self
            .ingest_client_resources
            .failed_ingestions_queues
            .clone();

        Ok(queue_uris.iter().map(|q| QueueClient::from(q)).collect())
    }

    pub async fn successful_ingestions_queues(&mut self) -> Result<Vec<QueueClient>> {
        // TODO: proper refresh and caching logic so we don't need to generate new clients every time
        self.ingest_client_resources
            .get_ingest_client_resources()
            .await?;

        let queue_uris = self
            .ingest_client_resources
            .successful_ingestions_queues
            .clone();

        Ok(queue_uris.iter().map(|q| QueueClient::from(q)).collect())
    }

    pub async fn temp_storage(&mut self) -> Result<Vec<ContainerClient>> {
        // TODO: proper refresh and caching logic so we don't need to generate new clients every time
        self.ingest_client_resources
            .get_ingest_client_resources()
            .await?;

        let container_uris = self.ingest_client_resources.temp_storage.clone();

        Ok(container_uris
            .iter()
            .map(|c| ContainerClient::from(c))
            .collect())
    }

    // pub async fn ingestions_status_tables(
    //     &mut self,
    //     client: KustoClient,
    // ) -> Result<Vec<ResourceUri>> {
    //     self.refresh(client).await?;
    //     Ok(self.ingestions_status_tables.clone())
    // }

    // pub fn retrieve_service_type(self) -> ServiceType {
    //     unimplemented!()
    // }

    pub async fn authorization_context(&mut self) -> Result<&KustoIdentityToken> {
        // TODO: proper refresh and caching logic so we don't need to query Kusto for the token every time
        self.authorization_context
            .get_authorization_context(self.ingest_client_resources.client.clone())
            .await?;

        Ok(&self.authorization_context.kusto_identity_token)
    }
}
