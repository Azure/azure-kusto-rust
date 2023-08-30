use azure_core::ClientOptions;

/// Allows configurability of ClientOptions for the storage clients used within [QueuedIngestClient](crate::queued_ingest::QueuedIngestClient)
#[derive(Clone, Default)]
pub struct QueuedIngestClientOptions {
    pub queue_service: ClientOptions,
    pub blob_service: ClientOptions,
}

impl From<ClientOptions> for QueuedIngestClientOptions {
    /// Creates a `QueuedIngestClientOptions` struct where the same [ClientOptions] are used for all services
    fn from(client_options: ClientOptions) -> Self {
        Self {
            queue_service: client_options.clone(),
            blob_service: client_options,
        }
    }
}

/// Builder for [QueuedIngestClientOptions], call `build()` to create the [QueuedIngestClientOptions]
#[derive(Clone, Default)]
pub struct QueuedIngestClientOptionsBuilder {
    queue_service: ClientOptions,
    blob_service: ClientOptions,
}

impl QueuedIngestClientOptionsBuilder {
    pub fn new() -> Self {
        Self {
            queue_service: ClientOptions::default(),
            blob_service: ClientOptions::default(),
        }
    }

    pub fn with_queue_service(mut self, queue_service: ClientOptions) -> Self {
        self.queue_service = queue_service;
        self
    }

    pub fn with_blob_service(mut self, blob_service: ClientOptions) -> Self {
        self.blob_service = blob_service;
        self
    }

    pub fn build(self) -> QueuedIngestClientOptions {
        QueuedIngestClientOptions {
            queue_service: self.queue_service,
            blob_service: self.blob_service,
        }
    }
}
