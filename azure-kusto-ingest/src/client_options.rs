use azure_core::ClientOptions;

/// Allows configurability of ClientOptions for the storage clients used within [QueuedIngestClient](crate::queued_ingest::QueuedIngestClient)
#[derive(Clone, Default)]
pub struct QueuedIngestClientOptions {
    pub queue_service_options: ClientOptions,
    pub blob_service_options: ClientOptions,
}

impl From<ClientOptions> for QueuedIngestClientOptions {
    /// Creates a `QueuedIngestClientOptions` struct where the same [ClientOptions] are used for all services
    fn from(client_options: ClientOptions) -> Self {
        Self {
            queue_service_options: client_options.clone(),
            blob_service_options: client_options,
        }
    }
}

/// Builder for [QueuedIngestClientOptions], call `build()` to create the [QueuedIngestClientOptions]
#[derive(Clone, Default)]
pub struct QueuedIngestClientOptionsBuilder {
    queue_service_options: ClientOptions,
    blob_service_options: ClientOptions,
}

impl QueuedIngestClientOptionsBuilder {
    pub fn new() -> Self {
        Self {
            queue_service_options: ClientOptions::default(),
            blob_service_options: ClientOptions::default(),
        }
    }

    pub fn with_queue_service_options(mut self, queue_service_options: ClientOptions) -> Self {
        self.queue_service_options = queue_service_options;
        self
    }

    pub fn with_blob_service_options(mut self, blob_service_options: ClientOptions) -> Self {
        self.blob_service_options = blob_service_options;
        self
    }

    pub fn build(self) -> QueuedIngestClientOptions {
        QueuedIngestClientOptions {
            queue_service_options: self.queue_service_options,
            blob_service_options: self.blob_service_options,
        }
    }
}
