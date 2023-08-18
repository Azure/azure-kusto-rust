use uuid::Uuid;

pub enum IngestionStatus {
    // The ingestion was queued.
    Queued,
    // The ingestion was successfully streamed
    Success
}

// The result of an ingestion.
pub struct IngestionResult {
    // Will be `Queued` if the ingestion is queued, or `Success` if the ingestion is streaming and successful.
    status: IngestionStatus,
    // The name of the database where the ingestion was performed.
    database: String,
    // The name of the table where the ingestion was performed.
    table: String,
    // The source id of the ingestion.
    source_id: Uuid,
    // The blob uri of the ingestion, if exists.
    blob_uri: Option<String>
}

impl IngestionResult {
    pub fn new(
        status: IngestionStatus,
        database: &String,
        table: &String,
        source_id: Uuid,
        blob_uri: Option<String>,
    ) -> Self {
        Self {
            status,
            database: database.clone(),
            table: table.clone(),
            source_id,
            blob_uri,
        }
    }
}
