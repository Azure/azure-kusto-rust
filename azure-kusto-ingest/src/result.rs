use uuid::Uuid;

#[derive(Debug)]
pub enum IngestionStatus {
    // The ingestion was queued.
    Queued,
    // The ingestion was successfully streamed
    Success,
}

// The result of an ingestion.
#[derive(Debug)]
pub struct IngestionResult {
    // Will be `Queued` if the ingestion is queued, or `Success` if the ingestion is streaming and successful.
    pub status: IngestionStatus,
    // The name of the database where the ingestion was performed.
    pub database: String,
    // The name of the table where the ingestion was performed.
    pub table: String,
    // The source id of the ingestion.
    pub source_id: Uuid,
    // The blob uri of the ingestion, if exists.
    pub blob_uri: Option<String>,
}

impl IngestionResult {
    pub fn new(
        status: IngestionStatus,
        database: &str,
        table: &str,
        source_id: Uuid,
        blob_uri: Option<String>,
    ) -> Self {
        Self {
            status,
            database: database.to_owned(),
            table: table.to_owned(),
            source_id,
            blob_uri,
        }
    }
}
