use crate::data_format::DataFormat;
use chrono::{DateTime, Utc};

/// Properties of ingestion that can be used when ingesting data into Kusto allowing for customisation of the ingestion process
#[derive(Clone, Debug, Default)]
pub struct IngestionProperties {
    /// Name of the database to ingest into
    pub database_name: String,
    /// Name of the table to ingest into
    pub table_name: String,
    /// Whether the blob is retained after ingestion, note that this requires extra permissions
    pub retain_blob_on_success: Option<bool>,
    /// Format of the data being ingested
    pub data_format: DataFormat,
    /// If set to `true`, any aggregation will be skipped. Default is `false`
    pub flush_immediately: Option<bool>,
    /// Allows for configurability of the `creationTime` property
    pub creation_time: Option<DateTime<Utc>>,
}
