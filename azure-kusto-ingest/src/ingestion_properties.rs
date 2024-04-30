use crate::data_format::DataFormat;

/// Properties of ingestion that can be used when ingesting data into Kusto allowing for customisation of the ingestion process
#[derive(Clone, Debug, Default)]
pub struct IngestionProperties {
    /// Name of the database to ingest into
    pub database_name: String,
    /// Name of the table to ingest into
    pub table_name: String,
    /// Whether the blob is retained after ingestion.
    /// Note that the default when not provided is `false`, meaning that Kusto will attempt to delete the blob upon ingestion.
    /// This will only be successful if provided sufficient permissions on the blob
    pub retain_blob_on_success: Option<bool>,
    /// Format of the data being ingested
    pub data_format: DataFormat,
    /// If set to `true`, any aggregation will be skipped. Default is `false`
    pub flush_immediately: Option<bool>,
}
