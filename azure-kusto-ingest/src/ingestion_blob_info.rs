use serde::Serialize;
use uuid::Uuid;

use crate::{
    data_format::DataFormat, descriptors::BlobDescriptor,
    ingestion_properties::IngestionProperties,
    resource_manager::authorization_context::KustoIdentityToken,
};

use time::{
    format_description::well_known::{iso8601, Iso8601},
    OffsetDateTime,
};
/// The [DEFAULT](iso8601::Config::DEFAULT) ISO8601 format that the time crate serializes to uses a 6 digit year,
/// Here we create our own serializer function that uses a 4 digit year which is exposed as `kusto_ingest_iso8601_format`
const CONFIG: iso8601::EncodedConfig = iso8601::Config::DEFAULT
    .set_year_is_six_digits(false)
    .encode();
const FORMAT: Iso8601<CONFIG> = Iso8601::<CONFIG>;
time::serde::format_description!(kusto_ingest_iso8601_format, OffsetDateTime, FORMAT);

/// Message to be serialized as JSON and sent to the ingestion queue
///
/// Basing the ingestion message on
/// https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-rest#ingestion-message-internal-structure
#[derive(Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct QueuedIngestionMessage {
    /// Message identifier for this upload
    id: Uuid,
    /// Path (URI) to the blob.
    /// This should include any SAS token required to access the blob, or hints to use managed identity auth.
    /// Extra permissions are required if the `RetainBlobOnSuccess` option is not true so that the ingestion service can delete the blob once it has completed ingesting the data.
    blob_path: String,
    // Name of the Kusto database the data will ingest into
    database_name: String,
    // Name of the Kusto table the the data will ingest into
    table_name: String,
    /// Size of the uncompressed data in bytes.
    /// Providing this value allows the ingestion service to optimize ingestion by potentially aggregating multiple blobs.
    /// Although this property is optional, it is recommended to provide the size as otherwise the service will access the blob just to retrieve the size.
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_data_size: Option<u64>,
    /// If set to `true`, the blob won't be deleted once ingestion is successfully completed.
    /// Default is `false` when this property is not specified. Note that this has implications on permissions required against the blob.
    #[serde(skip_serializing_if = "Option::is_none")]
    retain_blob_on_success: Option<bool>,
    /// If set to `true`, any server side aggregation will be skipped - thus overriding the batching policy. Default is `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    flush_immediately: Option<bool>,
    #[serde(with = "kusto_ingest_iso8601_format")]
    source_message_creation_time: OffsetDateTime,
    // source_message_creation_time: DateTime<Utc>,
    // Extra properties added to the ingestion command
    additional_properties: AdditionalProperties,
}

impl QueuedIngestionMessage {
    pub(crate) fn new(
        blob_descriptor: &BlobDescriptor,
        ingestion_properties: &IngestionProperties,
        authorization_context: KustoIdentityToken,
    ) -> Self {
        let additional_properties = AdditionalProperties {
            authorization_context,
            data_format: ingestion_properties.data_format.clone(),
        };

        Self {
            id: blob_descriptor.source_id,
            blob_path: blob_descriptor.uri(),
            raw_data_size: blob_descriptor.size,
            database_name: ingestion_properties.database_name.clone(),
            table_name: ingestion_properties.table_name.clone(),
            retain_blob_on_success: ingestion_properties.retain_blob_on_success,
            flush_immediately: ingestion_properties.flush_immediately,
            source_message_creation_time: OffsetDateTime::now_utc(),
            additional_properties,
        }
    }
}

/// Additional properties to be added to the ingestion message
/// This struct is modelled on: https://learn.microsoft.com/en-us/azure/data-explorer/ingestion-properties
#[derive(Serialize, Clone, Debug)]
struct AdditionalProperties {
    /// Authorization string obtained from Kusto to allow for ingestion
    #[serde(rename = "authorizationContext")]
    authorization_context: KustoIdentityToken,
    #[serde(rename = "format")]
    data_format: DataFormat,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_custom_iso8601_serialization() {
        #[derive(Serialize, Debug)]
        struct TestTimeSerialize {
            #[serde(with = "kusto_ingest_iso8601_format")]
            customised_time_format: time::OffsetDateTime,
        }

        let test_message = TestTimeSerialize {
            customised_time_format: time::OffsetDateTime::from_unix_timestamp_nanos(
                1_234_567_890_123_456_789,
            )
            .unwrap(),
        };

        let serialized_message = serde_json::to_string(&test_message).unwrap();
        assert_eq!(
            serialized_message,
            "{\"customised_time_format\":\"2009-02-13T23:31:30.123456789Z\"}"
        );
    }
}
