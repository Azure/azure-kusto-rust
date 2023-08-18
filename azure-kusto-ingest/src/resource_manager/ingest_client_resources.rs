use super::resource_uri::ResourceUri;
use anyhow::Result;
use azure_kusto_data::models::TableV1;

#[derive(Debug, Clone)]
pub struct RawIngestClientResources {
    pub secured_ready_for_aggregation_queues: Vec<ResourceUri>,
    pub failed_ingestions_queues: Vec<ResourceUri>,
    pub successful_ingestions_queues: Vec<ResourceUri>,
    pub temp_storage: Vec<ResourceUri>,
    pub ingestions_status_tables: Vec<ResourceUri>,
}

impl RawIngestClientResources {
    fn get_resource_by_name(table: &TableV1, resource_name: String) -> Result<Vec<ResourceUri>> {
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
        let resource_uris: Result<Vec<ResourceUri>> = table
            .rows
            .iter()
            .filter(|r| r[resource_type_name_index] == resource_name)
            .map(|r| {
                ResourceUri::try_from(
                    r[storage_root_index]
                        .as_str()
                        .expect("We should get result here")
                        .to_string(),
                )
            })
            .collect();

        resource_uris
    }
}

impl TryFrom<&TableV1> for RawIngestClientResources {
    type Error = anyhow::Error;

    fn try_from(table: &TableV1) -> std::result::Result<Self, Self::Error> {
        let secured_ready_for_aggregation_queues =
            Self::get_resource_by_name(table, "SecuredReadyForAggregationQueue".to_string())?;
        let failed_ingestions_queues =
            Self::get_resource_by_name(table, "FailedIngestionsQueue".to_string())?;
        let successful_ingestions_queues =
            Self::get_resource_by_name(table, "SuccessfulIngestionsQueue".to_string())?;
        let temp_storage = Self::get_resource_by_name(table, "TempStorage".to_string())?;
        let ingestions_status_tables =
            Self::get_resource_by_name(table, "IngestionsStatusTable".to_string())?;

        Ok(Self {
            secured_ready_for_aggregation_queues,
            failed_ingestions_queues,
            successful_ingestions_queues,
            temp_storage,
            ingestions_status_tables,
        })
    }
}
