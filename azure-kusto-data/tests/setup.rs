#![cfg(feature = "mock_transport_framework")]

use azure_kusto_data::prelude::*;
use dotenv::dotenv;

#[must_use]
pub fn create_kusto_client() -> (KustoClient, String) {
    dotenv().ok();
    (
        ConnectionString::with_default_auth(
            std::env::var("KUSTO_CLUSTER_URL").expect("Set env variable KUSTO_CLUSTER_URL first!"),
        )
        .try_into()
        .expect("Failed to create KustoClient"),
        std::env::var("KUSTO_DATABASE").expect("Set env variable KUSTO_DATABASE first!"),
    )
}
