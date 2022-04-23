#![cfg(feature = "mock_transport_framework")]
use dotenv::dotenv;
use std::fs;
use std::path::Path;
mod setup;

#[tokio::test]
#[ignore]
async fn create_query_delete_table() {
    dotenv().ok();

    let cargo_root = std::env::var("CARGO_MANIFEST_DIR").expect("Set by cargo");
    let kql_root = Path::new(&cargo_root).join("tests/inputs/e2e");

    let (client, database) = setup::create_kusto_client("data_create_query_delete_table")
        .await
        .unwrap();

    let filename = kql_root.join("01_prepare_table.kql");
    let query = fs::read_to_string(filename).expect("Something went wrong reading the file");
    let response = client
        .execute_command(&database, query)
        .into_future()
        .await
        .unwrap();

    println!("{:?}", response);

    let filename = kql_root.join("02_drop_table.kql");
    let query = fs::read_to_string(filename).expect("Something went wrong reading the file");
    let response = client
        .execute_command(&database, query)
        .into_future()
        .await
        .unwrap();

    println!("{:?}", response)
}
