#![cfg(feature = "mock_transport_framework")]
use dotenv::dotenv;
mod setup;

#[tokio::test]
async fn create_query_delete_table() {
    dotenv().ok();

    let (client, database) = setup::create_kusto_client("data_create_query_delete_table")
        .await
        .unwrap();

    let query = ".set KustoRsTest <| let text=\"Hello, World!\"; print str=text";
    let response = client
        .execute_command(&database, query)
        .into_future()
        .await
        .unwrap();

    assert_eq!(response.table_count(), 1);

    let query = ".show tables | where TableName == \"KustoRsTest\"";
    let response = client
        .execute_command(&database, query)
        .into_future()
        .await
        .unwrap();

    assert_eq!(response.table_count(), 4);

    let query = "KustoRsTest | take 1";
    let response = client
        .execute_query(&database, query)
        .into_future()
        .await
        .unwrap();

    let results = response.into_primary_results().collect::<Vec<_>>();
    assert_eq!(results[0].rows.len(), 1);

    let query = ".drop table KustoRsTest | where TableName == \"KustoRsTest\"";
    let response = client
        .execute_command(&database, query)
        .into_future()
        .await
        .unwrap();

    assert_eq!(response.tables[0].rows.len(), 0)
}
