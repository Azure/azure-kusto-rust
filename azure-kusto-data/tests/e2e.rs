#![cfg(feature = "mock_transport_framework")]
mod setup;

#[tokio::test]
async fn create_query_delete_table() {
    let (client, database) = setup::create_kusto_client("data_create_query_delete_table");

    let query = ".set KustoRsTest <| let text=\"Hello, World!\"; print str=text";
    let response = client
        .execute_command(database.clone(), query)
        .into_future()
        .await
        .expect("Failed to run query");

    assert_eq!(response.table_count(), 1);

    let query = ".show tables | where TableName == \"KustoRsTest\"";
    let response = client
        .execute_command(database.clone(), query)
        .into_future()
        .await
        .expect("Failed to run query");

    assert_eq!(response.table_count(), 4);

    let query = "KustoRsTest | take 1";
    let response = client
        .execute_query(database.clone(), query)
        .into_future()
        .await
        .expect("Failed to run query");

    let results = response.into_primary_results().collect::<Vec<_>>();
    assert_eq!(results[0].rows.len(), 1);

    let query = ".drop table KustoRsTest | where TableName == \"KustoRsTest\"";
    let response = client
        .execute_command(database.clone(), query)
        .into_future()
        .await
        .expect("Failed to run query");

    assert_eq!(response.tables[0].rows.len(), 0);
}
