use azure_kusto_data::models::V2QueryResult;
use azure_kusto_data::prelude::*;
use clap::Parser;
use futures::{pin_mut, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use azure_kusto_data::types::timespan::{KustoDateTime, KustoTimespan};

/// Simple program to greet a person
#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Kusto cluster endpoint
    #[clap(env, long)]
    endpoint: String,

    /// Name of the database
    #[clap(env, long)]
    database: String,

    /// Query to execute
    #[clap(env, long)]
    query: String,

    #[clap(env = "AZURE_CLIENT_ID", long)]
    application_id: String,

    #[clap(env = "AZURE_CLIENT_SECRET", long)]
    application_key: String,

    #[clap(env = "AZURE_TENANT_ID", long)]
    tenant_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let kcsb = ConnectionString::with_application_auth(
        args.endpoint.clone(),
        args.application_id.clone(),
        args.application_key.clone(),
        args.tenant_id.clone(),
    );

    let client = KustoClient::try_from(kcsb).unwrap();

    non_progressive(&args, &client).await;

    progressive(&args, &client).await?;

    to_struct(&args, &client).await?;

    Ok(())
}

async fn progressive(args: &Args, client: &KustoClient) -> Result<(), Box<dyn Error>> {
    println!("Querying {} with streaming client", args.query);

    let stream = client
        .execute_query(
            args.database.clone(),
            args.query.clone(),
            Some(ClientRequestProperties::from(
                OptionsBuilder::default()
                    .with_results_progressive_enabled(true)
                    .build()
                    .unwrap(),
            )),
        )
        .into_stream()
        .await?;

    println!("Printing all streaming results");

    pin_mut!(stream);

    while let Some(table) = stream.try_next().await? {
        match table {
            V2QueryResult::DataSetHeader(header) => println!("header: {:#?}", header),
            V2QueryResult::DataTable(table) => println!("table: {:#?}", table),
            V2QueryResult::DataSetCompletion(completion) => {
                println!("completion: {:#?}", completion)
            }
            V2QueryResult::TableHeader(header) => println!("header: {:#?}", header),
            V2QueryResult::TableFragment(fragment) => println!("fragment: {:#?}", fragment),
            V2QueryResult::TableProgress(progress) => println!("progress: {:#?}", progress),
            V2QueryResult::TableCompletion(completion) => {
                println!("completion: {:#?}", completion)
            }
        }
    }

    Ok(())
}

async fn non_progressive(args: &Args, client: &KustoClient) {
    println!("Querying {} with regular client", args.query);

    let response = client
        .execute_query(
            args.database.clone(),
            args.query.clone(),
            Some(ClientRequestProperties::from(
                OptionsBuilder::default()
                    .with_results_progressive_enabled(false)
                    .build()
                    .unwrap(),
            )),
        )
        .await
        .unwrap();

    println!("All results:");

    for table in &response.results {
        match table {
            V2QueryResult::DataSetHeader(header) => println!("header: {:#?}", header),
            V2QueryResult::DataTable(table) => println!("table: {:#?}", table),
            V2QueryResult::DataSetCompletion(completion) => {
                println!("completion: {:#?}", completion)
            }
            V2QueryResult::TableHeader(header) => println!("header: {:#?}", header),
            V2QueryResult::TableFragment(fragment) => println!("fragment: {:#?}", fragment),
            V2QueryResult::TableProgress(progress) => println!("progress: {:#?}", progress),
            V2QueryResult::TableCompletion(completion) => {
                println!("completion: {:#?}", completion)
            }
        }
    }

    // Print the primary tables
    let primary_results = response.into_primary_results().collect::<Vec<_>>();
    println!("primary results: {:#?}", primary_results);
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Item {
    vnum: i32,
    vdec: String, // optionally, you can use a decimal type here
    vdate: KustoDateTime,
    vspan: KustoTimespan,
    vobj: Value,
    vb: bool,
    vreal: f64,
    vstr: String,
    vlong: i64,
    vguid: String, // optionally, you can use a guid type here
}

async fn to_struct(args: &Args, client: &KustoClient) -> Result<(), Box<dyn Error>> {
    let query = r#"datatable(vnum:int, vdec:decimal, vdate:datetime, vspan:timespan, vobj:dynamic, vb:bool, vreal:real, vstr:string, vlong:long, vguid:guid)
[
    1, decimal(2.00000000000001), datetime(2020-03-04T14:05:01.3109965Z), time(01:23:45.6789000), dynamic({
  "moshe": "value"
}), true, 0.01, "asdf", 9223372036854775807, guid(74be27de-1e4e-49d9-b579-fe0b331d3642),
2, decimal(5.00000000000005), datetime(2022-05-06T16:07:03.1234300Z), time(04:56:59.9120000), dynamic({
"moshe": "value2"
}), false, 0.05, "qwerty", 9223372036854775806, guid(f6e97f76-8b73-45c0-b9ef-f68e8f897713),
3, decimal(9.9999999999999), datetime(2023-07-08T18:09:05.5678000Z), time(07:43:12.3456000), dynamic({
"moshe": "value3"
}), true, 0.99, "zxcv", 9223372036854775805, guid(d8e3575c-a7a0-47b3-8c73-9a7a6aaabc12),
]"#;

    let response = client
        .execute_query(args.database.clone(), query, None)
        .await?;

    let results = response
        .into_primary_results()
        .next()
        .ok_or_else(|| "Expected to get a primary result, but got none".to_string())?;

    let rows = results.rows;

    let items = serde_json::from_value::<Vec<Item>>(Value::Array(rows))?;

    println!("items: {:#?}", items);

    Ok(())
}
