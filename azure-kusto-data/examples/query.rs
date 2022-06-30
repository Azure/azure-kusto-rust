use azure_kusto_data::models::V2QueryResult;
use azure_kusto_data::prelude::*;
use azure_kusto_data::request_options::RequestOptionsBuilder;
use clap::Parser;
use futures::{pin_mut, TryStreamExt};
use std::error::Error;
use tokio::time::sleep;

/// Simple program to greet a person
#[derive(Parser, Debug)]
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

    let kcsb = ConnectionStringBuilder::new_with_aad_application_key_authentication(
        &args.endpoint,
        &args.tenant_id,
        &args.application_id,
        &args.application_key,
    );

    let client = KustoClient::try_from(kcsb).unwrap();

    println!("Querying {} with regular client", args.query);

    let response = client
        .execute_query_with_options(
            args.database.clone(),
            args.query.clone(),
            Some(
                RequestOptionsBuilder::default()
                    .with_results_progressive_enabled(false) // change to true to enable progressive results
                    .build()
                    .expect("Failed to create request options"),
            ),
        )
        .into_future()
        .await
        .unwrap();

    println!("All results:");

    for table in &response.tables {
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
    let primary_results = response.primary_results().collect::<Vec<_>>();
    println!("primary results: {:#?}", primary_results);

    println!("Querying {} with streaming client", args.query);

    let stream = client
        .execute_query_with_options(
            args.database,
            args.query,
            Some(
                RequestOptionsBuilder::default()
                    .with_results_progressive_enabled(true)
                    .build()
                    .expect("Failed to create request options"),
            ),
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
