use azure_kusto_data::models::V2ProgressiveResult;
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

    /*    let response = client
        .execute_query(args.database.clone(), args.query.clone())
        .into_future()
        .await
        .unwrap();

    for table in &response.tables {
        match table {
            ResultTable::DataSetHeader(header) => println!("header: {:#?}", header),
            ResultTable::DataTable(table) => println!("table: {:#?}", table),
            ResultTable::DataSetCompletion(completion) => println!("completion: {:#?}", completion),
        }
    }

    let primary_results = response.into_primary_results().collect::<Vec<_>>();
    println!("primary results: {:#?}", primary_results);*/

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
        .into_progressive_stream()
        .await?;

    pin_mut!(stream);

    while let Some(table) = stream.try_next().await? {
        match table {
            V2ProgressiveResult::DataSetHeader(header) => println!("header: {:#?}", header),
            V2ProgressiveResult::DataTable(table) => println!("table: {:#?}", table),
            V2ProgressiveResult::DataSetCompletion(completion) => {
                println!("completion: {:#?}", completion)
            }
        }
        sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}
