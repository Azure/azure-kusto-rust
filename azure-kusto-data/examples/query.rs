use azure_kusto_data::prelude::*;
use clap::Parser;
use futures::{pin_mut, TryStreamExt};
use std::error::Error;

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

    #[clap(env, long)]
    application_id: String,

    #[clap(env, long)]
    application_key: String,

    #[clap(env, long)]
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

    let response = client
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
    println!("primary results: {:#?}", primary_results);

    let mut stream = client
        .execute_query(args.database, args.query)
        .into_stream()
        .await?;

    pin_mut!(stream);

    while let Some(table) = stream.try_next().await? {
        match table {
            ResultTable::DataSetHeader(header) => println!("header: {:#?}", header),
            ResultTable::DataTable(table) => println!("table: {:#?}", table),
            ResultTable::DataSetCompletion(completion) => println!("completion: {:#?}", completion),
        }
    }

    Ok(())
}
