use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn parse_app_key() -> () {
    use azure_kusto_data::error::Error;
    use azure_kusto_data::prelude::*;
    ConnectionString::from_raw_connection_string("Data Source=localhost ; Application Client Id=f6f295b1-0ce0-41f1-bba3-735accac0c69; Appkey =1234;Authority Id= 25184ef2-1dc0-4b05-84ae-f505bf7964f4 ; aad federated security = True").unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("parse app key", |b| b.iter(|| parse_app_key()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
