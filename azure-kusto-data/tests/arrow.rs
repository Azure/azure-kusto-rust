#![cfg(feature = "mock_transport_framework")]
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;
mod setup;

// copied from datafusion repository.
// https://github.com/apache/arrow-datafusion/blob/41b4e491663029f653e491b110d0b5e74d08a0b6/datafusion/core/src/test_util.rs#L36
macro_rules! assert_batches_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        let formatted = arrow::util::pretty::pretty_format_batches($CHUNKS)
            .unwrap()
            .to_string();

        let actual_lines: Vec<&str> = formatted.trim().lines().collect();

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

#[tokio::test]
async fn arrow_roundtrip() {
    let (client, database) = setup::create_kusto_client("data_arrow_roundtrip")
        .await
        .unwrap();

    let query_path =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/inputs/alltypes.kql");
    let query = std::fs::read_to_string(query_path).unwrap();
    let response = client
        .execute_query(&database, query)
        .into_future()
        .await
        .unwrap();
    let batches = response
        .into_record_batches()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("bool_col", DataType::Boolean, true),
        Field::new("int_col", DataType::Int32, true),
        Field::new("bigint_col", DataType::Int64, true),
        Field::new("float_col", DataType::Float64, true),
        Field::new(
            "timestamp_col",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let expected = vec![
        "+----+----------+---------+------------+-----------+---------------------+",
        "| id | bool_col | int_col | bigint_col | float_col | timestamp_col       |",
        "+----+----------+---------+------------+-----------+---------------------+",
        "| 6  | true     | 0       | 0          | 0         | 2009-04-01 00:00:00 |",
        "| 7  | false    | 1       | 10         | 1.1       | 2009-04-01 00:01:00 |",
        "+----+----------+---------+------------+-----------+---------------------+",
    ];
    assert_batches_eq!(expected, &batches);
    assert_eq!(expected_schema, batches[0].schema())
}
