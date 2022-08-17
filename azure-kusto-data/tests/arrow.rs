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
    let (client, database) = setup::create_kusto_client();

    let query = "
        datatable(
            id:int,
            string_col:string,
            bool_col:bool,
            int_col:int,
            bigint_col:long,
            float_col:real,
            timestamp_col:datetime,
            duration_col:timespan
        ) [
            6, 'Hello', true, 0, 0, 0, datetime(2009-04-01 00:00:00), timespan(1.00:00:00.0000001),
            7, 'World', false, 1, 10, 1.1, datetime(2009-04-01 00:01:00), timespan(-00:01:00.0001001),
        ]
    ";
    let response = client
        .execute_query(database, query)
        .into_future()
        .await
        .expect("Failed to run query");
    let batches = response
        .record_batches()
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to collect batches");

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("string_col", DataType::Utf8, true),
        Field::new("bool_col", DataType::Boolean, true),
        Field::new("int_col", DataType::Int32, true),
        Field::new("bigint_col", DataType::Int64, true),
        Field::new("float_col", DataType::Float64, true),
        Field::new(
            "timestamp_col",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            "duration_col",
            DataType::Duration(TimeUnit::Nanosecond),
            true,
        ),
    ]));
    let expected = vec![
        "+----+------------+----------+---------+------------+-----------+---------------------+",
        "| id | string_col | bool_col | int_col | bigint_col | float_col | timestamp_col       |",
        "+----+------------+----------+---------+------------+-----------+---------------------+",
        "| 6  | Hello      | true     | 0       | 0          | 0         | 2009-04-01 00:00:00 |",
        "| 7  | World      | false    | 1       | 10         | 1.1       | 2009-04-01 00:01:00 |",
        "+----+------------+----------+---------+------------+-----------+---------------------+",
    ];
    assert_batches_eq!(
        expected,
        // we have to de-select the duration column, since pretty printing is not supported in arrow
        &[batches[0]
            .project(&[0, 1, 2, 3, 4, 5, 6])
            .expect("Failed to project numbers")]
    );
    assert_eq!(expected_schema, batches[0].schema());
}
