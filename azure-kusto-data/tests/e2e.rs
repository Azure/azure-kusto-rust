use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;
use time::Duration;

use azure_kusto_data::types::{KustoDateTime, KustoDuration};
use decimal::d128;
use uuid::Uuid;

mod setup;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Item {
    vnum: i32,
    vdec: d128,
    vdate: KustoDateTime,
    vspan: KustoDuration,
    vobj: Value,
    vb: bool,
    vreal: f64,
    vstr: String,
    vlong: i64,
    vguid: Uuid,
}

#[tokio::test]
async fn create_query_delete_table() {
    let (client, database) = setup::create_kusto_client();

    let query = r#".set-or-replace KustoRsTest <| datatable(vnum:int, vdec:decimal, vdate:datetime, vspan:timespan, vobj:dynamic, vb:bool, vreal:real, vstr:string, vlong:long, vguid:guid)
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
]
"#;
    let response = client
        .execute_command(database.clone(), query, None)
        .await
        .expect("Failed to run query");

    assert!(response.table_count() > 0);

    let query = ".show tables | where TableName == \"KustoRsTest\"";
    let response = client
        .execute_command(database.clone(), query, None)
        .await
        .expect("Failed to run query");

    assert_eq!(response.table_count(), 4);

    let query = "KustoRsTest";
    let response = client
        .execute_query(database.clone(), query, None)
        .await
        .expect("Failed to run query");

    let results = response.into_primary_results().next().expect("No results");

    let rows = results.rows;

    let expected = vec![
        Item {
            vnum: 1,
            vdec: d128!(2.00000000000001),
            vdate: KustoDateTime::from_str("2020-03-04T14:05:01.3109965Z").unwrap(),
            vspan: KustoDuration::from(
                Duration::seconds(3600 + 23 * 60 + 45) + Duration::microseconds(678900),
            ),
            vobj: Value::Object(serde_json::Map::from_iter(vec![(
                "moshe".to_string(),
                Value::String("value".to_string()),
            )])),
            vb: true,
            vreal: 0.01,
            vstr: "asdf".to_string(),
            vlong: 9223372036854775807,
            vguid: Uuid::parse_str("74be27de-1e4e-49d9-b579-fe0b331d3642").unwrap(),
        },
        Item {
            vnum: 2,
            vdec: d128!(5.00000000000005),
            vdate: KustoDateTime::from_str("2022-05-06T16:07:03.1234300Z").unwrap(),
            vspan: KustoDuration::from(
                Duration::seconds(4 * 3600 + 56 * 60 + 59) + Duration::microseconds(912000),
            ),
            vobj: Value::Object(serde_json::Map::from_iter(vec![(
                "moshe".to_string(),
                Value::String("value2".to_string()),
            )])),
            vb: false,
            vreal: 0.05,
            vstr: "qwerty".to_string(),
            vlong: 9223372036854775806,
            vguid: Uuid::parse_str("f6e97f76-8b73-45c0-b9ef-f68e8f897713").unwrap(),
        },
        Item {
            vnum: 3,
            vdec: d128!(9.9999999999999),
            vdate: KustoDateTime::from_str("2023-07-08T18:09:05.5678000Z").unwrap(),
            vspan: KustoDuration::from(
                Duration::seconds(7 * 3600 + 43 * 60 + 12) + Duration::microseconds(345600),
            ),
            vobj: Value::Object(serde_json::Map::from_iter(vec![(
                "moshe".to_string(),
                Value::String("value3".to_string()),
            )])),
            vb: true,
            vreal: 0.99,
            vstr: "zxcv".to_string(),
            vlong: 9223372036854775805,
            vguid: Uuid::parse_str("d8e3575c-a7a0-47b3-8c73-9a7a6aaabc12").unwrap(),
        },
    ];

    let items =
        serde_json::from_value::<Vec<Item>>(Value::Array(rows)).expect("Failed to deserialize");

    assert_eq!(items, expected);

    let query = ".drop table KustoRsTest | where TableName == \"KustoRsTest\"";
    let response = client
        .execute_command(database.clone(), query, None)
        .await
        .expect("Failed to run query");

    assert_eq!(response.tables[0].rows.len(), 0);
}
