use crate::models::ColumnType;
use crate::models::v2::{Column, DataSetCompletion, DataSetHeader, DataTable, Frame, OneApiError, OneApiErrors, Row, TableCompletion, TableFragment, TableFragmentType, TableHeader, TableKind};
use crate::models::v2::ErrorReportingPlacement::EndOfTable;

const V2_VALID_FRAMES: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inputs/v2/validFrames.json"));
const V2_TWO_TABLES: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inputs/v2/twoTables.json"));
const V2_PARTIAL_ERROR: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inputs/v2/partialError.json"));
const V2_PARTIAL_ERROR_FULL_DATASET: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inputs/v2/partialErrorFullDataset.json"));


fn expected_v2_valid_frames() -> Vec<Frame> {
    vec![
        Frame::DataSetHeader(DataSetHeader {
            is_progressive: false,
            version: "v2.0".to_string(),
            is_fragmented: Some(true),
            error_reporting_placement: Some(EndOfTable),
        }),
        Frame::DataTable(DataTable {
            table_id: 0,
            table_name: "@ExtendedProperties".to_string(),
            table_kind: TableKind::QueryProperties,
            columns: vec![
                Column {
                    column_name: "TableId".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "Key".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "Value".to_string(),
                    column_type: ColumnType::Dynamic,
                },
            ],
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(1)),
                    serde_json::Value::String("Visualization".to_string()),
                    serde_json::Value::String("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}".to_string()),
                ]),
            ],
        }),
        Frame::TableHeader(TableHeader {
            table_id: 1,
            table_name: "AllDataTypes".to_string(),
            table_kind: TableKind::PrimaryResult,
            columns: vec![
                Column {
                    column_name: "vnum".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "vdec".to_string(),
                    column_type: ColumnType::Decimal,
                },
                Column {
                    column_name: "vdate".to_string(),
                    column_type: ColumnType::DateTime,
                },
                Column {
                    column_name: "vspan".to_string(),
                    column_type: ColumnType::Timespan,
                },
                Column {
                    column_name: "vobj".to_string(),
                    column_type: ColumnType::Dynamic,
                },
                Column {
                    column_name: "vb".to_string(),
                    column_type: ColumnType::Bool,
                },
                Column {
                    column_name: "vreal".to_string(),
                    column_type: ColumnType::Real,
                },
                Column {
                    column_name: "vstr".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "vlong".to_string(),
                    column_type: ColumnType::Long,
                },
                Column {
                    column_name: "vguid".to_string(),
                    column_type: ColumnType::Guid,
                },
            ],
        }),
        Frame::TableFragment(TableFragment {
            table_fragment_type: TableFragmentType::DataAppend,
            table_id: 1,
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(1)),
                    serde_json::Value::String("2.00000000000001".to_string()),
                    serde_json::Value::String("2020-03-04T14:05:01.3109965Z".to_string()),
                    serde_json::Value::String("01:23:45.6789000".to_string()),
                    serde_json::Value::Object(vec![("moshe".to_string(), serde_json::Value::String("value".to_string()))].into_iter().collect()),
                    serde_json::Value::Bool(true),
                    serde_json::Value::Number(serde_json::Number::from_f64(0.01).unwrap()),
                    serde_json::Value::String("asdf".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(9223372036854775807i64)),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                ]),
            ],
        }),
        Frame::TableCompletion(TableCompletion {
            table_id: 1,
            row_count: 1,
            one_api_errors: None,
        }),
        Frame::DataTable(DataTable {
            table_id: 2,
            table_name: "QueryCompletionInformation".to_string(),
            table_kind: TableKind::QueryCompletionInformation,
            columns: vec![
                Column {
                    column_name: "Timestamp".to_string(),
                    column_type: ColumnType::DateTime,
                },
                Column {
                    column_name: "ClientRequestId".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "ActivityId".to_string(),
                    column_type: ColumnType::Guid,
                },
                Column {
                    column_name: "SubActivityId".to_string(),
                    column_type: ColumnType::Guid,
                },
                Column {
                    column_name: "ParentActivityId".to_string(),
                    column_type: ColumnType::Guid,
                },
                Column {
                    column_name: "Level".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "LevelName".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "StatusCode".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "StatusCodeName".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "EventType".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "EventTypeName".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "Payload".to_string(),
                    column_type: ColumnType::String,
                },
            ],
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::String("2023-11-26T13:34:17.0731478Z".to_string()),
                    serde_json::Value::String("blab6".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(4)),
                    serde_json::Value::String("Info".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                    serde_json::Value::String("S_OK (0)".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(4)),
                    serde_json::Value::String("QueryInfo".to_string()),
                    serde_json::Value::String("{\"Count\":1,\"Text\":\"Query completed successfully\"}".to_string()),
                ]),
                Row::Values(vec![
                    serde_json::Value::String("2023-11-26T13:34:17.0731478Z".to_string()),
                    serde_json::Value::String("blab6".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(4)),
                    serde_json::Value::String("Info".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                    serde_json::Value::String("S_OK (0)".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(5)),
                    serde_json::Value::String("WorkloadGroup".to_string()),
                    serde_json::Value::String("{\"Count\":1,\"Text\":\"default\"}".to_string()),
                ]),
            ],
        }),
        Frame::DataSetCompletion(DataSetCompletion {
            has_errors: false,
            cancelled: false,
            one_api_errors: None,
        }),
    ]
}


fn expected_v2_two_tables() -> Vec<Frame> {
    vec![
        Frame::DataSetHeader(DataSetHeader {
            is_progressive: false,
            version: "v2.0".to_string(),
            is_fragmented: Some(true),
            error_reporting_placement: Some(EndOfTable),
        }),
        Frame::DataTable(DataTable {
            table_id: 0,
            table_name: "@ExtendedProperties".to_string(),
            table_kind: TableKind::QueryProperties,
            columns: vec![
                Column {
                    column_name: "TableId".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "Key".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "Value".to_string(),
                    column_type: ColumnType::Dynamic,
                },
            ],
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(1)),
                    serde_json::Value::String("Visualization".to_string()),
                    serde_json::Value::String("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}".to_string()),
                ]),
                Row::Values(vec![2.into(), "Visualization".to_string().into(), "{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}".to_string().into()]),
            ],
        }),
        Frame::TableHeader(TableHeader {
            table_id: 1,
            table_name: "PrimaryResult".to_string(),
            table_kind: TableKind::PrimaryResult,
            columns: vec![
                Column {
                    column_name: "A".to_string(),
                    column_type: ColumnType::Int,
                },
            ],
        }),
        Frame::TableFragment(TableFragment {
            table_fragment_type: TableFragmentType::DataAppend,
            table_id: 1,
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(1)),
                ]),
            ],
        }),
        Frame::TableFragment(TableFragment {
            table_fragment_type: TableFragmentType::DataAppend,
            table_id: 1,
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(2)),
                ]),
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(3)),
                ]),
            ],
        }),
        Frame::TableCompletion(TableCompletion {
            table_id: 1,
            row_count: 3,
            one_api_errors: None,
        }),
        Frame::TableHeader(TableHeader {
            table_id: 2,
            table_name: "PrimaryResult".to_string(),
            table_kind: TableKind::PrimaryResult,
            columns: vec![
                Column {
                    column_name: "A".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "B".to_string(),
                    column_type: ColumnType::Int,
                },
            ],
        }),
        Frame::TableFragment(TableFragment {
            table_fragment_type: TableFragmentType::DataAppend,
            table_id: 2,
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::String("a".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(1)),
                ]),
            ],
        }),
        Frame::TableFragment(TableFragment {
            table_fragment_type: TableFragmentType::DataAppend,
            table_id: 2,
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::String("b".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(2)),
                ]),
                Row::Values(vec![
                    serde_json::Value::String("c".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(3)),
                ]),
            ],
        }),
        Frame::TableCompletion(TableCompletion {
            table_id: 2,
            row_count: 3,
            one_api_errors: None,
        }),
        Frame::DataTable(DataTable {
            table_id: 3,
            table_name: "QueryCompletionInformation".to_string(),
            table_kind: TableKind::QueryCompletionInformation,
            columns: vec![
                Column {
                    column_name: "Timestamp".to_string(),
                    column_type: ColumnType::DateTime,
                },
                Column {
                    column_name: "ClientRequestId".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "ActivityId".to_string(),
                    column_type: ColumnType::Guid,
                },
                Column {
                    column_name: "SubActivityId".to_string(),
                    column_type: ColumnType::Guid,
                },
                Column {
                    column_name: "ParentActivityId".to_string(),
                    column_type: ColumnType::Guid,
                },
                Column {
                    column_name: "Level".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "LevelName".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "StatusCode".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "StatusCodeName".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "EventType".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "EventTypeName".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "Payload".to_string(),
                    column_type: ColumnType::String,
                },
            ],
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::String("2023-11-28T11:13:43.2514779Z".to_string()),
                    serde_json::Value::String("blab6".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(4)),
                    serde_json::Value::String("Info".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                    serde_json::Value::String("S_OK (0)".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(4)),
                    serde_json::Value::String("QueryInfo".to_string()),
                    serde_json::Value::String("{\"Count\":1,\"Text\":\"Query completed successfully\"}".to_string()),
                ]),
                Row::Values(vec![
                    serde_json::Value::String("2023-11-28T11:13:43.2514779Z".to_string()),
                    serde_json::Value::String("blab6".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(4)),
                    serde_json::Value::String("Info".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                    serde_json::Value::String("S_OK (0)".to_string()),
                    serde_json::Value::Number(serde_json::Number::from(5)),
                    serde_json::Value::String("WorkloadGroup".to_string()),
                    serde_json::Value::String("{\"Count\":1,\"Text\":\"default\"}".to_string()),
                ]),
                Row::Values(vec![serde_json::Value::String("2023-11-28T11:13:43.2514779Z".to_string()), serde_json::Value::String("blab6".to_string()), serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()), serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()), serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()), serde_json::Value::from(4), serde_json::Value::String("Info".to_string()), serde_json::Value::from(0), serde_json::Value::String("S_OK (0)".to_string()), serde_json::Value::from(6), serde_json::Value::String("EffectiveRequestOptions".to_string()), serde_json::Value::String("{\"Count\":1,\"Text\":\"{\\\"DataScope\\\":\\\"All\\\",\\\"QueryConsistency\\\":\\\"strongconsistency\\\",\\\"MaxMemoryConsumptionPerIterator\\\":5368709120,\\\"MaxMemoryConsumptionPerQueryPerNode\\\":8589346816,\\\"QueryFanoutNodesPercent\\\":100,\\\"QueryFanoutThreadsPercent\\\":100}\"}".to_string())]),
                Row::Values(vec![serde_json::Value::String("2023-11-28T11:13:43.2514779Z".to_string()), serde_json::Value::String("blab6".to_string()), serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()), serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()), serde_json::Value::String("123e27de-1e4e-49d9-b579-fe0b331d3642".to_string()), serde_json::Value::from(6), serde_json::Value::String("Stats".to_string()), serde_json::Value::from(0), serde_json::Value::String("S_OK (0)".to_string()), serde_json::Value::from(0), serde_json::Value::String("QueryResourceConsumption".to_string()), serde_json::Value::String("{\"ExecutionTime\":0.0,\"resource_usage\":{\"cache\":{\"memory\":{\"hits\":0,\"misses\":0,\"total\":0},\"disk\":{\"hits\":0,\"misses\":0,\"total\":0},\"shards\":{\"hot\":{\"hitbytes\":0,\"missbytes\":0,\"retrievebytes\":0},\"cold\":{\"hitbytes\":0,\"missbytes\":0,\"retrievebytes\":0},\"bypassbytes\":0}},\"cpu\":{\"user\":\"00:00:00\",\"kernel\":\"00:00:00\",\"total cpu\":\"00:00:00\"},\"memory\":{\"peak_per_node\":524384},\"network\":{\"inter_cluster_total_bytes\":1099,\"cross_cluster_total_bytes\":0}},\"input_dataset_statistics\":{\"extents\":{\"total\":0,\"scanned\":0,\"scanned_min_datetime\":\"0001-01-01T00:00:00.0000000Z\",\"scanned_max_datetime\":\"0001-01-01T00:00:00.0000000Z\"},\"rows\":{\"total\":0,\"scanned\":0},\"rowstores\":{\"scanned_rows\":0,\"scanned_values_size\":0},\"shards\":{\"queries_generic\":0,\"queries_specialized\":0}},\"dataset_statistics\":[{\"table_row_count\":3,\"table_size\":15},{\"table_row_count\":3,\"table_size\":43}],\"cross_cluster_resource_usage\":{}}".to_string())]),
            ],
        }),
        Frame::DataSetCompletion(DataSetCompletion {
            has_errors: false,
            cancelled: false,
            one_api_errors: None,
        }),
    ]
}

fn expected_v2_partial_error() -> Vec<Frame> {
    vec![
        Frame::DataSetHeader(DataSetHeader {
            is_progressive: false,
            version: "v2.0".to_string(),
            is_fragmented: Some(true),
            error_reporting_placement: Some(EndOfTable),
        }),
        Frame::DataTable(DataTable {
            table_id: 0,
            table_name: "@ExtendedProperties".to_string(),
            table_kind: TableKind::QueryProperties,
            columns: vec![
                Column {
                    column_name: "TableId".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "Key".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "Value".to_string(),
                    column_type: ColumnType::Dynamic,
                },
            ],
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(1)),
                    serde_json::Value::String("Visualization".to_string()),
                    serde_json::Value::String("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}".to_string()),
                ]),
            ],
        }),
        Frame::TableHeader(TableHeader {
            table_id: 1,
            table_name: "PrimaryResult".to_string(),
            table_kind: TableKind::PrimaryResult,
            columns: vec![
                Column {
                    column_name: "A".to_string(),
                    column_type: ColumnType::Int,
                },
            ],
        }),
        Frame::TableFragment(TableFragment {
            table_fragment_type: TableFragmentType::DataAppend,
            table_id: 1,
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(1)),
                ]),
            ],
        }),
        Frame::TableCompletion(TableCompletion {
            table_id: 1,
            row_count: 1,
            one_api_errors: Some(vec![
                OneApiError {
                    error_message: crate::models::v2::ErrorMessage {
                        code: "LimitsExceeded".to_string(),
                        message: "Request is invalid and cannot be executed.".to_string(),
                        r#type: "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException".to_string(),
                        description: "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..".to_string(),
                        context: crate::models::v2::ErrorContext {
                            timestamp: "2023-11-28T08:30:06.4085369Z".to_string(),
                            service_alias: "<censored>".to_string(),
                            machine_name: "KSEngine000000".to_string(),
                            process_name: "Kusto.WinSvc.Svc".to_string(),
                            process_id: 4900,
                            thread_id: 6828,
                            client_request_id: "blab6".to_string(),
                            activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            sub_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            activity_type: "GW.Http.CallContext".to_string(),
                            parent_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            activity_stack: "(Activity stack: CRID=blab6 ARID=123e27de-1e4e-49d9-b579-fe0b331d3642 > GW.Http.CallContext/123e27de-1e4e-49d9-b579-fe0b331d3642)".to_string(),
                        },
                        is_permanent: false,
                    },
                },
            ]),
        }),
        Frame::DataSetCompletion(DataSetCompletion {
            has_errors: true,
            cancelled: false,
            one_api_errors: Some(vec![
                OneApiError {
                    error_message: crate::models::v2::ErrorMessage {
                        code: "LimitsExceeded".to_string(),
                        message: "Request is invalid and cannot be executed.".to_string(),
                        r#type: "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException".to_string(),
                        description: "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..".to_string(),
                        r#context: crate::models::v2::ErrorContext {
                            timestamp: "2023-11-28T08:30:06.4085369Z".to_string(),
                            service_alias: "<censored>".to_string(),
                            machine_name: "KSEngine000000".to_string(),
                            process_name: "Kusto.WinSvc.Svc".to_string(),
                            process_id: 4900,
                            thread_id: 6828,
                            client_request_id: "blab6".to_string(),
                            activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            sub_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            activity_type: "GW.Http.CallContext".to_string(),
                            parent_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            activity_stack: "(Activity stack: CRID=blab6 ARID=123e27de-1e4e-49d9-b579-fe0b331d3642 > GW.Http.CallContext/123e27de-1e4e-49d9-b579-fe0b331d3642)".to_string(),
                        },
                        is_permanent: false,
                    },
                },
            ]),
        }),
    ]
}

fn expected_v2_partial_error_full_dataset() -> Vec<Frame> {
    vec![
        Frame::DataSetHeader(DataSetHeader {
            is_progressive: false,
            version: "v2.0".to_string(),
            is_fragmented: Some(false),
            error_reporting_placement: Some(crate::models::v2::ErrorReportingPlacement::InData),
        }),
        Frame::DataTable(DataTable {
            table_id: 0,
            table_name: "@ExtendedProperties".to_string(),
            table_kind: TableKind::QueryProperties,
            columns: vec![
                Column {
                    column_name: "TableId".to_string(),
                    column_type: ColumnType::Int,
                },
                Column {
                    column_name: "Key".to_string(),
                    column_type: ColumnType::String,
                },
                Column {
                    column_name: "Value".to_string(),
                    column_type: ColumnType::Dynamic,
                },
            ],
            rows: vec![
                Row::Values(vec![
                    serde_json::Value::Number(serde_json::Number::from(1)),
                    serde_json::Value::String("Visualization".to_string()),
                    serde_json::Value::String("{\"Visualization\":null,\"Title\":null,\"XColumn\":null,\"Series\":null,\"YColumns\":null,\"AnomalyColumns\":null,\"XTitle\":null,\"YTitle\":null,\"XAxis\":null,\"YAxis\":null,\"Legend\":null,\"YSplit\":null,\"Accumulate\":false,\"IsQuerySorted\":false,\"Kind\":null,\"Ymin\":\"NaN\",\"Ymax\":\"NaN\",\"Xmin\":null,\"Xmax\":null}".to_string()),
                ]),
                Row::Error(OneApiErrors {
                    errors: vec![OneApiError {
                        error_message: crate::models::v2::ErrorMessage {
                            code: "LimitsExceeded".to_string(),
                            message: "Request is invalid and cannot be executed.".to_string(),
                            r#type: "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException".to_string(),
                            description: "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..".to_string(),
                            context: crate::models::v2::ErrorContext {
                                timestamp: "2023-12-18T08:25:05.8871389Z".to_string(),
                                service_alias: "ASAF".to_string(),
                                machine_name: "KSEngine000000".to_string(),
                                process_name: "Kusto.WinSvc.Svc".to_string(),
                                process_id: 4900,
                                thread_id: 4852,
                                client_request_id: "blab6".to_string(),
                                activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                                sub_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                                activity_type: "GW.Http.CallContext".to_string(),
                                parent_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                                activity_stack: "(Activity stack: CRID=blab6 ARID=d6a331d8-4b0e-498b-b72f-9f842b86e0b2 > GW.Http.CallContext/d6a331d8-4b0e-498b-b72f-9f842b86e0b2)".to_string(),
                            },
                            is_permanent: false,
                        },
                    }]
                }),
            ],
        }),
        Frame::DataSetCompletion(DataSetCompletion {
            has_errors: true,
            cancelled: false,
            one_api_errors: Some(vec![
                OneApiError {
                    error_message: crate::models::v2::ErrorMessage {
                        code: "LimitsExceeded".to_string(),
                        message: "Request is invalid and cannot be executed.".to_string(),
                        r#type: "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException".to_string(),
                        description: "Query execution has exceeded the allowed limits (80DA0003): The results of this query exceed the set limit of 1 records, so not all records were returned (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003). See https://aka.ms/kustoquerylimits for more information and possible solutions..".to_string(),
                        context: crate::models::v2::ErrorContext {
                            timestamp: "2023-12-18T08:25:05.8871389Z".to_string(),
                            service_alias: "ASAF".to_string(),
                            machine_name: "KSEngine000000".to_string(),
                            process_name: "Kusto.WinSvc.Svc".to_string(),
                            process_id: 4900,
                            thread_id: 4852,
                            client_request_id: "blab6".to_string(),
                            activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            sub_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            activity_type: "GW.Http.CallContext".to_string(),
                            parent_activity_id: "123e27de-1e4e-49d9-b579-fe0b331d3642".to_string(),
                            activity_stack: "(Activity stack: CRID=blab6 ARID=d6a331d8-4b0e-498b-b72f-9f842b86e0b2 > GW.Http.CallContext/d6a331d8-4b0e-498b-b72f-9f842b86e0b2)".to_string(),
                        },
                        is_permanent: false,
                    },
                },
            ]),
        }),
    ]
}


pub fn v2_files_full() -> Vec<(&'static str, Vec<Frame>)> {
    vec![
        (V2_VALID_FRAMES, expected_v2_valid_frames()),
        (V2_TWO_TABLES, expected_v2_two_tables()),
        (V2_PARTIAL_ERROR, expected_v2_partial_error()),
        (V2_PARTIAL_ERROR_FULL_DATASET, expected_v2_partial_error_full_dataset()),
    ]
}


pub fn v2_files_iterative() -> Vec<(&'static str, Vec<Frame>)> {
    vec![
        (V2_VALID_FRAMES, expected_v2_valid_frames()),
        (V2_TWO_TABLES, expected_v2_two_tables()),
        (V2_PARTIAL_ERROR, expected_v2_partial_error()),
    ]
}
