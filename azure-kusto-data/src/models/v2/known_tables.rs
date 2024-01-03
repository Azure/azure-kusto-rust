use serde::{Deserialize, Serialize};
use crate::{KustoInt, KustoString, KustoDateTime};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryProperties {
    table_id: KustoInt,
    key: KustoString,
    value: KustoDynamic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueryCompletionInformation {
    timestamp: KustoDateTime,
    client_request_id: KustoString,
    activity_id: KustoGuid,
    sub_activity_id: KustoGuid,
    parent_activity_id: KustoGuid,
    level: KustoInt,
    level_name: KustoString,
    status_code: KustoInt,
    status_code_name: KustoString,
    event_type: KustoInt,
    event_type_name: KustoString,
    payload: KustoString,
}
