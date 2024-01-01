use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct OneApiError {
    #[serde(rename = "error")]
    error_message: ErrorMessage,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ErrorMessage {
    code: String,
    message: String,
    #[serde(rename = "@type")]
    r#type: String,
    #[serde(rename = "@context")]
    context: ErrorContext,
    #[serde(rename = "@permanent")]
    is_permanent: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ErrorContext {
    timestamp: String,
    service_alias: String,
    machine_name: String,
    process_name: String,
    process_id: i32,
    thread_id: i32,
    client_request_id: String,
    activity_id: String,
    sub_activity_id: String,
    activity_type: String,
    parent_activity_id: String,
    activity_stack: String,
}
