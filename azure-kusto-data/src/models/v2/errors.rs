use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct OneApiErrors {
    #[serde(rename = "OneApiErrors")]
    pub errors: Vec<OneApiError>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct OneApiError {
    #[serde(rename = "error")]
    pub(crate) error_message: ErrorMessage,
}

impl Display for OneApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string_pretty(&self.error_message).unwrap()
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ErrorMessage {
    pub code: String,
    pub message: String,
    #[serde(rename = "@message")]
    pub description: String,
    #[serde(rename = "@type")]
    pub r#type: String,
    #[serde(rename = "@context")]
    pub context: ErrorContext,
    #[serde(rename = "@permanent")]
    pub is_permanent: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ErrorContext {
    pub timestamp: String,
    pub service_alias: String,
    pub machine_name: String,
    pub process_name: String,
    pub process_id: i32,
    pub thread_id: i32,
    pub client_request_id: String,
    pub activity_id: String,
    pub sub_activity_id: String,
    pub activity_type: String,
    pub parent_activity_id: String,
    pub activity_stack: String,
}
