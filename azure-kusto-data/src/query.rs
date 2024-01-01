use crate::prelude::ClientRequestProperties;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub(crate) struct QueryBody {
    /// Name of the database in scope that is the target of the query or control command
    pub db: String,
    /// Text of the query or control command to execute
    pub csl: String,
    /// Additional parameters and options for fine-grained control of the request behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<ClientRequestProperties>,
}
