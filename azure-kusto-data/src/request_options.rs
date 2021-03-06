use crate::types::{KustoDateTime, KustoDuration};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DataScope {
    Default,
    All,
    Hotcache,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    Csl,
    Kql,
    Sql,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum QueryConsistency {
    Strongconsistency,
    Weakconsistency,
    Affinitizedweakconsistency,
    Databaseaffinitizedweakconsistency,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, derive_builder::Builder)]
#[builder(setter(into, strip_option, prefix = "with"), default)]
pub struct RequestOptions {
    /// If set and positive, indicates the maximum number of HTTP redirects that the client will process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_max_redirect_count: Option<i64>,
    /// If true, disables reporting partial query failures as part of the result set
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferpartialqueryfailures: Option<bool>,
    /// A hint to use shuffle strategy for materialized views that are referenced in the query.
    /// The property is an array of materialized views names and the shuffle keys to use.
    /// Examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized_view_shuffle: Option<serde_json::Value>,
    /// Overrides the default maximum amount of memory a whole query may allocate per node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory_consumption_per_query_per_node: Option<u64>,
    /// Overrides the default maximum amount of memory a query operator may allocate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxmemoryconsumptionperiterator: Option<u64>,
    /// Overrides the default maximum number of columns a query is allowed to produce.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxoutputcolumns: Option<u64>,
    /// Enables setting the request timeout to its maximum value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub norequesttimeout: Option<bool>,
    /// Enables suppressing truncation of the query results returned to the caller.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notruncation: Option<bool>,
    /// If true, push simple selection through aggregation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub push_selection_through_aggregation: Option<bool>,
    /// When evaluating the bin_auto() function, the start value to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_bin_auto_at: Option<String>,
    /// When evaluating the bin_auto() function, the bin size value to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_bin_auto_size: Option<String>,
    /// The default parameter value of the cursor_after() function when called without parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_cursor_after_default: Option<String>,
    /// The default parameter value of the cursor_before_or_at() function when called without parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_cursor_before_or_at_default: Option<String>,
    /// Overrides the cursor value returned by the cursor_current() or current_cursor() functions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_cursor_current: Option<String>,
    /// Disables usage of cursor functions in the context of the query.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_cursor_disabled: Option<bool>,
    /// List of table names that should be scoped to cursor_after_default .. cursor_before_or_at_default (upper bound is optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_cursor_scoped_tables: Option<Vec<String>>,
    /// Controls the query's datascope -- whether the query applies to all data or just part of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    query_datascope: Option<DataScope>,
    /// Controls the column name for the query's datetime scope (query_datetimescope_to / query_datetimescope_from).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_datetimescope_column: Option<String>,
    /// Controls the query's datetime scope (earliest) -- used as auto-applied filter on query_datetimescope_column only (if defined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_datetimescope_from: Option<KustoDateTime>,
    /// Controls the query's datetime scope (latest) -- used as auto-applied filter on query_datetimescope_column only (if defined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_datetimescope_to: Option<KustoDateTime>,
    /// If set, controls the way the subquery merge behaves: the executing node will introduce an additional
    /// level in the query hierarchy for each subgroup of nodes; the size of the subgroup is set by this option.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_distribution_nodes_span: Option<i32>,
    /// The percentage of nodes to fan out execution to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_fanout_nodes_percent: Option<i32>,
    /// The percentage of threads to fan out execution to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_fanout_threads_percent: Option<i32>,
    /// If specified, forces Row Level Security rules, even if row_level_security policy is disabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_force_row_level_security: Option<bool>,
    /// Controls how the query text is to be interpreted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_language: Option<QueryLanguage>,
    ///  Enables logging of the query parameters, so that they can be viewed later in the .show queries journal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_log_query_parameters: Option<bool>,
    /// Overrides the default maximum number of entities in a union.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_max_entities_in_union: Option<i64>,
    /// Overrides the datetime value returned by the now(0s) function.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_now: Option<KustoDateTime>,
    ///  If set, generate python debug query for the enumerated python node (default first).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_python_debug: Option<i32>,
    /// If set, retrieves the schema of each tabular data in the results of the query instead of the data itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_results_apply_getschema: Option<bool>,
    /// If positive, controls the maximum age of the cached query results the service is allowed to return
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_results_cache_max_age: Option<KustoDuration>,
    /// If set, enables per-shard query cache.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_results_cache_per_shard: Option<bool>,
    /// Hint for Kusto as to how many records to send in each update (takes effect only if OptionResultsProgressiveEnabled is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_results_progressive_row_count: Option<i64>,
    /// Hint for Kusto as to how often to send progress frames (takes effect only if OptionResultsProgressiveEnabled is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_results_progressive_update_period: Option<i32>,
    ///  Enables limiting query results to this number of records.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_take_max_records: Option<i64>,
    /// Controls query consistency
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queryconsistency: Option<QueryConsistency>,
    /// Request application name to be used in the reporting (e.g. show queries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_app_name: Option<String>,
    /// If specified, blocks access to tables for which row_level_security policy is enabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_block_row_level_security: Option<bool>,
    /// If specified, indicates that the request can't call-out to a user-provided service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_callout_disabled: Option<bool>,
    /// Arbitrary text that the author of the request wants to include as the request description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_description: Option<String>,
    /// If specified, indicates that the request can't invoke code in the ExternalTable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_external_table_disabled: Option<bool>,
    /// If specified, indicates that the service should not impersonate the caller's identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_impersonation_disabled: Option<bool>,
    /// If specified, indicates that the request can't write anything.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_readonly: Option<bool>,
    ///  If specified, indicates that the request can't access remote databases and clusters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_remote_entities_disabled: Option<bool>,
    /// If specified, indicates that the request can't invoke code in the sandbox.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_sandboxed_execution_disabled: Option<bool>,
    /// Request user to be used in the reporting (e.g. show queries).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_user: Option<String>,
    /// If set, enables the progressive query stream
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results_progressive_enabled: Option<bool>,
    /// Overrides the default request timeout.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub servertimeout: Option<KustoDuration>,
    /// Overrides the default maximum number of records a query is allowed to return to the caller (truncation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncationmaxrecords: Option<i64>,
    /// Overrides the default maximum data size a query is allowed to return to the caller (truncation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncationmaxsize: Option<i64>,
    /// Validates user's permissions to perform the query and doesn't run the query itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_permissions: Option<bool>,
}
