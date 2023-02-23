//! Request options for the Azure Data Explorer Client.

use std::borrow::Cow;
use crate::types::{KustoDateTime, KustoDuration};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use serde_with::skip_serializing_none;

/// Controls the hot or cold cache for the scope of the query.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DataScope {
    /// Default cache behavior.
    Default,
    /// Mark as All.
    All,
    /// Mark as Hot Cache
    #[serde(rename = "hotcache")]
    HotCache,
}

/// Controls the language of the query.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    /// Old name for KQL.
    Csl,
    /// Kusto Query Language - the recommended language for querying.
    Kql,
    /// Structured Query Language - can be used, but is not recommended.
    Sql,
}

/// The consistency level for the query.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum QueryConsistency {
    /// Strong Consistency - the results of this query can be observed in following queries immediately.
    #[serde(rename = "strongconsistency")]
    StrongConsistency,
    /// Weak consistency - can execute on any node on the cluster, which improves performance but with weaker guarantees.
    #[serde(rename = "weakconsistency")]
    WeakConsistency,
    /// Same as weak consistency, but affinized by the query text.
    #[serde(rename = "affinitizedweakconsistency")]
    AffinitizedWeakConsistency,
    /// Same as weak consistency, but affinized by the database.
    #[serde(rename = "databaseaffinitizedweakconsistency")]
    DatabaseAffinitizedWeakConsistency,
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, Debug, Clone, Default, derive_builder::Builder)]
#[builder(setter(into, strip_option, prefix = "with"), default)]
/// Properties for a query.
pub struct ClientRequestProperties {
    /// Options to control the query.
    pub options: Option<Options>,
    /// Parameters to pass to the query.
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    #[serde(skip)]
    /// Client request id.
    pub client_request_id: Option<String>,
    #[serde(skip)]
    /// Application name for tracing.
    pub application: Option<String>,
    #[serde(skip)]
    /// User name for tracing.
    pub user: Option<String>
}

impl ClientRequestProperties {
    /// Add a query parameter with a string value.
    pub fn add_string_parameter(&mut self, name: Cow<str>, value: Cow<str>) {
        self.add_parameter(name, serde_json::Value::String(value.into()));
    }

    /// Add a query parameter with an integer value.
    pub fn add_i64_parameter(&mut self, name: Cow<str>, value: i64) {
        self.add_parameter(name, serde_json::Value::Number(value.into()));
    }

    /// Add a query parameter with a float value.
    pub fn add_f64_parameter(&mut self, name: Cow<str>, value: f64) {
        self.add_parameter(name, Number::from_f64(value).map(serde_json::Value::Number).unwrap_or_else(||serde_json::Value::String(value.to_string())));
    }

    /// Add a query parameter with a boolean value.
    pub fn add_bool_parameter(&mut self, name: Cow<str>, value: bool) {
        self.add_parameter(name, serde_json::Value::Bool(value));
    }

    /// Add a query parameter with a generic value.
    pub fn add_parameter(&mut self, name: Cow<str>, value: serde_json::Value) {
        if self.parameters.is_none() {
            self.parameters = Some(HashMap::new());
        }
        self.parameters.as_mut().unwrap().insert(name.into(), value);
    }
}

impl From<Options> for ClientRequestProperties {
    fn from(options: Options) -> Self {
        Self {
            options: Some(options),
            ..Default::default()
        }
    }
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize, Debug, Clone, Default, derive_builder::Builder)]
#[builder(setter(into, strip_option, prefix = "with"), default)]
/// Request options for queries, can be used to set the size, consistency, and other options.
pub struct Options {
    /// If set and positive, indicates the maximum number of HTTP redirects that the client will process.
    pub client_max_redirect_count: Option<i64>,
    /// If true, disables reporting partial query failures as part of the result set
    #[serde(rename = "deferpartialqueryfailures")]
    pub defer_partial_query_failures: Option<bool>,
    /// A hint to use shuffle strategy for materialized views that are referenced in the query.
    /// The property is an array of materialized views names and the shuffle keys to use.
    /// Examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys)
    pub materialized_view_shuffle: Option<serde_json::Value>,
    /// Overrides the default maximum amount of memory a whole query may allocate per node
    pub max_memory_consumption_per_query_per_node: Option<u64>,
    /// Overrides the default maximum amount of memory a query operator may allocate.
    #[serde(rename = "maxmemoryconsumptionperiterator")]
    pub max_memory_consumption_per_iterator: Option<u64>,
    /// Overrides the default maximum number of columns a query is allowed to produce.
    #[serde(rename = "maxoutputcolumns")]
    pub max_output_columns: Option<u64>,
    /// Enables setting the request timeout to its maximum value.
    #[serde(rename = "norequesttimeout")]
    pub no_request_timeout: Option<bool>,
    /// Enables suppressing truncation of the query results returned to the caller.
    #[serde(rename = "notruncation")]
    pub no_truncation: Option<bool>,
    /// If true, push simple selection through aggregation
    pub push_selection_through_aggregation: Option<bool>,
    /// When evaluating the bin_auto() function, the start value to use.
    pub query_bin_auto_at: Option<String>,
    /// When evaluating the bin_auto() function, the bin size value to use.
    pub query_bin_auto_size: Option<String>,
    /// The default parameter value of the cursor_after() function when called without parameters.
    pub query_cursor_after_default: Option<String>,
    /// The default parameter value of the cursor_before_or_at() function when called without parameters.
    pub query_cursor_before_or_at_default: Option<String>,
    /// Overrides the cursor value returned by the cursor_current() or current_cursor() functions.
    pub query_cursor_current: Option<String>,
    /// Disables usage of cursor functions in the context of the query.
    pub query_cursor_disabled: Option<bool>,
    /// List of table names that should be scoped to cursor_after_default .. cursor_before_or_at_default (upper bound is optional).
    pub query_cursor_scoped_tables: Option<Vec<String>>,
    /// Controls the query's datascope -- whether the query applies to all data or just part of it.
    query_datascope: Option<DataScope>,
    /// Controls the column name for the query's datetime scope (query_datetimescope_to / query_datetimescope_from).
    #[serde(rename = "query_datetimescope_column")]
    pub query_datetime_scope_column: Option<String>,
    /// Controls the query's datetime scope (earliest) -- used as auto-applied filter on query_datetimescope_column only (if defined).
    #[serde(rename = "query_datetimescope_from")]
    pub query_datetime_scope_from: Option<KustoDateTime>,
    /// Controls the query's datetime scope (latest) -- used as auto-applied filter on query_datetimescope_column only (if defined).
    #[serde(rename = "query_datetimescope_to")]
    pub query_datetime_scope_to: Option<KustoDateTime>,
    /// If set, controls the way the subquery merge behaves: the executing node will introduce an additional
    /// level in the query hierarchy for each subgroup of nodes; the size of the subgroup is set by this option.
    pub query_distribution_nodes_span: Option<i32>,
    /// The percentage of nodes to fan out execution to.
    pub query_fanout_nodes_percent: Option<i32>,
    /// The percentage of threads to fan out execution to.
    pub query_fanout_threads_percent: Option<i32>,
    /// If specified, forces Row Level Security rules, even if row_level_security policy is disabled
    pub query_force_row_level_security: Option<bool>,
    /// Controls how the query text is to be interpreted.
    pub query_language: Option<QueryLanguage>,
    ///  Enables logging of the query parameters, so that they can be viewed later in the .show queries journal.
    pub query_log_query_parameters: Option<bool>,
    /// Overrides the default maximum number of entities in a union.
    pub query_max_entities_in_union: Option<i64>,
    /// Overrides the datetime value returned by the now(0s) function.
    pub query_now: Option<KustoDateTime>,
    ///  If set, generate python debug query for the enumerated python node (default first).
    pub query_python_debug: Option<i32>,
    /// If set, retrieves the schema of each tabular data in the results of the query instead of the data itself.
    pub query_results_apply_getschema: Option<bool>,
    /// If positive, controls the maximum age of the cached query results the service is allowed to return
    pub query_results_cache_max_age: Option<KustoDuration>,
    /// If set, enables per-shard query cache.
    pub query_results_cache_per_shard: Option<bool>,
    /// Hint for Kusto as to how many records to send in each update (takes effect only if OptionResultsProgressiveEnabled is set)
    pub query_results_progressive_row_count: Option<i64>,
    /// Hint for Kusto as to how often to send progress frames (takes effect only if OptionResultsProgressiveEnabled is set)
    pub query_results_progressive_update_period: Option<i32>,
    ///  Enables limiting query results to this number of records.
    pub query_take_max_records: Option<i64>,
    /// Controls query consistency
    #[serde(skip_serializing_if = "Option::is_none", rename = "queryconsistency")]
    pub query_consistency: Option<QueryConsistency>,
    /// Request application name to be used in the reporting (e.g. show queries).
    pub request_app_name: Option<String>,
    /// If specified, blocks access to tables for which row_level_security policy is enabled
    pub request_block_row_level_security: Option<bool>,
    /// If specified, indicates that the request can't call-out to a user-provided service.
    pub request_callout_disabled: Option<bool>,
    /// Arbitrary text that the author of the request wants to include as the request description.
    pub request_description: Option<String>,
    /// If specified, indicates that the request can't invoke code in the ExternalTable.
    pub request_external_table_disabled: Option<bool>,
    /// If specified, indicates that the service should not impersonate the caller's identity.
    pub request_impersonation_disabled: Option<bool>,
    /// If specified, indicates that the request can't write anything.
    pub request_readonly: Option<bool>,
    ///  If specified, indicates that the request can't access remote databases and clusters.
    pub request_remote_entities_disabled: Option<bool>,
    /// If specified, indicates that the request can't invoke code in the sandbox.
    pub request_sandboxed_execution_disabled: Option<bool>,
    /// Request user to be used in the reporting (e.g. show queries).
    pub request_user: Option<String>,
    /// If set, enables the progressive query stream
    pub results_progressive_enabled: Option<bool>,
    /// Overrides the default request timeout.
    #[serde(rename = "servertimeout")]
    pub server_timeout: Option<KustoDuration>,
    /// Overrides the default maximum number of records a query is allowed to return to the caller (truncation).
    #[serde(rename = "truncationmaxrecords")]
    pub truncation_max_records: Option<i64>,
    /// Overrides the default maximum data size a query is allowed to return to the caller (truncation).
    #[serde(rename = "truncationmaxsize")]
    pub truncation_max_size: Option<i64>,
    /// Validates user's permissions to perform the query and doesn't run the query itself.
    pub validate_permissions: Option<bool>,
    /// If set, enables the newlines between frames in the progressive query stream.
    #[builder(default = "Some(true)")]
    results_v2_newlines_between_frames: Option<bool>,
    /// Additional options to be passed to the service.
    #[serde(flatten)]
    pub additional: HashMap<String, String>,
}
