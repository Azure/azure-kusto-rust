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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RequestOptions {
    /// If set and positive, indicates the maximum number of HTTP redirects that the client will process.
    client_max_redirect_count: Option<i64>,
    /// If true, disables reporting partial query failures as part of the result set
    deferpartialqueryfailures: Option<bool>,
    /// A hint to use shuffle strategy for materialized views that are referenced in the query.
    /// The property is an array of materialized views names and the shuffle keys to use.
    /// Examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys)
    materialized_view_shuffle: Option<serde_json::Value>,
    /// Overrides the default maximum amount of memory a whole query may allocate per node
    max_memory_consumption_per_query_per_node: Option<u64>,
    /// Overrides the default maximum amount of memory a query operator may allocate.
    maxmemoryconsumptionperiterator: Option<u64>,
    /// Overrides the default maximum number of columns a query is allowed to produce.
    maxoutputcolumns: Option<u64>,
    /// Enables setting the request timeout to its maximum value.
    norequesttimeout: Option<bool>,
    /// Enables suppressing truncation of the query results returned to the caller.
    notruncation: Option<bool>,
    /// If true, push simple selection through aggregation
    push_selection_through_aggregation: Option<bool>,
    /// When evaluating the bin_auto() function, the start value to use.
    query_bin_auto_at: Option<String>,
    /// When evaluating the bin_auto() function, the bin size value to use.
    query_bin_auto_size: Option<String>,
    /// The default parameter value of the cursor_after() function when called without parameters.
    query_cursor_after_default: Option<String>,
    /// The default parameter value of the cursor_before_or_at() function when called without parameters.
    query_cursor_before_or_at_default: Option<String>,
    /// Overrides the cursor value returned by the cursor_current() or current_cursor() functions.
    query_cursor_current: Option<String>,
    /// Disables usage of cursor functions in the context of the query.
    query_cursor_disabled: Option<bool>,
    /// List of table names that should be scoped to cursor_after_default .. cursor_before_or_at_default (upper bound is optional).
    query_cursor_scoped_tables: Option<Vec<String>>,
    // Controls the query's datascope -- whether the query applies to all data or just part of it.
    query_datascope: Option<DataScope>,
    /// Controls the column name for the query's datetime scope (query_datetimescope_to / query_datetimescope_from).
    query_datetimescope_column: Option<String>,
    /// Controls the query's datetime scope (earliest) -- used as auto-applied filter on query_datetimescope_column only (if defined).
    query_datetimescope_from: Option<KustoDateTime>,
    /// Controls the query's datetime scope (latest) -- used as auto-applied filter on query_datetimescope_column only (if defined).
    query_datetimescope_to: Option<KustoDateTime>,
    /// If set, controls the way the subquery merge behaves: the executing node will introduce an additional
    /// level in the query hierarchy for each subgroup of nodes; the size of the subgroup is set by this option.
    query_distribution_nodes_span: Option<i32>,
    /// The percentage of nodes to fan out execution to.
    query_fanout_nodes_percent: Option<i32>,
    /// The percentage of threads to fan out execution to.
    query_fanout_threads_percent: Option<i32>,
    /// If specified, forces Row Level Security rules, even if row_level_security policy is disabled
    query_force_row_level_security: Option<bool>,
    /// Controls how the query text is to be interpreted.
    query_language: Option<QueryLanguage>,
    ///  Enables logging of the query parameters, so that they can be viewed later in the .show queries journal.
    query_log_query_parameters: Option<bool>,
    /// Overrides the default maximum number of entities in a union.
    query_max_entities_in_union: Option<i64>,
    /// Overrides the datetime value returned by the now(0s) function.
    query_now: Option<KustoDateTime>,
    ///  If set, generate python debug query for the enumerated python node (default first).
    query_python_debug: Option<i32>,
    /// If set, retrieves the schema of each tabular data in the results of the query instead of the data itself.
    query_results_apply_getschema: Option<bool>,
    /// If positive, controls the maximum age of the cached query results the service is allowed to return
    query_results_cache_max_age: Option<KustoDuration>,
    /// If set, enables per-shard query cache.
    query_results_cache_per_shard: Option<bool>,
    /// Hint for Kusto as to how many records to send in each update (takes effect only if OptionResultsProgressiveEnabled is set)
    query_results_progressive_row_count: Option<i64>,
    /// Hint for Kusto as to how often to send progress frames (takes effect only if OptionResultsProgressiveEnabled is set)
    query_results_progressive_update_period: Option<i32>,
    ///  Enables limiting query results to this number of records.
    query_take_max_records: Option<i64>,
    /// Controls query consistency
    queryconsistency: Option<QueryConsistency>,
    /// Request application name to be used in the reporting (e.g. show queries).
    request_app_name: Option<String>,
    /// If specified, blocks access to tables for which row_level_security policy is enabled
    request_block_row_level_security: Option<bool>,
    /// If specified, indicates that the request can't call-out to a user-provided service.
    request_callout_disabled: Option<bool>,
    /// Arbitrary text that the author of the request wants to include as the request description.
    request_description: Option<String>,
    /// If specified, indicates that the request can't invoke code in the ExternalTable.
    request_external_table_disabled: Option<bool>,
    /// If specified, indicates that the service should not impersonate the caller's identity.
    request_impersonation_disabled: Option<bool>,
    /// If specified, indicates that the request can't write anything.
    request_readonly: Option<bool>,
    ///  If specified, indicates that the request can't access remote databases and clusters.
    request_remote_entities_disabled: Option<bool>,
    /// If specified, indicates that the request can't invoke code in the sandbox.
    request_sandboxed_execution_disabled: Option<bool>,
    /// Request user to be used in the reporting (e.g. show queries).
    request_user: Option<String>,
    /// If set, enables the progressive query stream
    results_progressive_enabled: Option<bool>,
    /// Overrides the default request timeout.
    servertimeout: Option<KustoDuration>,
    /// Overrides the default maximum number of records a query is allowed to return to the caller (truncation).
    truncationmaxrecords: Option<i64>,
    /// Overrides the default maximum data size a query is allowed to return to the caller (truncation).
    truncationmaxsize: Option<i64>,
    /// Validates user's permissions to perform the query and doesn't run the query itself.
    validate_permissions: Option<bool>,
}

#[derive(Default)]
pub struct RequestOptionsBuilder(RequestOptions);

impl RequestOptionsBuilder {
    /// If set and positive, indicates the maximum number of HTTP redirects that the client will process.
    pub fn with_client_max_redirect_count(&mut self, count: i64) -> &mut Self {
        self.0.client_max_redirect_count = Some(count);
        self
    }

    /// If true, disables reporting partial query failures as part of the result set
    pub fn with_defer_partial_query_failures(
        &mut self,
        defer_partial_query_failures: bool,
    ) -> &mut Self {
        self.0.deferpartialqueryfailures = Some(defer_partial_query_failures);
        self
    }

    /// A hint to use shuffle strategy for materialized views that are referenced in the query.
    /// The property is an array of materialized views names and the shuffle keys to use.
    /// Examples: 'dynamic([ { "Name": "V1", "Keys" : [ "K1", "K2" ] } ])' (shuffle view V1 by K1, K2) or 'dynamic([ { "Name": "V1" } ])' (shuffle view V1 by all keys)
    pub fn with_materialized_view_shuffle(
        &mut self,
        materialized_view_shuffle: serde_json::Value,
    ) -> &mut Self {
        self.0.materialized_view_shuffle = Some(materialized_view_shuffle);
        self
    }

    /// Overrides the default maximum amount of memory a whole query may allocate per node
    pub fn with_max_memory_consumption_per_query_per_node(
        &mut self,
        max_memory_consumption_per_query_per_node: u64,
    ) -> &mut Self {
        self.0.max_memory_consumption_per_query_per_node =
            Some(max_memory_consumption_per_query_per_node);
        self
    }

    /// Overrides the default maximum amount of memory a query operator may allocate.
    pub fn with_max_memory_consumption_per_iterator(
        &mut self,
        max_memory_consumption_per_iterator: u64,
    ) -> &mut Self {
        self.0.maxmemoryconsumptionperiterator = Some(max_memory_consumption_per_iterator);
        self
    }

    /// Overrides the default maximum number of columns a query is allowed to produce.
    pub fn with_max_output_columns(&mut self, max_output_columns: u64) -> &mut Self {
        self.0.maxoutputcolumns = Some(max_output_columns);
        self
    }

    /// Enables setting the request timeout to its maximum value.
    pub fn with_no_request_timeout(&mut self, no_request_timeout: bool) -> &mut Self {
        self.0.norequesttimeout = Some(no_request_timeout);
        self
    }

    /// Enables suppressing truncation of the query results returned to the caller.
    pub fn with_no_truncation(&mut self, no_truncation: bool) -> &mut Self {
        self.0.notruncation = Some(no_truncation);
        self
    }

    /// If true, push simple selection through aggregation
    pub fn with_push_selection_through_aggregation(
        &mut self,
        push_selection_through_aggregation: bool,
    ) -> &mut Self {
        self.0.push_selection_through_aggregation = Some(push_selection_through_aggregation);
        self
    }

    /// When evaluating the bin_auto() function, the start value to use.
    pub fn with_query_bin_auto_at(&mut self, query_bin_auto_at: impl Into<String>) -> &mut Self {
        self.0.query_bin_auto_at = Some(query_bin_auto_at.into());
        self
    }

    /// When evaluating the bin_auto() function, the bin size value to use.
    pub fn with_query_bin_auto_size(
        &mut self,
        query_bin_auto_size: impl Into<String>,
    ) -> &mut Self {
        self.0.query_bin_auto_size = Some(query_bin_auto_size.into());
        self
    }

    /// The default parameter value of the cursor_after() function when called without parameters.
    pub fn with_query_cursor_after_default(
        &mut self,
        query_cursor_after_default: impl Into<String>,
    ) -> &mut Self {
        self.0.query_cursor_after_default = Some(query_cursor_after_default.into());
        self
    }

    /// The default parameter value of the cursor_before_or_at() function when called without parameters.
    pub fn with_query_cursor_before_or_at_default(
        &mut self,
        query_cursor_before_or_at_default: impl Into<String>,
    ) -> &mut Self {
        self.0.query_cursor_before_or_at_default = Some(query_cursor_before_or_at_default.into());
        self
    }

    /// Overrides the cursor value returned by the cursor_current() or current_cursor() functions.
    pub fn with_query_cursor_current(
        &mut self,
        query_cursor_current: impl Into<String>,
    ) -> &mut Self {
        self.0.query_cursor_current = Some(query_cursor_current.into());
        self
    }

    /// Disables usage of cursor functions in the context of the query.
    pub fn with_query_cursor_disabled(&mut self, query_cursor_disabled: bool) -> &mut Self {
        self.0.query_cursor_disabled = Some(query_cursor_disabled);
        self
    }

    /// List of table names that should be scoped to cursor_after_default .. cursor_before_or_at_default (upper bound is optional).
    pub fn with_query_cursor_scoped_tables(
        &mut self,
        query_cursor_scoped_tables: Vec<String>,
    ) -> &mut Self {
        self.0.query_cursor_scoped_tables = Some(query_cursor_scoped_tables);
        self
    }

    /// // Controls the query's datascope -- whether the query applies to all data or just part of it.
    pub fn with_query_datascope(&mut self, query_datascope: DataScope) -> &mut Self {
        self.0.query_datascope = Some(query_datascope);
        self
    }

    /// Controls the column name for the query's datetime scope (query_datetimescope_to / query_datetimescope_from).
    pub fn with_query_datetimescope_column(
        &mut self,
        query_datetimescope_column: impl Into<String>,
    ) -> &mut Self {
        self.0.query_datetimescope_column = Some(query_datetimescope_column.into());
        self
    }

    /// Controls the query's datetime scope (earliest)
    /// used as auto-applied filter on query_datetimescope_column only (if defined).
    pub fn with_query_datetimescope_from(
        &mut self,
        query_datetimescope_from: impl Into<KustoDateTime>,
    ) -> &mut Self {
        self.0.query_datetimescope_from = Some(query_datetimescope_from.into());
        self
    }

    /// Controls the query's datetime scope (latest)
    /// used as auto-applied filter on query_datetimescope_column only (if defined).
    pub fn with_query_datetimescope_to(
        &mut self,
        query_datetimescope_to: impl Into<KustoDateTime>,
    ) -> &mut Self {
        self.0.query_datetimescope_to = Some(query_datetimescope_to.into());
        self
    }

    /// If set, controls the way the subquery merge behaves: the executing node will introduce an additional
    /// level in the query hierarchy for each subgroup of nodes; the size of the subgroup is set by this option.
    pub fn with_query_distribution_nodes_span(
        &mut self,
        query_distribution_nodes_span: i32,
    ) -> &mut Self {
        self.0.query_distribution_nodes_span = Some(query_distribution_nodes_span);
        self
    }

    /// The percentage of nodes to fan out execution to.
    pub fn with_query_fanout_nodes_percent(
        &mut self,
        query_fanout_nodes_percent: i32,
    ) -> &mut Self {
        self.0.query_fanout_nodes_percent = Some(query_fanout_nodes_percent);
        self
    }

    /// The percentage of threads to fan out execution to.
    pub fn with_query_fanout_threads_percent(
        &mut self,
        query_fanout_threads_percent: i32,
    ) -> &mut Self {
        self.0.query_fanout_threads_percent = Some(query_fanout_threads_percent);
        self
    }

    /// If specified, forces Row Level Security rules, even if row_level_security policy is disabled
    pub fn with_query_force_row_level_security(
        &mut self,
        query_force_row_level_security: bool,
    ) -> &mut Self {
        self.0.query_force_row_level_security = Some(query_force_row_level_security);
        self
    }

    /// Controls how the query text is to be interpreted.
    pub fn with_query_language(&mut self, query_language: QueryLanguage) -> &mut Self {
        self.0.query_language = Some(query_language);
        self
    }

    ///  Enables logging of the query parameters, so that they can be viewed later in the .show queries journal.
    pub fn with_query_log_query_parameters(
        &mut self,
        query_log_query_parameters: bool,
    ) -> &mut Self {
        self.0.query_log_query_parameters = Some(query_log_query_parameters);
        self
    }

    /// Overrides the default maximum number of entities in a union.
    pub fn with_query_max_entities_in_union(
        &mut self,
        query_max_entities_in_union: i64,
    ) -> &mut Self {
        self.0.query_max_entities_in_union = Some(query_max_entities_in_union);
        self
    }

    /// Overrides the datetime value returned by the now(0s) function.
    pub fn with_query_now(&mut self, query_now: impl Into<KustoDateTime>) -> &mut Self {
        self.0.query_now = Some(query_now.into());
        self
    }

    ///  If set, generate python debug query for the enumerated python node (default first).
    pub fn with_query_python_debug(&mut self, query_python_debug: i32) -> &mut Self {
        self.0.query_python_debug = Some(query_python_debug);
        self
    }

    /// If set, retrieves the schema of each tabular data in the results of the query instead of the data itself.
    pub fn with_query_results_apply_getschema(
        &mut self,
        query_results_apply_getschema: bool,
    ) -> &mut Self {
        self.0.query_results_apply_getschema = Some(query_results_apply_getschema);
        self
    }

    /// If positive, controls the maximum age of the cached query results the service is allowed to return
    pub fn with_query_results_cache_max_age(
        &mut self,
        query_results_cache_max_age: impl Into<KustoDuration>,
    ) -> &mut Self {
        self.0.query_results_cache_max_age = Some(query_results_cache_max_age.into());
        self
    }

    /// If set, enables per-shard query cache.
    pub fn with_query_results_cache_per_shard(
        &mut self,
        query_results_cache_per_shard: bool,
    ) -> &mut Self {
        self.0.query_results_cache_per_shard = Some(query_results_cache_per_shard);
        self
    }

    /// Hint for Kusto as to how many records to send in each update
    /// (takes effect only if OptionResultsProgressiveEnabled is set)
    pub fn with_query_results_progressive_row_count(
        &mut self,
        query_results_progressive_row_count: i64,
    ) -> &mut Self {
        self.0.query_results_progressive_row_count = Some(query_results_progressive_row_count);
        self
    }

    /// Hint for Kusto as to how often to send progress frames
    /// (takes effect only if OptionResultsProgressiveEnabled is set)
    pub fn with_query_results_progressive_update_period(
        &mut self,
        query_results_progressive_update_period: i32,
    ) -> &mut Self {
        self.0.query_results_progressive_update_period =
            Some(query_results_progressive_update_period);
        self
    }

    ///  Enables limiting query results to this number of records.
    pub fn with_query_take_max_records(&mut self, query_take_max_records: i64) -> &mut Self {
        self.0.query_take_max_records = Some(query_take_max_records);
        self
    }

    /// Controls query consistency
    pub fn with_query_consistency(&mut self, query_consistency: QueryConsistency) -> &mut Self {
        self.0.queryconsistency = Some(query_consistency);
        self
    }

    /// Request application name to be used in the reporting (e.g. show queries).
    pub fn with_request_app_name(&mut self, request_app_name: impl Into<String>) -> &mut Self {
        self.0.request_app_name = Some(request_app_name.into());
        self
    }

    /// If specified, blocks access to tables for which row_level_security policy is enabled
    pub fn with_request_block_row_level_security(
        &mut self,
        request_block_row_level_security: bool,
    ) -> &mut Self {
        self.0.request_block_row_level_security = Some(request_block_row_level_security);
        self
    }

    /// If specified, indicates that the request can't call-out to a user-provided service.
    pub fn with_request_callout_disabled(&mut self, request_callout_disabled: bool) -> &mut Self {
        self.0.request_callout_disabled = Some(request_callout_disabled);
        self
    }

    /// Arbitrary text that the author of the request wants to include as the request description.
    pub fn with_request_description(
        &mut self,
        request_description: impl Into<String>,
    ) -> &mut Self {
        self.0.request_description = Some(request_description.into());
        self
    }

    /// If specified, indicates that the request can't invoke code in the ExternalTable.
    pub fn with_request_external_table_disabled(
        &mut self,
        request_external_table_disabled: bool,
    ) -> &mut Self {
        self.0.request_external_table_disabled = Some(request_external_table_disabled);
        self
    }

    /// If specified, indicates that the service should not impersonate the caller's identity.
    pub fn with_request_impersonation_disabled(
        &mut self,
        request_impersonation_disabled: bool,
    ) -> &mut Self {
        self.0.request_impersonation_disabled = Some(request_impersonation_disabled);
        self
    }

    /// If specified, indicates that the request can't write anything.
    pub fn with_request_readonly(&mut self, request_readonly: bool) -> &mut Self {
        self.0.request_readonly = Some(request_readonly);
        self
    }

    ///  If specified, indicates that the request can't access remote databases and clusters.
    pub fn with_request_remote_entities_disabled(
        &mut self,
        request_remote_entities_disabled: bool,
    ) -> &mut Self {
        self.0.request_remote_entities_disabled = Some(request_remote_entities_disabled);
        self
    }

    /// If specified, indicates that the request can't invoke code in the sandbox.
    pub fn with_request_sandboxed_execution_disabled(
        &mut self,
        request_sandboxed_execution_disabled: bool,
    ) -> &mut Self {
        self.0.request_sandboxed_execution_disabled = Some(request_sandboxed_execution_disabled);
        self
    }

    /// Request user to be used in the reporting (e.g. show queries).
    pub fn with_request_user(&mut self, request_user: impl Into<String>) -> &mut Self {
        self.0.request_user = Some(request_user.into());
        self
    }

    /// If set, enables the progressive query stream
    pub fn with_results_progressive_enabled(
        &mut self,
        results_progressive_enabled: bool,
    ) -> &mut Self {
        self.0.results_progressive_enabled = Some(results_progressive_enabled);
        self
    }

    /// Overrides the default request timeout.
    pub fn with_server_timeout(&mut self, server_timeout: impl Into<KustoDuration>) -> &mut Self {
        self.0.servertimeout = Some(server_timeout.into());
        self
    }

    /// Overrides the default maximum number of records a query is allowed to return to the caller (truncation).
    pub fn with_truncation_max_records(&mut self, truncation_max_records: i64) -> &mut Self {
        self.0.truncationmaxrecords = Some(truncation_max_records);
        self
    }

    /// Overrides the default maximum data size a query is allowed to return to the caller (truncation).
    pub fn with_truncation_max_size(&mut self, truncation_max_size: i64) -> &mut Self {
        self.0.truncationmaxsize = Some(truncation_max_size);
        self
    }

    /// Validates user's permissions to perform the query and doesn't run the query itself.
    pub fn with_validate_permissions(&mut self, validate_permissions: bool) -> &mut Self {
        self.0.validate_permissions = Some(validate_permissions);
        self
    }
}
