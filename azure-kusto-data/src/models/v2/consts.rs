use serde::{Deserialize, Serialize};

/// Where errors are reported - within the data, at the end of the table, or at the end of the dataset.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ErrorReportingPlacement {
    /// Errors are reported within the data.
    InData,
    /// Errors are reported at the end of the table.
    EndOfTable,
    /// Errors are reported at the end of the dataset.
    EndOfDataSet,
}

/// The type of the fragment of a table (in progressive mode), instructs to how to use it.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum TableFragmentType {
    /// Append the data to the rest of the fragments.
    DataAppend,
    /// Replace all previous data with this fragment.
    DataReplace,
}

/// Categorizes data tables according to the role they play in the data set that a Kusto query returns.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum TableKind {
    /// The table contains the actual data returned by the query.
    PrimaryResult,
    /// Information about the runtime of query.
    QueryCompletionInformation,
    /// Trace Log for the query.
    QueryTraceLog,
    /// Perf log for the query.
    QueryPerfLog,
    /// Table of contents for the other parts.
    TableOfContents,
    /// Properties of the query.
    QueryProperties,
    /// Execution plan for the query.
    QueryPlan,
    /// Unknown table kind.
    Unknown,
}
