use std::sync::Arc;
use crate::error::{Error::JsonError, Result};
use crate::models::v2;
use futures::{stream, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, Stream, StreamExt, TryStreamExt};
use futures::lock::Mutex;
use tokio::spawn;
use crate::models::v2::{DataTable, Frame, QueryCompletionInformation, QueryProperties, TableKind};

pub fn parse_frames_iterative(
    reader: impl AsyncBufRead + Unpin,
) -> impl Stream<Item = Result<v2::Frame>> {
    let buf = Vec::with_capacity(4096);
    stream::unfold((reader, buf), |(mut reader, mut buf)| async move {
        let size = reader.read_until(b'\n', &mut buf).await.ok()?;
        if size == 0 {
            return None;
        }

        if buf[0] == b']' {
            return None;
        }

        Some((
            serde_json::from_slice(&buf[1..size]).map_err(JsonError),
            (reader, buf),
        ))
    })
}

pub async fn parse_frames_full(
    mut reader: (impl AsyncBufRead + Send + Unpin),
) -> Result<Vec<v2::Frame>> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    return Ok(serde_json::from_slice(&buf)?);
}

struct Dataset {
    header : Option<v2::DataSetHeader>,
    completion : Option<v2::DataSetCompletion>,
    query_properties : Option<Vec<QueryProperties>>,
    query_completion_information : Option<Vec<QueryCompletionInformation>>,
    results : Vec<DataTable>,
}

impl Dataset {
    async fn from_stream(mut stream: impl Stream<Item = Result<Frame>>) -> Result<Self> {
        let mut dataset = Dataset {
            header : None,
            completion : None,
            query_properties : None,
            query_completion_information : None,
            results : Vec::new(),
        };
        let mut current_table = Some(DataTable {
            table_id: 0,
            table_name: "".to_string(),
            table_kind: TableKind::PrimaryResult,
            columns: Vec::new(),
            rows: Vec::new(),
        });

        while let Some(frame) = stream.try_next().await? {
            match frame {
                v2::Frame::DataSetHeader(header) => {
                    dataset.header = Some(header);
                },
                v2::Frame::DataSetCompletion(completion) => {
                    dataset.completion = Some(completion);
                },
                // TODO: properly handle errors/missing
                v2::Frame::DataTable(table) if table.table_kind == TableKind::QueryProperties => {
                    dataset.query_properties.replace(table.deserialize_values::<QueryProperties>().expect("failed to deserialize query properties"));
                },
                v2::Frame::DataTable(table) if table.table_kind == TableKind::QueryCompletionInformation => {
                    dataset.query_completion_information.replace(table.deserialize_values::<QueryCompletionInformation>().expect("failed to deserialize query completion information"));
                },
                v2::Frame::DataTable(table) => {
                    dataset.results.push(table);
                },
                // TODO - handle errors
                v2::Frame::TableHeader(table_header) => {
                    if let Some(table) = &mut current_table {
                        table.table_id = table_header.table_id;
                        table.table_name = table_header.table_name.clone();
                        table.table_kind = table_header.table_kind;
                        table.columns = table_header.columns.clone();
                    }
                }
                v2::Frame::TableFragment(table_fragment) => {
                    if let Some(table) = &mut current_table {
                        table.rows.extend(table_fragment.rows);
                    }
                }
                v2::Frame::TableCompletion(table_completion) => {
                    if let Some(table) = current_table.take() {
                        dataset.results.push(table);
                    }
                }
                Frame::TableProgress(_) => {}
            }
        }
        Ok(dataset)
    }
}


/// Arc Mutex
type M<T> = Arc<Mutex<T>>;
/// Arc Mutex Option
type OM<T> = M<Option<T>>;

struct StreamingDataset {
    header : OM<v2::DataSetHeader>,
    completion : OM<v2::DataSetCompletion>,
    query_properties : OM<Vec<QueryProperties>>,
    query_completion_information : OM<Vec<QueryCompletionInformation>>,
    results : M<Vec<DataTable>>,
    stream : M<dyn Stream<Item = Result<Frame>>>,
}

impl StreamingDataset {
    fn new(stream: impl Stream<Item=Result<Frame>> + Send + 'static) -> Self {
        StreamingDataset {
            header: Arc::new(Mutex::new(None)),
            completion: Arc::new(Mutex::new(None)),
            query_properties: Arc::new(Mutex::new(None)),
            query_completion_information: Arc::new(Mutex::new(None)),
            results: Arc::new(Mutex::new(Vec::new())),
            stream: Arc::new(Mutex::new(stream)),
        };
        // TODO: to spawn a task we have to have a runtime. We wanted to be runtime independent, and that may still be a desire, but currently azure core isn't, so we might as well use tokio here.
        tokio::spawn(
    }

}
