use std::sync::Arc;
use crate::error::{Error, Error::JsonError, Partial, Result};
use crate::models::v2;
use futures::{stream, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, Stream, StreamExt, TryStreamExt, pin_mut};
use futures::lock::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::models::v2::{DataSetCompletion, DataSetHeader, DataTable, Frame, QueryCompletionInformation, QueryProperties, Row, TableKind};

pub fn parse_frames_iterative(
    reader: impl AsyncBufRead + Unpin,
) -> impl Stream<Item=Result<Frame>> {
    let buf = Vec::with_capacity(4096);
    stream::unfold((reader, buf), |(mut reader, mut buf)| async move {
        buf.clear();
        let size = reader.read_until(b'\n', &mut buf).await.ok()? - 1;
        if size <= 0 {
            return None;
        }

        dbg!(String::from_utf8_lossy(&buf[1..size]));

        if buf[0] == b']' {
            return None;
        }

        Some((
            serde_json::from_slice(&buf[1..size]).map_err(JsonError),
            (reader, buf),
        ))
    })
}

async fn parse_frames_full(
    mut reader: (impl AsyncBufRead + Send + Unpin),
) -> Result<Vec<Frame>> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    return Ok(serde_json::from_slice(&buf)?);
}


/// Arc Mutex
type M<T> = Arc<Mutex<T>>;
/// Arc Mutex Option
type OM<T> = M<Option<T>>;

pub struct IterativeDataset {
    pub header: OM<v2::DataSetHeader>,
    pub completion: OM<v2::DataSetCompletion>,
    pub query_properties: OM<Vec<QueryProperties>>,
    pub query_completion_information: OM<Vec<QueryCompletionInformation>>,
    pub results: Receiver<Result<IterativeTable>>,
}

struct IterativeTable {
    pub table_id: i32,
    pub table_name: String,
    pub table_kind: TableKind,
    pub columns: Vec<v2::Column>,
    pub rows: Receiver<Result<Vec<Row>>>,
}

impl IterativeDataset {
    pub fn from_async_buf_read(stream: impl AsyncBufRead + Send + Unpin + 'static) -> Arc<Self> {
        let stream = parse_frames_iterative(stream).map_err(|e| (None, e))?;
        Self::new(stream)
    }

    fn new(stream: impl Stream<Item=Result<Frame>> + Send + 'static) -> Arc<Self> {
        // TODO: make channel size configurable
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let res = IterativeDataset {
            header: Arc::new(Mutex::new(None)),
            completion: Arc::new(Mutex::new(None)),
            query_properties: Arc::new(Mutex::new(None)),
            query_completion_information: Arc::new(Mutex::new(None)),
            results: rx,
        };
        let res = Arc::new(res);
        let tokio_res = res.clone();
        // TODO: to spawn a task we have to have a runtime. We wanted to be runtime independent, and that may still be a desire, but currently azure core isn't, so we might as well use tokio here.
        tokio::spawn(async move {
            tokio_res.populate_with_stream(stream, tx).await;
        });

        res
    }

    async fn populate_with_stream(&self, stream: impl Stream<Item=Result<Frame>>, tx: Sender<Result<IterativeTable>>) {
        pin_mut!(stream);

        let mut rows_tx = None;

        while let Some(frame) = stream.try_next().await.transpose() {
            // TODO: handle errors
            let Ok(frame) = frame else {
                tx.send(Err(Error::ExternalError("Failed to parse frames".to_string()))).await.expect("failed to send error");
                continue;
            };

            match frame {
                Frame::DataSetHeader(header) => {
                    self.header.lock().await.replace(header);
                }
                Frame::DataSetCompletion(completion) => {
                    if let Some(errs) = &completion.one_api_errors {
                        // todo - better error than crashing when failing to send
                        tx.send(Err(errs.clone().into())).await.expect("failed to send error");
                    }
                    self.completion.lock().await.replace(completion);
                }
                // TODO: properly handle errors/missing
                Frame::DataTable(table) if table.table_kind == TableKind::QueryProperties => {
                    self.query_properties.lock().await.replace(table.deserialize_values::<QueryProperties>().expect("failed to deserialize query properties"));
                }
                Frame::DataTable(table) if table.table_kind == TableKind::QueryCompletionInformation => {
                    self.query_completion_information.lock().await.replace(table.deserialize_values::<QueryCompletionInformation>().expect("failed to deserialize query completion information"));
                }
                Frame::DataTable(table) => {
                    let (datatable_tx, datatable_rx) = tokio::sync::mpsc::channel(1);

                    tx.send(Ok(IterativeTable {
                        table_id: table.table_id,
                        table_name: table.table_name,
                        table_kind: table.table_kind,
                        columns: table.columns,
                        rows: datatable_rx,
                    })).await.expect("failed to send table");

                    datatable_tx.send(Ok(table.rows)).await.expect("failed to send rows");
                }
                Frame::TableHeader(table_header) => {
                    let (rows_tx_, rows_rx) = tokio::sync::mpsc::channel(1);

                    tx.send(Ok(IterativeTable {
                        table_id: table_header.table_id,
                        table_name: table_header.table_name,
                        table_kind: table_header.table_kind,
                        columns: table_header.columns,
                        rows: rows_rx,
                    })).await.expect("failed to send table");

                    rows_tx = Some(rows_tx_);
                }
                Frame::TableFragment(table_fragment) => {
                    if let Some(rows_tx) = &mut rows_tx {
                        rows_tx.send(Ok(table_fragment.rows)).await.expect("failed to send rows");
                    }
                }
                Frame::TableCompletion(table_completion) => {
                    if let Some(rows_tx) = rows_tx.take() {
                        if let Some(errs) = &table_completion.one_api_errors {
                            // todo - better error than crashing when failing to send
                            rows_tx.send(Err(errs.clone().into())).await.expect("failed to send rows");
                        }
                    }
                }
                Frame::TableProgress(_) => {}
            }
        }
    }
}

pub struct FullDataset {
    pub header: Option<DataSetHeader>,
    pub completion: Option<DataSetCompletion>,
    pub query_properties: Option<Vec<QueryProperties>>,
    pub query_completion_information: Option<Vec<QueryCompletionInformation>>,
    pub results: Vec<DataTable>,
}

impl FullDataset {
    pub async fn from_async_buf_read(reader: impl AsyncBufRead + Send + Unpin + 'static) -> Partial<FullDataset> {
        let vec = parse_frames_full(reader).await.map_err(|e| (None, e))?;
        Self::from_frame_stream(stream::iter(vec.into_iter())).await
    }

    async fn from_frame_stream(stream: impl Stream<Item=Frame> + Send + 'static) -> Partial<FullDataset> {
        pin_mut!(stream);

        let mut dataset = FullDataset {
            header: None,
            completion: None,
            query_properties: None,
            query_completion_information: None,
            results: Vec::new(),
        };

        let mut current_table = Some(DataTable {
            table_id: 0,
            table_name: "".to_string(),
            table_kind: TableKind::PrimaryResult,
            columns: Vec::new(),
            rows: Vec::new(),
        });

        let mut errors: Vec<Error> = Vec::new();


        while let Some(frame) = stream.next().await {
            match frame {
                Frame::DataSetHeader(header) => {
                    dataset.header = Some(header);
                }
                Frame::DataSetCompletion(completion) => {
                    if let Some(errs) = &completion.one_api_errors {
                        errors.push(errs.clone().into());
                    }
                    dataset.completion = Some(completion);
                }
                // TODO: properly handle errors/missing
                Frame::DataTable(table) if table.table_kind == TableKind::QueryProperties => {
                    match table.deserialize_values::<QueryProperties>() {
                        Ok(query_properties) => {
                            dataset.query_properties = Some(query_properties);
                        }
                        Err((q, e)) => {
                            dataset.query_properties = q;
                            errors.push(e);
                        }
                    }
                }
                Frame::DataTable(table) if table.table_kind == TableKind::QueryCompletionInformation => {
                    match table.deserialize_values::<QueryCompletionInformation>() {
                        Ok(query_completion_information) => {
                            dataset.query_completion_information = Some(query_completion_information);
                        }
                        Err((q, e)) => {
                            dataset.query_completion_information = q;
                            errors.push(e);
                        }
                    }
                }
                Frame::DataTable(table) => {
                    dataset.results.push(table);
                }
                Frame::TableHeader(table_header) => {
                    if let Some(table) = &mut current_table {
                        table.table_id = table_header.table_id;
                        table.table_name = table_header.table_name.clone();
                        table.table_kind = table_header.table_kind;
                        table.columns = table_header.columns.clone();
                    }
                }
                Frame::TableFragment(table_fragment) => {
                    if let Some(table) = &mut current_table {
                        table.rows.extend(table_fragment.rows);
                    }
                }
                Frame::TableCompletion(table_completion) => {
                    //todo handle table errors
                    if let Some(table) = current_table.take() {
                        dataset.results.push(table);
                    }
                }
                Frame::TableProgress(_) => {}
            }
        }

        match &errors[..] {
            [] => Partial::Ok(dataset),
            [e] => Partial::Err((Some(dataset), (*e).clone())),
            _ => Partial::Err((Some(dataset), Error::MultipleErrors(errors))),
        }
    }
}

// test

#[cfg(test)]
mod tests {
    use futures::io::Cursor;
    use futures::StreamExt;
    use crate::models::test_helpers::{v2_files_full, v2_files_iterative};

    #[tokio::test]
    async fn test_parse_frames_full() {
        for (contents, frames) in v2_files_full() {
            println!("testing: {}", contents);
            let reader = Cursor::new(contents.as_bytes());
            let parsed_frames = super::parse_frames_full(reader).await.unwrap();
            assert_eq!(parsed_frames, frames);
        }
    }

    #[tokio::test]
    async fn test_parse_frames_iterative() {
        for (contents, frames) in v2_files_iterative() {
            println!("testing: {}", contents);
            let reader = Cursor::new(contents.as_bytes());
            let parsed_frames = super::parse_frames_iterative(reader)
                .map(|f| f.expect("failed to parse frame"))
                .collect::<Vec<_>>().await;
            assert_eq!(parsed_frames, frames);
        }
    }
}

