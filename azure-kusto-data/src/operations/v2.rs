use crate::error::{partial_from_tuple, Error, Error::JsonError, Partial, PartialExt, Result};
use crate::models::v2;
use crate::models::v2::{DataTable, Frame, QueryCompletionInformation, QueryProperties, TableKind};
use futures::lock::Mutex;
use futures::{
    pin_mut, stream, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, Stream, StreamExt, TryStreamExt,
};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

pub fn parse_frames_iterative(
    reader: impl AsyncBufRead + Unpin + Send + Sync
) -> impl Stream<Item = Result<Frame>> {
    let buf = Vec::with_capacity(4096);
    stream::unfold((reader, buf), |(mut reader, mut buf)| async move {
        buf.clear();
        let size = reader.read_until(b'\n', &mut buf).await.ok()? - 1;
        if size <= 0 {
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
) -> Result<Vec<Frame>> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    return Ok(serde_json::from_slice(&buf)?);
}

/// Arc Mutex
type M<T> = Arc<Mutex<T>>;
/// Arc Mutex Option
type OM<T> = M<Option<T>>;

pub(crate) struct IterativeDataset {
    header: OM<v2::DataSetHeader>,
    completion: OM<v2::DataSetCompletion>,
    query_properties: OM<Vec<QueryProperties>>,
    query_completion_information: OM<Vec<QueryCompletionInformation>>,
    results: Receiver<Partial<DataTable>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl IterativeDataset {
    pub fn new(stream: impl Stream<Item = Result<Frame>> + Send + 'static) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut res = IterativeDataset {
            header: Arc::new(Mutex::new(None)),
            completion: Arc::new(Mutex::new(None)),
            query_properties: Arc::new(Mutex::new(None)),
            query_completion_information: Arc::new(Mutex::new(None)),
            results: rx,
            join_handle: None,
        };

        let header = res.header.clone();
        let completion = res.completion.clone();
        let query_properties = res.query_properties.clone();
        let query_completion_information = res.query_completion_information.clone();

        // TODO: to spawn a task we have to have a runtime. We wanted to be runtime independent, and that may still be a desire, but currently azure core isn't, so we might as well use tokio here.
        let handle = tokio::spawn(async move {
            if let Err(e) = populate_with_stream(
                header,
                completion,
                query_properties,
                query_completion_information,
                stream,
                &tx,
            )
            .await
            {
                let _ = tx.send(e.into()).await; // Best effort to send the error to the receiver
            }
        });

        res.join_handle.replace(handle);


        res
    }
}

async fn populate_with_stream(
    header_store: OM<v2::DataSetHeader>,
    completion_store: OM<v2::DataSetCompletion>,
    query_properties: OM<Vec<QueryProperties>>,
    query_completion_information: OM<Vec<QueryCompletionInformation>>,
    stream: impl Stream<Item = Result<Frame>>,
    tx: &Sender<Partial<DataTable>>,
) -> Result<()> {
    pin_mut!(stream);

    let mut current_table = DataTable {
        table_id: 0,
        table_name: "".to_string(),
        table_kind: TableKind::PrimaryResult,
        columns: Vec::new(),
        rows: Vec::new(),
    };

    while let Some(frame) = stream.try_next().await.transpose() {
        let frame = frame?;
        match frame {
            Frame::DataSetHeader(header) => {
                header_store.lock().await.replace(header);
            }
            Frame::DataSetCompletion(completion) => {
                completion_store.lock().await.replace(completion);
            }
            Frame::DataTable(table) if table.table_kind == TableKind::QueryProperties => {
                let mut query_properties = query_properties.lock().await;
                match table
                    .deserialize_values::<QueryProperties>()
                    .ignore_partial_results()
                {
                    Ok(v) => {
                        query_properties.replace(v);
                    }
                    Err(e) => tx.send(e.into()).await?,
                }
            }
            Frame::DataTable(table)
                if table.table_kind == TableKind::QueryCompletionInformation =>
            {
                let mut query_completion = query_completion_information.lock().await;
                match table
                    .deserialize_values::<QueryCompletionInformation>()
                    .ignore_partial_results()
                {
                    Ok(v) => {
                        query_completion.replace(v);
                    }
                    Err(e) => tx.send(e.into()).await?,
                }
            }
            Frame::DataTable(table) => {
                tx.send(Ok(table)).await?;
            }
            Frame::TableHeader(table_header) => {
                current_table.table_id = table_header.table_id;
                current_table.table_name = table_header.table_name;
                current_table.table_kind = table_header.table_kind;
                current_table.columns = table_header.columns;
            }
            Frame::TableFragment(table_fragment) => {
                current_table.rows.extend(table_fragment.rows);
            }
            Frame::TableCompletion(table_completion) => {
                let new_table = std::mem::replace(
                    &mut current_table,
                    DataTable {
                        table_id: 0,
                        table_name: "".to_string(),
                        table_kind: TableKind::PrimaryResult,
                        columns: Vec::new(),
                        rows: Vec::new(),
                    },
                );
                tx.send(partial_from_tuple((
                    Some(new_table),
                    table_completion.one_api_errors.map(|e| {
                        e.into_iter()
                            .map(Error::QueryApiError)
                            .collect::<Vec<Error>>()
                            .into()
                    }),
                )))
                .await?;
            }
            Frame::TableProgress(_) => {}
        }
    }

    Ok(())
}

// test

#[cfg(test)]
mod tests {
    use crate::models::test_helpers::{v2_files_full, v2_files_iterative};
    use futures::io::Cursor;
    use futures::StreamExt;
    use crate::error::PartialExt;

    #[tokio::test]
    async fn test_parse_frames_full() {
        for (contents, frames) in v2_files_full() {
            let reader = Cursor::new(contents.as_bytes());
            let parsed_frames = super::parse_frames_full(reader).await.unwrap();
            assert_eq!(parsed_frames, frames);
        }
    }

    #[tokio::test]
    async fn test_parse_frames_iterative() {
        for (contents, frames) in v2_files_iterative() {
            let reader = Cursor::new(contents.as_bytes());
            let parsed_frames = super::parse_frames_iterative(reader)
                .map(|f| f.expect("failed to parse frame"))
                .collect::<Vec<_>>()
                .await;
            assert_eq!(parsed_frames, frames);
        }
    }

    #[tokio::test]
    async fn test_streaming_dataset() {
        for (contents, frames) in v2_files_iterative() {
            let reader = Cursor::new(contents.as_bytes());
            let mut dataset = super::IterativeDataset::new(super::parse_frames_iterative(reader));
            let mut tables = frames.into_iter();

            tables.next(); // skip the header
            tables.next(); // skip the query properties


            while let Some(table) = dataset.results.recv().await {
                let (table, errs) = table.to_tuple();
                let errs = errs.map(|e| match &e {
                    super::Error::QueryApiError(ex) => vec![ex.clone()],
                    super::Error::MultipleErrors(v) => v.iter().map(|e| match e {
                        super::Error::QueryApiError(ex) => ex.clone(),
                        _ => panic!("expected a query api error")
                    }).collect(),
                    _ => panic!("expected a query api error")
                });

                let table = table.expect("missing table");

                let frame = tables.next().expect("missing frame");
                if let super::Frame::TableHeader(expected_header) = frame {
                    assert_eq!(table.table_id, expected_header.table_id);
                    assert_eq!(table.table_name, expected_header.table_name);
                    assert_eq!(table.table_kind, expected_header.table_kind);
                    assert_eq!(table.columns, expected_header.columns);
                } else if let super::Frame::DataSetCompletion(completion) = frame {
                    assert_eq!(completion.one_api_errors, errs);
                    break;
                } else {
                    panic!("expected a table header or a completion frame");
                }

                let mut expected_rows = Vec::new();

                while let Some(fragment) = tables.next() {
                    if let super::Frame::TableFragment(expected_fragment) = fragment {
                        assert_eq!(table.table_id, expected_fragment.table_id);
                        expected_rows.extend(expected_fragment.rows);
                    } else if let super::Frame::TableCompletion(c) = fragment {
                        assert_eq!(c.one_api_errors, errs);
                        break;
                    }
                }

                assert_eq!(table.rows, expected_rows);
            }
        }
    }
}
