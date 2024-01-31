use crate::error::ParseError;
use crate::error::{Error::JsonError, Partial, PartialExt, Result};
use crate::models::v2;
use crate::models::v2::{DataTable, Frame, QueryCompletionInformation, QueryProperties, TableKind};
use futures::lock::Mutex;
use futures::{
    pin_mut, stream, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, Stream, StreamExt, TryStreamExt,
};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

pub fn parse_frames_iterative(
    reader: impl AsyncBufRead + Unpin,
) -> impl Stream<Item = Result<Frame>> {
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

struct StreamingDataset {
    header: OM<v2::DataSetHeader>,
    completion: OM<v2::DataSetCompletion>,
    query_properties: OM<Vec<QueryProperties>>,
    query_completion_information: OM<Vec<QueryCompletionInformation>>,
    results: Receiver<Result<DataTable>>,
}

impl StreamingDataset {
    fn new(stream: impl Stream<Item = Result<Frame>> + Send + 'static) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let res = StreamingDataset {
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
            if let Err(e) = tokio_res.populate_with_stream(stream, tx).await {
                let _ = tx.send(Err(e)).await; // Best effort to send the error to the receiver
            }
        });

        res
    }

    async fn populate_with_stream(
        &self,
        stream: impl Stream<Item = Result<Frame>>,
        tx: Sender<Partial<DataTable>>,
    ) -> Result<()> {
        pin_mut!(stream);

        let mut current_table = Some(DataTable {
            table_id: 0,
            table_name: "".to_string(),
            table_kind: TableKind::PrimaryResult,
            columns: Vec::new(),
            rows: Vec::new(),
        });

        while let Some(frame) = stream.try_next().await.transpose() {
            let frame = frame?;
            match frame {
                Frame::DataSetHeader(header) => {
                    self.header.lock().await.replace(header);
                }
                Frame::DataSetCompletion(completion) => {
                    self.completion.lock().await.replace(completion);
                }
                Frame::DataTable(table) if table.table_kind == TableKind::QueryProperties => {
                    let mut query_properties = self.query_properties.lock().await;
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
                    let mut query_completion = self.query_completion_information.lock().await;
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
                    let mut table = current_table
                        .take()
                        .ok_or(ParseError::Frame("Table is unexpectedly none".into()))?;
                    table.table_id = table_header.table_id;
                    table.table_name = table_header.table_name.clone();
                    table.table_kind = table_header.table_kind;
                    table.columns = table_header.columns.clone();
                }
                Frame::TableFragment(table_fragment) => {
                    current_table
                        .take()
                        .ok_or(ParseError::Frame(
                            "TableFragment without TableHeader".into(),
                        ))?
                        .rows
                        .extend(table_fragment.rows);
                }
                Frame::TableCompletion(table_completion) => {
                    tx.send(Ok(current_table.take().ok_or(ParseError::Frame(
                        "TableCompletion without TableHeader".into(),
                    ))?))
                    .await?;
                }
                Frame::TableProgress(_) => {}
            }
        }

        Ok(())
    }
}

// test

#[cfg(test)]
mod tests {
    use crate::models::test_helpers::{v2_files_full, v2_files_iterative};
    use futures::io::Cursor;
    use futures::StreamExt;

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
                .collect::<Vec<_>>()
                .await;
            assert_eq!(parsed_frames, frames);
        }
    }
}
