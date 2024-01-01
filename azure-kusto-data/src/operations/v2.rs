use std::io;

use crate::models::v2;
use crate::operations::skip_reader::ToJsonLinesReader;
use futures::io::BufReader;
use futures::{stream, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, Stream};

pub fn parse_frames_iterative(
    reader: (impl AsyncBufRead),
) -> impl Stream<Item = Result<v2::Frame, io::Error>> {
    let mut reader = BufReader::new(ToJsonLinesReader::new(reader));
    let mut buf = Vec::new();

    stream::unfold(&mut reader, |reader| async move {
        buf.clear();
        let read = reader.read_until(b'\n', &mut buf).await?;
        if read == 0 {
            return Ok(None);
        }
        let result = serde_json::from_slice(&buf[..read])?;
        Ok(Some((result, reader)))
    })
}

pub async fn parse_frames_full(
    mut reader: (impl AsyncBufRead + Send + Unpin),
) -> Result<Vec<v2::Frame>, io::Error> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    return Ok(serde_json::from_slice(&buf)?);
}
