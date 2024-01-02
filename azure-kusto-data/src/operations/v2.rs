use std::io;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

use crate::models::v2;
use futures::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, Stream};

pub fn parse_frames_iterative(reader: impl AsyncBufRead) -> impl Stream<Item = Result<v2::Frame, io::Error>> {
    FrameParser {
        reader,
        buf: Vec::new(),
        finished: false,
    }
}


pub async fn parse_frames_full(
    mut reader: (impl AsyncBufRead + Send + Unpin),
) -> Result<Vec<v2::Frame>, io::Error> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    return Ok(serde_json::from_slice(&buf)?);
}


struct FrameParser<T: AsyncBufRead> {
    reader: T,
    buf: Vec<u8>,
    finished: bool,
}

impl<T: AsyncBufRead> Stream for FrameParser<T> {
    type Item = Result<v2::Frame, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        pin!(self);
        if self.finished {
            return Poll::Ready(None);
        }

        self.buf.clear();
        let read = futures::ready!(self.reader.read_until(b'\n', &mut self.buf))?;

        if read == 0 {
            return Poll::Ready(None);
        }

        let result: Result<v2::Frame, io::Error> =
            serde_json::from_slice(&self.buf[..read]).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e));

        self.finished = result.is_err();
        Poll::Ready(Some(result))
    }
}

