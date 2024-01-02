use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt};
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

/// A reader that converts kusto's json object format into json lines.
pub struct ToJsonLinesReader<T: AsyncRead> {
    reader: T,
    read_initial_bracket: bool,
    should_skip_next: bool,
    finished: bool,
}

impl<T: AsyncRead> ToJsonLinesReader<T> {
    pub(crate) fn new(reader: T) -> Self {
        Self {
            reader,
            read_initial_bracket: false,
            should_skip_next: false,
            finished: false,
        }
    }
}

#[async_trait]
impl<T: AsyncRead> AsyncRead for ToJsonLinesReader<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        pin!(self);
        pin!(buf);
        if !self.read_initial_bracket {
            let mut bracket = [0u8; 1];
            futures::ready!(self.reader.read_exact(&mut bracket))?;
            if bracket[0] != b'[' {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected initial bracket",
                )));
            }
            self.read_initial_bracket = true;
        }

        let read = futures::ready!(self.reader.read(buf))?;

        let mut actual_index = 0;

        for i in 0..read {
            if self.should_skip_next {
                self.should_skip_next = false;
                actual_index -= 1;
                match buf[i] {
                    b',' => continue,
                    b']' => {
                        self.finished = true;
                        return Poll::Ready(Ok(actual_index));
                    }
                    _ => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Unexpected character after newline",
                        )))
                    }
                }
            }

            if buf[i] == b'\n' {
                if self.should_skip_next {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Unexpected newline",
                    )));
                }
                self.should_skip_next = true;
            }

            buf[actual_index] = buf[i];
            actual_index += 1;
        }

        Poll::Ready(Ok(actual_index))
    }
}
