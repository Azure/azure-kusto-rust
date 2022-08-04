use futures::io::BufReader;
use futures::{pin_mut, stream, AsyncRead, AsyncReadExt, Stream, TryStreamExt};
use serde::de::DeserializeOwned;
use std::io;
use std::pin::Pin;

// TODO: Find a crate that does this better / move this into another crate

async fn read_skipping_ws(reader: impl AsyncRead + Send) -> io::Result<u8> {
    pin_mut!(reader);
    loop {
        let mut byte = 0u8;
        reader.read_exact(std::slice::from_mut(&mut byte)).await?;
        print!("{}", byte as char);
        if !byte.is_ascii_whitespace() {
            return Ok(byte);
        }
    }
}

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

const BUFFER_SIZE: usize = 4096;

async fn deserialize_single<T: DeserializeOwned, R: AsyncRead + Send>(
    reader: R,
) -> io::Result<(T, Vec<u8>)> {
    let mut vec = Vec::with_capacity(BUFFER_SIZE);
    let mut buf = [0; BUFFER_SIZE];
    let mut leftover = Vec::with_capacity(BUFFER_SIZE);

    pin_mut!(reader);

    loop {
        let size = reader.read(&mut buf).await?;
        print!("{}", String::from_utf8_lossy(&buf[..size]));
        vec.extend_from_slice(&buf[..size]);

        let res = serde_json::from_slice::<T>(vec.as_slice());

        match res {
            Ok(t) => return Ok((t, leftover)),
            Err(e) => {
                if e.is_syntax() {
                    let i = e.column() - 1;
                    leftover.extend_from_slice(&vec[i..]);
                    return Ok((serde_json::from_slice::<T>(&vec[..i])?, leftover));
                } else if e.is_eof() {
                    continue;
                } else {
                    return Err(e.into());
                }
            }
        }
    }
}

async fn yield_next_obj<T: DeserializeOwned, R: AsyncRead + Send>(
    reader: R,
    first_time: bool,
) -> io::Result<Option<(T, Vec<u8>)>> {
    pin_mut!(reader);

    match read_skipping_ws(&mut reader).await? {
        b'[' if first_time => {
            let (result, leftover) = deserialize_single(&mut reader).await?;
            Ok(Some((result, leftover)))
        }
        b',' if !first_time => {
            let (result, leftover) = deserialize_single(&mut reader).await?;
            Ok(Some((result, leftover)))
        }
        b']' if !first_time => Ok(None),
        c => Err(invalid_data(&format!("Unexpected char {}", c as char))),
    }
}

pub fn iter_results<T: DeserializeOwned, R: AsyncRead + Send + 'static>(
    reader: R,
) -> impl Stream<Item = Result<T, io::Error>> {
    stream::try_unfold(
        (
            Box::pin(BufReader::new(reader)) as Pin<Box<dyn AsyncRead + Send>>,
            true,
        ),
        |(mut reader, first_time)| async move {
            let result = yield_next_obj::<T, _>(reader.as_mut(), first_time).await?;
            Ok(result.map(|(result, leftover)| {
                (
                    result,
                    (
                        Box::pin(
                            stream::iter(vec![Ok(leftover.into_iter())])
                                .into_async_read()
                                .chain(reader),
                        ) as Pin<Box<dyn AsyncRead + Send>>,
                        false,
                    ),
                )
            }))
        },
    )
}
