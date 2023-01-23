use std::io;
use std::io::Read;

use futures::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, stream, Stream};
use serde::de::DeserializeOwned;

// TODO: Find a crate that does this better / move this into another crate

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

async fn deserialize_single<T: DeserializeOwned>(
    reader: &mut (impl AsyncBufRead + Send  + Unpin),
    buf: &mut Vec<u8>,
) -> io::Result<T> {
    buf.resize(0, 0);
    let size = reader.read_until(b'\n', buf).await?;
    return Ok(serde_json::from_slice(&buf[..size-1])?);
}

async fn read_byte(reader: &mut (impl AsyncBufRead + Send  + Unpin)) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).await?;
    Ok(buf[0])
}

async fn yield_next_obj<T: DeserializeOwned>(
    reader: &mut (impl AsyncBufRead + Send  + Unpin),
    buf: &mut Vec<u8>,
) -> Result<Option<T>, io::Error> {
    Ok(Some(match read_byte(reader).await? {
        b'[' => {
            let newline = read_byte(reader).await?;
            if newline != b'\n' {
                return Err(invalid_data(&format!("Expected newline after opening '[', found {:?}", newline)));
            }
            deserialize_single(reader, buf).await?
        }
        b',' => deserialize_single(reader, buf).await?,
        b']' => return Ok(None),
        b => return Err(invalid_data(&format!("Unexpected byte {:?}", b))),
    }
    ))
}

pub fn iter_results<T: DeserializeOwned>(
    reader: (impl AsyncBufRead + Send  + Unpin),
) -> impl Stream<Item=Result<T, io::Error>> {
    let buf = vec![];

    stream::try_unfold((buf, reader),  move |(mut buf, mut reader)| async {
        yield_next_obj(&mut reader, &mut buf).await.map(|r| r.map(|obj| {
            (obj, (buf, reader))
        }))
    })
}
