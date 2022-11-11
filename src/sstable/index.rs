use std::{
    future::poll_fn,
    io::ErrorKind as IoErrorKind,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use pin_project_lite::pin_project;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncReadExt, BufReader},
};
use tokio_stream::Stream;

use crate::{error::Error, fixme};

pub(crate) static INDEX: &str = "index";

enum IndexInterleaverState {
    Active(Option<IndexRow>),
    Done,
}

pub(crate) type DynIndexStream =
    Box<dyn Stream<Item = IndexRowResult> + Send + Sync + Unpin + 'static>;

type IndexRowResult = Result<IndexRow, Error>;

pin_project! {
    /// yields values from multiple indexes in alphabetical order, discarding all but the newest timestamp for duplicates.
    pub(crate) struct CompactionIndexInterleaver {
        streams: Vec<DynIndexStream>,
        states: Vec<IndexInterleaverState>,
        buf: Vec<(usize, IndexRow)>,
        has_errored: bool,
    }
}

impl IndexInterleaverState {
    fn row(&self) -> Option<&IndexRow> {
        match self {
            IndexInterleaverState::Active(row) => row.as_ref(),
            _ => None,
        }
    }

    fn pop_row(&mut self) -> Option<IndexRow> {
        match self {
            IndexInterleaverState::Active(row) => row.take(),
            _ => None,
        }
    }
}

// Given several IndexRows, yields all index values in alphabetical order,
// discarding duplicates (keeping those with the newest timestamp).
impl CompactionIndexInterleaver {
    /// Create a new index interleaver. Streams must yield their values in alphabetical order,
    /// and must be internally unique. Duplicates between streams will be resolved by a timestamp comparison (newest wins).
    pub(crate) fn new_from_streams(streams: Vec<DynIndexStream>) -> Self {
        let states = (0..streams.len())
            .map(|_| IndexInterleaverState::Active(None))
            .collect();

        Self {
            streams,
            states,
            buf: vec![],
            has_errored: false,
        }
    }

    pub(crate) async fn next_row(&mut self) -> Option<Result<(usize, IndexRow), Error>> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next_row(cx)).await
    }

    fn poll_next_row(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<(usize, IndexRow), Error>>> {
        let this = self.project();

        // Check all streams; if any is pending we must return pending.
        for (index, stream) in this.streams.iter_mut().enumerate() {
            match this.states.get(index).unwrap() {
                IndexInterleaverState::Active(None) => {
                    tracing::debug!("poll_next_row: {index} :: Active(None)");
                    match Pin::new(stream).poll_next(context) {
                        Poll::Ready(row) => match row {
                            None => {
                                tracing::debug!("poll_next_row: {index} :: --> Poll::Ready(None)");
                                this.states[index] = IndexInterleaverState::Done;
                            }
                            Some(Ok(row)) => {
                                tracing::debug!(
                                    "poll_next_row: {index} :: --> Poll::Ready(Some({row:?}))"
                                );
                                this.states[index] = IndexInterleaverState::Active(Some(row));
                            }

                            Some(Err(e)) => {
                                *this.has_errored = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        },

                        Poll::Pending => {
                            tracing::debug!("poll_next_row: {index} :: --> Poll::Pending");
                            return Poll::Pending;
                        }
                    }
                }
                IndexInterleaverState::Active(Some(_row)) => {
                    tracing::debug!("poll_next_row: already fetched a row for {index}");
                }
                IndexInterleaverState::Done => {
                    tracing::debug!("poll_next_row: {index} DONE");
                    // this one isn't involved in the comparison
                    // tracing::debug!("index is done");
                }
            }
        }

        // find the alphabetically first item of the current fetched rows
        let mut fetched: Vec<_> = this
            .states
            .iter()
            .enumerate()
            .filter_map(|(i, f)| f.row().map(|r| (i, r)))
            .collect();

        if fetched.is_empty() {
            return Poll::Ready(None);
        }

        // each index should have be complete de-duplicated internally, so we only need to compare the 'head' positions.
        // Sort by string (reversed), then timestamp if the data is equal.
        // The alphabetically-first string with the newest timestamp should appear at the top.
        fetched.sort_by(|(_i, a), (_j, b)| {
            let string_cmp = a.key.cmp(&b.key).reverse();

            if string_cmp.is_eq() {
                a.timestamp.cmp(&b.timestamp)
            } else {
                string_cmp
            }
        });

        let tail = fetched.pop().unwrap();

        fetched.retain(|(_, row)| {
            if row.key == tail.1.key {
                true
            } else {
                tracing::debug!(">>>> DROPPING DUPLICATE {row:?}");
                false
            }
        });

        // drop the borrows
        let tail = tail.0;
        let indexes = fetched.into_iter().map(|r| r.0).collect::<Vec<_>>();

        // discard the duplicates for this key
        for index in indexes {
            this.states[index].pop_row();
        }

        let index_row = this.states[tail].pop_row().unwrap();

        Poll::Ready(Some(Ok((tail, index_row))))
    }
}

#[derive(Debug)]
pub struct IndexRow {
    pub key: String,
    pub timestamp: Duration,
    pub(crate) start: u64,
    pub(crate) len: u64,
}

pin_project! {
    pub(crate) struct IndexRows {
        #[pin]
        reader: BufReader<File>,
        internal_buf: Vec<u8>,
        has_errored: bool,
    }
}

impl IndexRows {
    pub(crate) async fn from_path(index_path: impl AsRef<Path>) -> Self {
        let reader = BufReader::new(
            tokio::fs::File::open(index_path.as_ref())
                .await
                .expect("IndexRows::from_path: index_path {index_path:?} not found"),
        );
        IndexRows {
            reader,
            internal_buf: Vec::new(),
            has_errored: false,
        }
    }

    pub(crate) async fn next_row(&mut self) -> Result<Option<IndexRow>, Error> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next_row(cx)).await
    }

    fn poll_next_row(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IndexRow>, Error>> {
        if self.has_errored {
            return Poll::Ready(Ok(None));
        }

        let mut this = self.project();

        loop {
            match parse_index_row(this.internal_buf.as_slice()) {
                Ok((row, count)) => {
                    let mut new_buf = this.internal_buf.split_off(count);
                    fixme("is there a better way to discard consumed bytes?");
                    std::mem::swap(&mut new_buf, &mut this.internal_buf);

                    return Poll::Ready(Ok(Some(row)));
                }
                Err(e) => {
                    if e.kind() == IoErrorKind::UnexpectedEof {
                        let result = this.reader.as_mut().poll_fill_buf(cx);

                        match result {
                            Poll::Ready(Ok(slice)) => {
                                let len = slice.len();
                                this.internal_buf.extend_from_slice(slice);
                                this.reader.as_mut().consume(len);

                                // no more bytes to fetch
                                if len == 0 {
                                    return Poll::Ready(Ok(None));
                                }
                            }

                            Poll::Ready(Err(e)) => {
                                *this.has_errored = true;
                                return Poll::Ready(Err(e.into()));
                            }
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Stream for IndexRows {
    type Item = IndexRowResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_row(cx).map(Result::transpose)
    }
}

impl Stream for CompactionIndexInterleaver {
    type Item = Result<(usize, IndexRow), Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_row(cx)
    }
}

fn parse_index_row(bytes: &[u8]) -> Result<(IndexRow, usize), std::io::Error> {
    let mut reader = std::io::Cursor::new(bytes);
    let mut keylen = [0; 1];
    std::io::Read::read_exact(&mut reader, &mut keylen)?;
    let keylen = keylen[0] as usize;

    let mut key = BytesMut::zeroed(keylen as usize);
    let mut secs = [0; 8];
    let mut nanos = [0; 4];
    let mut start = [0; 8];
    let mut len = [0; 8];

    std::io::Read::read_exact(&mut reader, &mut key)?;
    std::io::Read::read_exact(&mut reader, &mut secs)?;
    std::io::Read::read_exact(&mut reader, &mut nanos)?;
    std::io::Read::read_exact(&mut reader, &mut start)?;
    std::io::Read::read_exact(&mut reader, &mut len)?;

    Ok((
        IndexRow {
            key: std::str::from_utf8(key.as_ref()).unwrap().to_owned(), // String::from_utf8_lossy(key.into()).to_owned().into(),
            timestamp: Duration::new(u64::from_be_bytes(secs), u32::from_be_bytes(nanos)),
            start: u64::from_be_bytes(start),
            len: u64::from_be_bytes(len),
        },
        29 + keylen,
    ))
}

#[deprecated = "the bufreader wrapper version is better"]
async fn parse_index_row_async(bytes: &mut BufReader<File>) -> Result<IndexRow, std::io::Error> {
    let keylen = bytes.read_u8().await? as usize;

    let mut key = BytesMut::zeroed(keylen);
    bytes.read_exact(&mut key).await?;
    let secs = bytes.read_u64().await?;
    let nanos = bytes.read_u32().await?;
    let start = bytes.read_u64().await?;
    let len = bytes.read_u64().await?;

    Ok(IndexRow {
        key: std::str::from_utf8(key.as_ref()).unwrap().to_owned(), // String::from_utf8_lossy(key.into()).to_owned().into(),
        timestamp: Duration::new(secs, nanos),
        start,
        len,
    })
}

impl IndexRow {
    pub(crate) fn key(&self) -> &str {
        self.key.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[tokio::test]
    async fn test_this_index() {
        fixme("move this to a fixture");
        let path = PathBuf::from(
            "/Users/zach/data/table_0_test/1667358803-0770610000-MRyEyEZqSzebE8ir.index",
        );

        let file = tokio::fs::File::open(&path).await.unwrap();
        let reader = BufReader::new(file);
        let mut stream = IndexRows {
            reader,
            internal_buf: vec![],
            has_errored: false,
        };

        loop {
            let res = stream.next_row().await;
            println!("got {:?}", res);

            if res.is_ok() && res.unwrap().is_none() {
                break;
            }
        }
    }
}

pub(crate) fn serialize_row(key: &str, ts: &Duration, offset: usize, len: usize) -> BytesMut {
    let mut buf = BytesMut::new();
    let keylen = key.len() as u8;
    buf.put_u8(keylen);
    buf.put_slice(key.as_bytes());
    buf.put_u64(ts.as_secs());
    buf.put_u32(ts.subsec_nanos());
    buf.put_u64(offset as u64);
    buf.put_u64(len as u64);

    buf
}
