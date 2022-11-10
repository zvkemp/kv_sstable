use tokio_stream::Stream;

use crate::{
    error::Error,
    sstable::index::{CompactionIndexInterleaver, DynIndexStream, IndexRow, IndexRows},
};
use std::{path::Path, pin::Pin};

pub async fn list_keys_in_indexes(
    index_paths: impl Iterator<Item = &Path>,
) -> Pin<Box<dyn Stream<Item = Result<(usize, IndexRow), Error>>>> {
    let mut streams = vec![];

    for path in index_paths {
        streams.push(Box::new(IndexRows::from_path(path).await) as DynIndexStream);
    }

    Box::pin(CompactionIndexInterleaver::new_from_streams(streams))
}
