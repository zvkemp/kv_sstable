use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    sync::RwLock,
    time::Instant,
};
use tokio_stream::{Stream, StreamExt};
use xorf::{Filter, Xor8};

use crate::{
    error::Error,
    fixme, fixme_msg,
    memtable::MemTable,
    sstable::index::{CompactionIndexInterleaver, DynIndexStream, IndexRows},
    table::KeyRange,
    util::{md5sum, murmur3, Checksum, Uuid},
};

pub(crate) mod data;
pub(crate) mod index;

pub(crate) static SSTABLE_EXT: &str = "sstable";
pub(crate) static SUMMARY_EXT: &str = "summary";
pub(crate) static WRITE_LOG: &str = "write_log";

pub struct Topic {
    pub path: PathBuf,
}

impl Topic {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Topic { path: path.into() }
    }
}

#[derive(Debug)]
pub struct SSTable {
    // pub path: PathBuf,
    // pub uuid: Uuid,
    inner: Arc<SSTableInner>,
}

#[derive(Debug)]
struct SSTableInner {
    summary: Summary,
    mmap: Mmap,
    uuid: Uuid,
}

impl SSTable {
    // NOTE: Not deriving this because I don't want it to be public. The only clone should
    // be for doing compactions; anything else will cause a panic.
    pub(crate) fn _clone(&self) -> Self {
        SSTable {
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn uuid(&self) -> &Uuid {
        &self.inner.uuid
    }

    pub(crate) fn ptr_eq(&self, other: &SSTable) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) async fn decommission(self) -> Result<(), Error> {
        let mut tries = 5;
        let mut inner_arc = self.inner;

        // NOTE: this originally happened because during the gap between removing the compaction task
        // and finalizing the compaction, another compaction task could start. That has been fixed.
        let inner = loop {
            match Arc::try_unwrap(inner_arc) {
                Ok(inner) => break inner,
                Err(ret) => {
                    if tries == 0 {
                        panic!("memory leak");
                    }

                    inner_arc = ret;

                    tries -= 1;
                    tracing::error!("Someone holding onto a reference for a decomissioned table. remaining tries = {tries}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };

        inner.decommission().await
    }

    pub(crate) fn generation(&self) -> usize {
        self.uuid().generation()
    }

    pub(crate) fn key_range(&self) -> KeyRange {
        KeyRange {
            first_key: self.first_key().to_owned(),
            last_key: self.last_key().to_owned(),
            min_timestamp: *self.min_timestamp(),
            max_timestamp: *self.max_timestamp(),
        }
    }

    pub(crate) fn index_path(&self) -> PathBuf {
        self.inner.summary.path.join(INDEX)
    }

    pub(crate) fn uuid_string(&self) -> String {
        self.uuid().to_string()
    }

    pub(crate) async fn find_timestamp_greater_than(
        &mut self,
        key: &str,
        search_ts: &Duration,
    ) -> Result<Option<Duration>, Error> {
        // These conditionals should weed out almost everything
        if self.max_timestamp() > search_ts {
            if self.first_key() <= key && self.last_key() >= key {
                if let Some(IndexEntry { timestamp, .. }) = self.get_key(key).await? {
                    if &timestamp >= search_ts {
                        return Ok(Some(timestamp));
                    }
                }
            }
        }

        Ok(None)
    }

    pub(crate) fn max_timestamp(&self) -> &Duration {
        &self.inner.summary.max_timestamp
    }

    pub(crate) fn min_timestamp(&self) -> &Duration {
        &self.inner.summary.min_timestamp
    }

    pub(crate) fn first_key(&self) -> &str {
        self.inner.summary.first_key.as_str()
    }

    pub(crate) fn last_key(&self) -> &str {
        self.inner.summary.last_key.as_str()
    }
}

#[cfg(fixme)]
pin_project! {
    struct Md5BufReader<R> {
        #[pin]
        inner: BufReader<R>,
        hasher: Option<Md5>,
    }
}

#[cfg(fixme)]
impl<R: AsyncRead> AsyncRead for Md5BufReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();

        match this.inner.poll_read(cx, buf) {
            std::task::Poll::Ready(_) => todo!(),
            std::task::Poll::Pending => todo!(),
        }
    }
}

impl SSTable {
    pub fn new(summary: Summary, mmap: Mmap, uuid: Uuid) -> Self {
        // fixme: timestamp based v7?
        // let uuid = Uuid::new_v4();
        // let options = OptionOptions::new().create()
        // let f = tokio::fs::File::open(&path).await;
        SSTable {
            inner: Arc::new(SSTableInner {
                summary,
                mmap,
                uuid,
            }),
        }
    }

    pub async fn from_dir(path: &Path) -> Self {
        let uuid: Uuid = path.file_stem().unwrap().to_string_lossy().parse().unwrap();

        let summary_path = path.join(SUMMARY_EXT);
        let summary_file = tokio::fs::read(&summary_path).await.unwrap();
        let summary: Summary = bincode::deserialize(&summary_file).unwrap();

        let mmap_path = path.join(SSTABLE_EXT);
        let file = std::fs::File::open(&mmap_path).unwrap();

        fixme("does this need to be on spawn blocking?");
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };

        SSTable::new(summary, mmap, uuid)
    }

    async fn write_key_and_value(
        key: &str,
        ts: &Duration,
        mut bytes: &[u8],
        checksum: Checksum,
        sstable: &mut File,
        index_file: &mut File,
        offset: &mut usize,
        index_offset: &mut usize,
    ) -> Result<(), Error> {
        let len = bytes.len();

        let mut checksum_slice = checksum.as_slice();

        sstable.write_all_buf(&mut bytes).await.unwrap();
        sstable.write_all_buf(&mut checksum_slice).await.unwrap();
        let mut serialized_index_row = index::serialize_row(key, ts, *offset, len);
        let index_row_len = serialized_index_row.len();

        index_file
            .write_all_buf(&mut serialized_index_row)
            .await
            .unwrap();

        *index_offset += index_row_len;
        *offset += len + 16;

        Ok(())
    }
}

impl SSTableInner {
    async fn decommission(self) -> Result<(), Error> {
        let summary_path = self.summary.path.join(SUMMARY_EXT);
        let index_path = self.summary.path.join(INDEX);
        let sstable_path = self.summary.path.join(SSTABLE_EXT);

        // unmap before we delete the file!
        drop(self.mmap);

        tokio::fs::remove_file(sstable_path).await?;
        fixme("should we retain the index and summary in case we have a stream running?");
        // tokio::fs::remove_file(index_path).await?;
        fixme("should we retain the index and summary in case we have a stream running?");
        // tokio::fs::remove_file(summary_path).await?;
        // tokio::fs::remove_dir(self.summary.path).await?;
        fixme("we can clean up dangling indexes and summaries if the streams vec is empty");

        Ok(())
    }
}

use self::index::INDEX;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    key: String,
    offset: usize,
}

#[derive(Serialize, Deserialize)]
pub struct Summary {
    first_key: String,
    last_key: String,
    min_timestamp: Duration,
    max_timestamp: Duration,
    key_count: usize,
    timestamp: Duration,
    path: PathBuf,
    filter: Xor8,
    #[deprecated = "find a better way to interact with and cache indexes"]
    #[serde(skip)]
    index: RwLock<Option<LoadedIndex>>,
    #[serde(default)]
    samples: Vec<Sample>,
}

impl std::fmt::Debug for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Summary")
            .field("first_key", &self.first_key)
            .field("last_key", &self.last_key)
            .field("min_timestamp", &self.min_timestamp)
            .field("max_timestamp", &self.max_timestamp)
            .field("key_count", &self.key_count)
            .field("timestamp", &self.timestamp)
            .field("path", &self.path)
            .field("filter", &"Xor8<...>")
            .field("samples", &self.samples)
            .field("index", &self.index)
            .finish()
    }
}

pub(crate) struct SSTableWriter {
    uuid: Uuid,
}

impl SSTableWriter {
    // we will inherit a UUID from a memtable
    pub(crate) fn from_uuid(uuid: Uuid) -> Self {
        Self { uuid }
    }

    pub(crate) fn from_generation(generation: usize) -> Self {
        let uuid = Uuid::new_with_generation(generation);

        Self::from_uuid(uuid)
    }

    pub(crate) async fn write_data(
        &self,
        path: &Path,
        mut sorted_data: impl Stream<Item = Result<(Duration, String, impl AsRef<[u8]>), Error>> + Unpin,
    ) -> Result<SSTable, Error> {
        let enclosing_directory = path.join(self.uuid.to_string()).to_owned();
        let working_directory = enclosing_directory.with_extension("writing");

        if enclosing_directory.is_dir()
            || enclosing_directory.is_file()
            || working_directory.is_dir()
            || working_directory.is_file()
        {
            return Err(Error::SSTableAlreadyExists {
                path: enclosing_directory,
            });
        }

        tokio::fs::create_dir_all(&working_directory).await?;

        // .../root_dir/shard/:table_name/:uuid.writing/sstable
        // .../root_dir/shard/:table_name/:uuid.writing/index
        // .../root_dir/shard/:table_name/:uuid.writing/summary
        let sstable_path = working_directory.join(SSTABLE_EXT);
        let index_path = working_directory.join(INDEX);
        let summary_path = working_directory.join(SUMMARY_EXT);

        let mut options = OpenOptions::new();
        options.create(true);
        options.write(true);

        let mut sstable_file = options.open(&sstable_path).await.unwrap();
        let mut index_file = options.open(&index_path).await.unwrap();

        let mut offset = 0usize;
        let mut index_offset = 0usize;

        let mut first_key = None;
        let mut last_key = None;
        let mut min_timestamp = None;
        let mut max_timestamp = None;
        let mut key_count = 0;
        let mut fingerprints = HashSet::new();
        let mut samples = vec![];

        while let Some(res) = sorted_data.next().await {
            let (timestamp, key, data) = res?;
            if first_key.is_none() {
                first_key = Some(key.clone());
            }

            // FIXME: better way to track this?
            last_key = Some(key.clone());

            if min_timestamp.is_none() {
                min_timestamp = Some(timestamp);
            }

            if max_timestamp.is_none() {
                max_timestamp = Some(timestamp);
            }

            // FIXME: do this without copying?
            min_timestamp = std::cmp::min(min_timestamp, Some(timestamp));
            max_timestamp = std::cmp::max(max_timestamp, Some(timestamp));

            key_count += 1;

            // FIXME: necessary to re-sum values during a compaction? We probably also check during read
            let checksum = md5sum(&data);

            // #[cfg(fixme)]
            // let (ts, bytes) = self.data.get(&key).unwrap();
            SSTable::write_key_and_value(
                &key,
                &timestamp,
                data.as_ref(),
                checksum,
                &mut sstable_file,
                &mut index_file,
                &mut offset,
                &mut index_offset,
            )
            .await
            .unwrap();

            fixme("variable sample rate");
            if key_count > 0 && key_count % 128 == 0 {
                samples.push(Sample {
                    key: key.clone(),
                    offset: index_offset,
                });
            }

            let f = murmur3(&mut key.as_bytes()) as u64;
            fingerprints.insert(f);
        }

        index_file.flush().await.unwrap();
        index_file.sync_all().await.unwrap();

        let Some(first_key) = first_key else {
            return Err(Error::NoDataWasWritten);
        };

        let Some(last_key) = last_key else {
            return Err(Error::NoDataWasWritten);
        };

        let fingerprints = fingerprints.into_iter().collect::<Vec<_>>();
        let filter = Xor8::from(&fingerprints);

        let summary = Summary {
            first_key,
            last_key,
            min_timestamp: min_timestamp
                .expect("should not have missing timestamps if a key was written"),
            max_timestamp: max_timestamp
                .expect("should not have missing timestamps if a key was written"),
            key_count,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            filter,
            path: enclosing_directory.clone(),
            samples,
            index: RwLock::new(None),
        };

        let serialized_summary = bincode::serialize(&summary).unwrap();
        fixme("fixme - this unwrap has panicked; no such file or directory - perhaps if a compaction was in progress?");
        tokio::fs::write(&summary_path, serialized_summary)
            .await
            .expect(&format!("Panic on summary_path {:?}", summary_path));

        tokio::fs::rename(working_directory, &enclosing_directory).await?;

        // This will only happen when persisting a memtable
        let _ = tokio::fs::remove_file(enclosing_directory.with_extension("write_log")).await;

        Ok(SSTable::from_dir(enclosing_directory.as_path()).await)
    }
}

impl SSTable {
    #[cfg(feature = "loaded_index")]
    pub async fn get_key(&mut self, key: &str) -> Option<(Duration, u64, u64)> {
        fixme("where does this belong?");

        fixme("it probably makes more sense to keep the index in a LRU row cache attached to the Table ");
        fixme("that can page in chunks of this index as necessary");
        if self.inner.summary.index.read().await.is_none() {
            let mut write = self.inner.summary.index.write().await;
            let entries = self.inner.summary.load_index().await;
            *write = Some(entries);
        }

        let idx_read = self.inner.summary.index.read().await;
        idx_read.as_ref().unwrap().get(key).map(ToOwned::to_owned)
    }

    #[cfg(not(feature = "loaded_index"))]
    pub async fn get_key(&mut self, key: &str) -> Result<Option<IndexEntry>, Error> {
        use std::borrow::Borrow;

        use tokio::io::AsyncSeekExt;

        let mut offset_lower = 0;
        for sample in self.inner.summary.samples.iter() {
            if sample.key.as_str() > key {
                break;
            } else {
                offset_lower = sample.offset;
            }
        }

        let mut reader = BufReader::new(self.inner.summary.read_index().await?);
        reader
            .seek(std::io::SeekFrom::Start(offset_lower as u64))
            .await?;
        // if offset_upper is none, there weren't enough keys to do a meaningful sample

        while let Ok((idx_key, entry)) = read_index_entry(&mut reader).await {
            match Borrow::<str>::borrow(&idx_key).cmp(key) {
                std::cmp::Ordering::Equal => {
                    return Ok(Some(entry));
                }
                std::cmp::Ordering::Greater => break,
                _ => {}
            }
        }

        Ok(None)
    }

    pub async fn get(&mut self, key: &str) -> Result<(Duration, Bytes), Error> {
        if key < self.first_key() || key > self.last_key() {
            return Err(Error::KeyNotInRange);
        }
        tracing::debug!(
            "SSTABLE::get[{key} ({} .. {})]",
            self.inner.summary.first_key,
            self.inner.summary.last_key
        );

        let fingerprint = murmur3(&mut key.as_bytes()) as u64;

        if !self.inner.summary.filter.contains(&fingerprint) {
            return Err(Error::KeyNotInXorFilter);
        }

        let IndexEntry {
            timestamp,
            position,
            len,
        } = self
            .get_key(key)
            .await?
            .ok_or_else(|| Error::DataNotFound {
                key: key.to_owned(),
            })?;

        // let now = Instant::now();

        let start = position as usize;
        let end = (position + len) as usize;
        let data = &self.inner.mmap[start..end];
        let checksum = &self.inner.mmap[end..(end + 16)];

        let checksum2 = md5sum(data);

        if checksum != checksum2 {
            return Err(Error::InvalidChecksum {
                key: key.to_string(),
                table_path: self.inner.summary.path.to_string_lossy().into(),
            });
        }

        // let mut ts_data = Bytes::copy_from_slice(data);
        // let data = ts_data.split_off(std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
        // let mut reader = BufReader::new(ts_data.as_ref());

        // let seconds = reader.read_u64().await.unwrap();
        // let nanoseconds = reader.read_u32().await.unwrap();

        // let timestamp = Duration::new(seconds, nanoseconds);

        // Ok((timestamp, data))
        Ok((timestamp, Bytes::copy_from_slice(data)))
    }

    pub(crate) async fn compact(tables: &[SSTable]) -> Result<SSTable, Error> {
        // FIXME: can we run some sort of progress marker to avoid duplicating long-running work, if the process has to restart?
        // Or is it good enough to just seek the source files to after the last complete entry in the compacted index?
        // For now, assume we just need to restart
        fixme("validate all tables belong to the same table path");
        fixme("validate all tables belong to the same generation");
        if tables.is_empty() {
            return Err(Error::NoTablesInCompaction);
        }

        let generation = tables.first().unwrap().generation() + 1;
        // let uuid = new_uuid_with_generation(&timestamp(), generation);
        let root_path = tables
            .first()
            .unwrap()
            .inner
            .summary
            .path
            .parent()
            .unwrap()
            .to_owned();

        let index_paths = tables.iter().map(|x| x.index_path()).collect::<Vec<_>>();

        let mut index_iterators = Vec::with_capacity(index_paths.len());

        for index_path in index_paths {
            let stream = IndexRows::from_path(&index_path).await;
            index_iterators.push(Box::new(stream) as DynIndexStream);
        }

        let original_key_count: usize = tables.iter().map(|x| x.inner.summary.key_count).sum();

        let interleaver = CompactionIndexInterleaver::new_from_streams(index_iterators);

        let writer = SSTableWriter::from_generation(generation);

        let data_stream = interleaver.map(|res| {
            res.map(|(table_index, row)| {
                let timestamp = row.timestamp;
                let key = row.key;

                let start = row.start as usize;
                let end = start + row.len as usize;

                let data = &(tables.get(table_index).unwrap().inner.mmap)[start..end];

                (timestamp, key, data)
            })
        });

        let new_sstable = writer.write_data(&root_path, data_stream).await?;

        let key_count = new_sstable.inner.summary.key_count;
        tracing::warn!(
            "compaction done; key_count delta is {original_key_count} - {key_count} = {}",
            original_key_count - key_count
        );
        Ok(new_sstable)
    }
}

impl Summary {
    #[deprecated = "not useful for large indexes - we don't want to load the whole thing into memory"]
    async fn load_index(&self) -> LoadedIndex {
        let mut index = BufReader::new(self.read_index().await.unwrap());
        let mut entries: HashMap<String, IndexEntry> = HashMap::new();

        while let Ok((key, entry)) = read_index_entry(&mut index).await {
            entries.insert(key.into(), entry);
        }

        entries
    }

    async fn read_index(&self) -> Result<File, Error> {
        let index_path = self.path.join(INDEX);
        let file = tokio::fs::File::open(&index_path).await?;
        // this wrapper speeds things up by like 20x
        Ok(file)
    }
}

#[derive(Debug)]
pub struct IndexEntry {
    timestamp: Duration,
    position: u64,
    len: u64,
}
async fn read_index_entry<T: tokio::io::AsyncRead + Unpin>(
    index: &mut BufReader<T>,
) -> Result<(String, IndexEntry), Error> {
    let keylen = match index.read_u8().await {
        Ok(kl) => kl,
        Err(_e) => Err(_e)?,
    };
    let mut key = BytesMut::zeroed(keylen as usize);
    index.read_exact(&mut key).await.unwrap();
    let secs = index.read_u64().await.unwrap();
    let nanos = index.read_u32().await.unwrap();
    let position = index.read_u64().await.unwrap();
    let len = index.read_u64().await.unwrap();
    let timestamp = Duration::new(secs, nanos);

    Ok((
        String::from_utf8_lossy(&key).into(),
        IndexEntry {
            timestamp,
            position,
            len,
        },
    ))
}

type LoadedIndex = HashMap<String, IndexEntry>;

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::ops::Range;

    use super::*;

    pub(crate) fn make_key(i: usize) -> String {
        format!("key_{i:0>6}")
    }

    /// Returns an sstable and a list of the keys/timestamps that were inserted into it.
    pub(crate) async fn build_test_sstable(
        generation: usize,
        path: &Path,
        range: Range<usize>,
        timestamps: impl Iterator<Item = Duration>,
    ) -> (SSTable, Vec<(String, Duration)>) {
        let uuid = Uuid::new_with_generation(generation);
        let table_writer = SSTableWriter::from_uuid(uuid);

        let mut all_inserts = vec![];

        let data_stream = range.zip(timestamps).map(|(index, timestamp)| {
            let data = format!("data for key {index} {timestamp:?}");
            let key = make_key(index);

            all_inserts.push((key.clone(), timestamp));
            Ok((timestamp, key, data.as_bytes().to_vec()))
        });

        (
            table_writer
                .write_data(path, tokio_stream::iter(data_stream))
                .await
                .unwrap(),
            all_inserts,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::util::{murmur2, rand_bytes, timestamp};

    use super::*;
    use rand::distributions::Standard;
    use rand::Rng;
    use tempdir::TempDir;
    use tokio::time::Instant;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_memtable_to_disk() {
        let tempdir = TempDir::new("test_memtable_to_disk").unwrap();

        let mut memtable = MemTable::new(Some(tempdir.path())).await.unwrap();

        println!("tempdir={tempdir:?}");

        for i in 0..1000 {
            let len = (i + 10) * 10;
            let data = rand_bytes(len);
            let key = format!("key/for/{i:0>7}");
            memtable.insert(key, timestamp(), data).await.unwrap();
            println!("{i} {len}");
        }

        let data_798 = memtable
            .data
            .get(&format!("key/for/0000798"))
            .unwrap()
            .clone();
        let data_432 = memtable
            .data
            .get(&format!("key/for/0000432"))
            .unwrap()
            .clone();
        let data_124 = memtable
            .data
            .get(&format!("key/for/0000124"))
            .unwrap()
            .clone();

        println!("data generated");

        let mut sstable = memtable
            .to_persistent_storage(tempdir.path())
            .await
            .unwrap();

        // println!("summary={summary:?}");

        let (_, fetched_798) = sstable.get("key/for/0000798").await.unwrap();
        let (_, fetched_124) = sstable.get("key/for/0000124").await.unwrap();
        let (_, fetched_432) = sstable.get("key/for/0000432").await.unwrap();

        assert_eq!(fetched_124, data_124.1);
        assert_eq!(fetched_432, data_432.1);
        assert_eq!(fetched_798, data_798.1);

        let time = Instant::now();

        for i in 0..1000 {
            println!("{i}");
            let (_, _fetched_124) = sstable.get("key/for/0000124").await.unwrap();
        }

        println!("1000 fetches in {:?}", time.elapsed());
        // let fetched_432 = summary.get(format!("key/for/432")).await.unwrap();
        // let fetched_124 = summary.get(format!("key/for/124")).await.unwrap();

        let index_path = sstable.inner.summary.path.join(INDEX);
        let mut stream = IndexRows::from_path(index_path).await;

        while let Some(index_row) = stream.next().await {
            println!("{index_row:?}");
        }
    }

    #[test]
    fn test_u32_bytes() {
        dbg!(1073741824u64.to_be_bytes());
    }

    #[test]
    fn test_murmur() {
        for i in 0..1 {
            let key = format!("key_{i:0>6}");
            dbg!(murmur2(key.as_bytes()));
        }
    }
}
