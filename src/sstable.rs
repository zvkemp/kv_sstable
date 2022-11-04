use std::{
    collections::HashMap,
    io::ErrorKind as IoErrorKind,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    stream,
    sync::RwLock,
    time::Instant,
};
use tokio_stream::{Stream, StreamExt};

use crate::{
    error::Error,
    fixme, fixme_msg,
    sstable::index::{CompactionIndexInterleaver, IndexRows},
};

pub(crate) mod data;
pub(crate) mod index;

pub(crate) static WRITING_SSTABLE_EXT: &str = "writing_sstable";
pub(crate) static SSTABLE_EXT: &str = "sstable";
pub(crate) static WRITING_SUMMARY_EXT: &str = "writing_summary";
pub(crate) static SUMMARY_EXT: &str = "summary";

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
        let inner = Arc::try_unwrap(self.inner).expect("memory leak!");
        inner.decommission().await
    }

    pub(crate) fn generation(&self) -> usize {
        self.uuid().generation
    }

    pub(crate) fn index_path(&self) -> PathBuf {
        self.inner.summary.path.join(INDEX)
    }

    pub(crate) fn uuid_string(&self) -> String {
        self.uuid().to_string()
    }
}

pub struct MemTable {
    data: HashMap<String, (Duration, Bytes)>,
    uuid: Uuid,
    write_log: Option<File>,
    // the size in-memory of the Bytes data only (does not include timestamps or keys)
    // FIXME: should include keys?
    memsize: usize,
    pub(crate) last_insert_at: Instant,
}

impl MemTable {
    // Just to allow mem::swap; this shouldn't be opened for writing.
    pub(crate) fn new_for_shutdown() -> Self {
        Self {
            data: Default::default(),
            uuid: Uuid::for_shutdown(),
            write_log: None,
            memsize: 0,
            last_insert_at: Instant::now(),
        }
    }

    pub(crate) async fn insert(
        &mut self,
        key: String,
        timestamp: Duration,
        data: Bytes,
    ) -> Result<(), Error> {
        self.insert_inner(key, timestamp, data, true).await
    }

    async fn insert_inner(
        &mut self,
        key: String,
        timestamp: Duration,
        data: Bytes,
        write_to_log: bool,
    ) -> Result<(), Error> {
        fixme("only accept data newer than the memtables started at timestamp?");
        fixme("also write this insert to an append-only log");

        if let Some((prev_ts, prev_data)) = self.data.get(&key) {
            if prev_ts > &timestamp {
                // keep the 'old' data because it has a newer timestamp
                // FIXME: do we need to write-log it?
                return Err(Error::NewerDataAlreadyHere);
            } else if prev_ts == &timestamp {
                todo!("wtf to do here");
            } else {
                self.memsize -= prev_data.len();
            }
        }

        if write_to_log {
            self.write_log(&key, &timestamp, &data).await?;
        }

        self.memsize += data.len();
        self.data.insert(key, (timestamp, data));
        self.last_insert_at = Instant::now();

        Ok(())
    }

    async fn write_log(
        &mut self,
        key: &str,
        timestamp: &Duration,
        data: &Bytes,
    ) -> Result<(), Error> {
        fixme("should these be delimited somehow?");
        // write-log format should be:
        // key len (u8)
        // key...
        // timestamp_secs (u64)
        // timestamp_nanos (u32)
        // data len (u64)
        // bytes
        // \n\r? or some other terminator?
        // self.write_log.
        let write_log = self
            .write_log
            .as_mut()
            .ok_or_else(|| Error::MemTableClosed)?;
        let mut buf = BufWriter::new(&mut *write_log);

        buf.write_u8(key.len() as u8).await.unwrap();
        buf.write_all(key.as_bytes()).await.unwrap();
        buf.write_u64(timestamp.as_secs()).await.unwrap();
        buf.write_u32(timestamp.subsec_nanos()).await.unwrap();
        buf.write_u64(data.len() as u64).await.unwrap();
        buf.write_all(data.as_ref()).await.unwrap();
        buf.write_all("\r\n".as_bytes()).await.unwrap();
        buf.flush().await.unwrap();

        // write_log.sync_all().await.unwrap();

        Ok(())
    }

    pub(crate) fn get(&self, key: &str) -> Option<&(Duration, Bytes)> {
        self.data.get(key)
    }

    pub(crate) async fn new(path: Option<&Path>) -> Result<Self, Error> {
        let uuid = Uuid::new_with_generation(0);

        let write_log = match path {
            Some(path) => {
                let table_name = path.file_name().unwrap().to_str().unwrap();
                let mut write_log_path = path.join(uuid.to_string());
                write_log_path.set_extension("write_log");

                let mut opts = tokio::fs::OpenOptions::new();
                opts.append(true).create(true);
                Some(opts.open(&write_log_path).await?)
            }
            None => None,
        };

        Ok(Self {
            data: Default::default(),
            uuid,
            write_log,
            memsize: 0,
            last_insert_at: Instant::now(),
        })
    }

    pub(crate) fn memsize(&self) -> usize {
        self.memsize
    }

    pub(crate) async fn close_write_log(&mut self) -> Result<(), Error> {
        if let Some(mut write_log) = self.write_log.take() {
            write_log.flush().await.unwrap();
            write_log.sync_all().await.unwrap();
            drop(write_log);
        }

        Ok(())
    }

    pub(crate) async fn recover_write_logs(
        write_logs: Vec<PathBuf>,
    ) -> Result<Vec<SSTable>, Error> {
        let mut sstables = vec![];

        for write_log in write_logs {
            let uuid = write_log.file_stem().unwrap().to_string_lossy().to_owned();

            let mut memtable = MemTable {
                data: Default::default(),
                uuid: uuid.parse()?,
                write_log: None,
                memsize: 0,
                last_insert_at: Instant::now(),
            };

            let file = tokio::fs::File::open(&write_log).await?;
            let mut reader = BufReader::new(file);

            loop {
                // could have stopped writing anywhere in a non-clean shutdown, so just process until EOF
                match Self::from_write_logs_loop_inner(&mut reader).await {
                    Ok((key, timestamp, data)) => {
                        memtable.insert_inner(key, timestamp, data, false).await?
                    }

                    Err(e) => {
                        if e.kind() == IoErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                }
            }

            if !memtable.is_empty() {
                let sstable = memtable
                    .to_persistent_storage(write_log.parent().unwrap())
                    .await?;

                sstables.push(sstable);
            } else {
                tokio::fs::remove_file(write_log).await?;
            }
        }

        Ok(sstables)
    }

    async fn from_write_logs_loop_inner(
        reader: &mut BufReader<File>,
    ) -> Result<(String, Duration, Bytes), std::io::Error> {
        // buf.write_u8(key.len() as u8).await.unwrap();
        let keylen = reader.read_u8().await?;
        let mut key = BytesMut::zeroed(keylen as usize);
        // buf.write_all(key.as_bytes()).await.unwrap();
        reader.read_exact(&mut key).await?;
        // buf.write_u64(timestamp.as_secs()).await.unwrap();
        let secs = reader.read_u64().await?;
        // buf.write_u32(timestamp.subsec_nanos()).await.unwrap();
        let nanos = reader.read_u32().await?;
        // buf.write_u64(data.len() as u64).await.unwrap();
        let len = reader.read_u64().await?;
        // buf.write_all(data.as_ref()).await.unwrap();
        let mut data = BytesMut::zeroed(len as usize);
        reader.read_exact(&mut data).await?;
        let trailer = vec![reader.read_u8().await?, reader.read_u8().await?];

        if trailer != "\r\n".as_bytes() {
            return Err(std::io::Error::new(
                IoErrorKind::UnexpectedEof,
                "Something is wrong with this file",
            ));
        }

        let key = String::from_utf8(key.to_vec()).expect("FIXME");
        let entry = (key, Duration::new(secs, nanos), data.into());
        Ok(entry)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
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
        sstable: &mut File,
        index_file: &mut File,
        offset: &mut usize,
    ) -> Result<(), Error> {
        let len = bytes.len();

        sstable.write_all_buf(&mut bytes).await.unwrap();
        index_file
            .write_all_buf(&mut index::serialize_row(&key, ts, *offset, len))
            .await
            .unwrap();

        *offset += len;

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
        tokio::fs::remove_file(index_path).await?;
        tokio::fs::remove_file(summary_path).await?;
        tokio::fs::remove_dir(self.summary.path).await?;

        Ok(())
    }
}

use rand::{
    distributions::{Alphanumeric, Standard},
    Rng,
};

use self::index::INDEX;

pub fn rand_bytes(n: usize) -> Bytes {
    rand::thread_rng().sample_iter(&Standard).take(n).collect()
}

pub fn rand_guid(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Summary {
    first_key: String,
    last_key: String,
    key_count: usize,
    timestamp: Duration,
    path: PathBuf,
    #[deprecated = "find a better way to interact and cache indexes"]
    #[serde(skip)]
    index: RwLock<Option<LoadedIndex>>,
}

struct SSTableWriter {
    uuid: Uuid,
}

#[derive(Clone, Debug)]
pub struct Uuid {
    timestamp: Duration,
    generation: usize,
    guid: String,
}

impl Uuid {
    fn new_with_generation(generation: usize) -> Self {
        let timestamp = timestamp();
        let guid = rand_guid(16);

        Self {
            timestamp,
            generation,
            guid,
        }
    }

    fn for_shutdown() -> Uuid {
        Self {
            timestamp: Duration::from_secs(0),
            generation: 0,
            guid: "SHUTDOWN".to_owned(),
        }
    }
}

impl FromStr for Uuid {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn from_str_inner(s: &str) -> Option<Uuid> {
            let mut segments = s.splitn(4, '-');

            let secs: u64 = segments.next()?.parse().ok()?;
            let nsecs: u32 = segments.next()?.parse().ok()?;
            let generation: usize = segments.next()?.parse().ok()?;
            let guid = segments.next()?.to_owned();

            Some(Uuid {
                timestamp: Duration::new(secs, nsecs),
                generation,
                guid,
            })
        }

        from_str_inner(s).ok_or(Error::InvalidUuid {
            input: s.to_owned(),
        })
    }
}

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{:0>10}-{}-{}",
            self.timestamp.as_secs(),
            self.timestamp.subsec_nanos(),
            self.generation,
            self.guid
        )
    }
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
        mut sorted_data: impl Stream<Item = (Duration, String, impl AsRef<[u8]>)> + Unpin,
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

        let mut first_key = None;
        let mut last_key = None;
        let mut key_count = 0;

        while let Some((timestamp, key, data)) = sorted_data.next().await {
            if first_key.is_none() {
                first_key = Some(key.clone());
            }

            // FIXME: better way to track this?
            last_key = Some(key.clone());

            key_count += 1;

            // #[cfg(fixme)]
            // let (ts, bytes) = self.data.get(&key).unwrap();
            SSTable::write_key_and_value(
                &key,
                &timestamp,
                data.as_ref(),
                &mut sstable_file,
                &mut index_file,
                &mut offset,
            )
            .await
            .unwrap();
        }

        index_file.flush().await.unwrap();
        index_file.sync_all().await.unwrap();

        let Some(first_key) = first_key else {
            return Err(Error::NoDataWasWritten);
        };

        fixme_msg("", "sample keys between first and last");

        let Some(last_key) = last_key else {
            return Err(Error::NoDataWasWritten);
        };

        let summary = Summary {
            first_key,
            last_key,
            key_count,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            path: enclosing_directory.clone(),
            index: RwLock::new(None),
        };

        let serialized_summary = bincode::serialize(&summary).unwrap();
        tokio::fs::write(&summary_path, serialized_summary)
            .await
            .unwrap();

        tokio::fs::rename(working_directory, &enclosing_directory).await?;

        // This will only happen when persisting a memtable
        let _ = tokio::fs::remove_file(enclosing_directory.with_extension("write_log")).await;

        Ok(SSTable::from_dir(enclosing_directory.as_path()).await)
    }
}

impl MemTable {
    // FIXME: should path come from self?
    pub(crate) async fn to_persistent_storage(&self, path: &Path) -> Result<SSTable, Error> {
        let mut keys = self.data.keys().cloned().collect::<Vec<String>>();
        keys.sort();

        let table_writer = SSTableWriter::from_uuid(self.uuid.clone());

        // This MemTable needs to remain available for live queries during this process,
        // so there are a few things copied here. `bytes.clone()` should be efficient however.
        let data_stream = keys.into_iter().map(|key| {
            let (ts, bytes) = self.data.get(&key).unwrap();
            (*ts, key, bytes.clone())
        });

        table_writer
            .write_data(path, tokio_stream::iter(data_stream))
            .await
    }
}

impl SSTable {
    pub async fn get(&mut self, key: &str) -> Result<(Duration, Bytes), Error> {
        if key < self.inner.summary.first_key.as_str() || key > self.inner.summary.last_key.as_str()
        {
            return Err(Error::KeyNotInRange);
        }
        tracing::debug!(
            "SSTABLE::get[{key} ({} .. {})]",
            self.inner.summary.first_key,
            self.inner.summary.last_key
        );

        fixme("where does this belong?");
        if self.inner.summary.index.read().await.is_none() {
            let mut write = self.inner.summary.index.write().await;
            let entries = self.inner.summary.load_index().await;
            *write = Some(entries);
        }

        let idx_read = self.inner.summary.index.read().await;
        let (timestamp, position, len) =
            idx_read
                .as_ref()
                .unwrap()
                .get(key)
                .ok_or_else(|| Error::DataNotFound {
                    key: key.to_owned(),
                })?;

        let now = Instant::now();

        let start = *position as usize;
        let end = (position + len) as usize;
        let data = &self.inner.mmap[start..end];

        // let mut ts_data = Bytes::copy_from_slice(data);
        // let data = ts_data.split_off(std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
        // let mut reader = BufReader::new(ts_data.as_ref());

        // let seconds = reader.read_u64().await.unwrap();
        // let nanoseconds = reader.read_u32().await.unwrap();

        // let timestamp = Duration::new(seconds, nanoseconds);

        // Ok((timestamp, data))
        Ok((*timestamp, Bytes::copy_from_slice(data)))
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
            index_iterators.push(stream);
        }

        let original_key_count: usize = tables.iter().map(|x| x.inner.summary.key_count).sum();

        let mut interleaver = CompactionIndexInterleaver::new_from_streams(index_iterators);

        let writer = SSTableWriter::from_generation(generation);

        let data_stream = interleaver.map(|(table_index, row)| {
            let timestamp = row.timestamp;
            let key = row.key;

            let start = row.start as usize;
            let end = start + row.len as usize;

            let data = &(tables.get(table_index).unwrap().inner.mmap)[start..end];

            (timestamp, key, data)
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
        let index_path = self.path.join(INDEX);
        let index_f = tokio::fs::File::open(&index_path).await.unwrap();

        // this wrapper speeds things up by like 20x
        let mut index = BufReader::new(index_f);

        let mut entries: HashMap<String, (Duration, u64, u64)> = HashMap::new();

        loop {
            let keylen = match index.read_u8().await {
                Ok(kl) => kl,
                Err(_e) => break,
            };
            let mut key = BytesMut::zeroed(keylen as usize);
            index.read_exact(&mut key).await.unwrap();
            let secs = index.read_u64().await.unwrap();
            let nanos = index.read_u32().await.unwrap();
            let position = index.read_u64().await.unwrap();
            let len = index.read_u64().await.unwrap();

            let duration = Duration::new(secs, nanos);

            entries.insert(
                String::from_utf8(key.to_vec()).unwrap(),
                (duration, position, len),
            );
        }

        entries
    }
}

type LoadedIndex = HashMap<String, (Duration, u64, u64)>;

pub struct KeySpace {
    root: PathBuf, // e.g. [/unified_reports/:hour_timestamp/]
    shard_id: u16,
    memtable: MemTable,
    // FIXME
}

pub fn timestamp() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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

        let (_, fetched_798) = sstable.get(&format!("key/for/0000798")).await.unwrap();
        let (_, fetched_124) = sstable.get(&format!("key/for/0000124")).await.unwrap();
        let (_, fetched_432) = sstable.get(&format!("key/for/0000432")).await.unwrap();

        assert_eq!(fetched_124, data_124.1);
        assert_eq!(fetched_432, data_432.1);
        assert_eq!(fetched_798, data_798.1);

        let time = Instant::now();

        for i in 0..1000 {
            println!("{i}");
            let (_, _fetched_124) = sstable.get(&format!("key/for/0000124")).await.unwrap();
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
}
