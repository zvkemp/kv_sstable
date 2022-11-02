use std::{
    cmp::min,
    collections::{HashMap, VecDeque},
    future::poll_fn,
    io::ErrorKind as IoErrorKind,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, Bytes, BytesMut};
use memmap2::Mmap;
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::RwLock,
    time::Instant,
};
use tokio_stream::Stream;

use crate::{error::Error, fixme, fixme_msg};

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
    uuid: String,
}

impl SSTable {
    // NOTE: Not deriving this because I don't want it to be public. The only clone should
    // be for doing compactions; anything else will cause a panic.
    pub(crate) fn _clone(&self) -> Self {
        SSTable {
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn uuid(&self) -> &str {
        self.inner.uuid.as_str()
    }

    pub(crate) fn ptr_eq(&self, other: &SSTable) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) async fn decommission(self) -> Result<(), Error> {
        let inner = Arc::try_unwrap(self.inner).unwrap();
        inner.decommission().await
    }
}

pub struct MemTable {
    data: HashMap<String, (Duration, Bytes)>,
    uuid: String,
    write_log: Option<File>,
    // the size in-memory of the Bytes data only (does not include timestamps or keys)
    // FIXME: should include keys?
    memsize: usize,
}

impl MemTable {
    // Just to allow mem::swap; this shouldn't be opened for writing.
    pub(crate) fn new_for_shutdown() -> Self {
        Self {
            data: Default::default(),
            uuid: "shutdown".into(),
            write_log: None,
            memsize: 0,
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

    pub(crate) fn new_uuid(ts: &Duration) -> String {
        format!(
            "{}-{:0>10}-{}",
            ts.as_secs(),
            ts.subsec_nanos(),
            rand_guid(16)
        )
    }

    pub(crate) async fn new(path: &Path) -> Result<Self, Error> {
        let table_name = path.file_name().unwrap().to_str().unwrap();
        let ts = timestamp();
        // keep this sortable by timestamp
        let uuid = Self::new_uuid(&ts);
        let mut write_log_path = path.join(&uuid);
        write_log_path.set_extension("write_log");

        let mut opts = tokio::fs::OpenOptions::new();
        opts.append(true).create(true);
        let write_log = opts.open(&write_log_path).await?;

        Ok(Self {
            data: Default::default(),
            uuid,
            write_log: Some(write_log),
            memsize: 0,
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
                uuid: uuid.into(),
                write_log: None,
                memsize: 0,
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
    pub fn new(summary: Summary, mmap: Mmap, uuid: String) -> Self {
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

    pub async fn from_file(path: &Path) -> Self {
        let summary_path = path.with_extension("summary");
        let summary_file = tokio::fs::read(dbg!(&summary_path)).await.unwrap();
        let summary: Summary = bincode::deserialize(&summary_file).unwrap();

        let uuid = summary_path
            .file_stem()
            .unwrap()
            .to_string_lossy()
            .to_owned()
            .into();

        let mmap_path = path.with_extension("sstable");
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
        let ts_secs: u64 = ts.as_secs();
        let ts_nanos: u32 = ts.subsec_nanos();
        let len = bytes.len();

        sstable.write_all_buf(&mut bytes).await.unwrap();
        let keylen = key.len() as u8; // FIXME: enforce key len

        // FIXME: buffer writes?
        index_file.write_u8(keylen).await.unwrap();
        index_file.write_all(key.as_bytes()).await.unwrap();
        index_file.write_u64(ts_secs).await.unwrap();
        index_file.write_u32(ts_nanos).await.unwrap();
        index_file.write_u64(*offset as u64).await.unwrap();
        index_file.write_u64(len as u64).await.unwrap();
        *offset += len;

        Ok(())
    }
}

impl SSTableInner {
    async fn decommission(self) -> Result<(), Error> {
        let summary_path = self.summary.path.with_extension("summary");
        let index_path = self.summary.path.with_extension("index");
        let sstable_path = self.summary.path.with_extension("sstable");

        // unmap before we delete the file!
        drop(self.mmap);

        tokio::fs::remove_file(sstable_path).await?;
        tokio::fs::remove_file(index_path).await?;
        tokio::fs::remove_file(summary_path).await?;

        Ok(())
    }
}

use rand::{
    distributions::{Alphanumeric, Standard},
    Rng,
};

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
    timestamp: Duration,
    path: PathBuf,
    #[deprecated = "find a better way to interact and cache indexes"]
    #[serde(skip)]
    index: RwLock<Option<LoadedIndex>>,
}

impl MemTable {
    // FIXME: should path come from self?
    pub async fn to_persistent_storage(&self, path: &Path) -> Result<SSTable, Error> {
        let mut keys = self.data.keys().cloned().collect::<Vec<String>>();
        keys.sort();

        let uuid = &self.uuid;
        let pathbuf = path.join(uuid).to_owned();
        let mut data_path = pathbuf.clone();
        let mut summary_path = pathbuf.clone();
        data_path.set_extension("writing_sstable");
        summary_path.set_extension("writing_summary");
        let mut options = OpenOptions::new();
        options.create(true);
        options.write(true);
        let mut file = options.open(&data_path).await.unwrap();

        let mut offset = 0usize;

        // let mut index: Vec<(String, Duration, u64, u64)> = vec![];

        let index_path = pathbuf.with_extension("writing_index");
        let mut index_file = options.open(&index_path).await.unwrap();

        let first_key = keys.first().cloned().unwrap();
        let last_key = keys.last().cloned().unwrap();

        // write all data in order
        for key in keys {
            let (ts, bytes) = self.data.get(&key).unwrap();
            println!("WRITING {key}");
            SSTable::write_key_and_value(&key, ts, bytes, &mut file, &mut index_file, &mut offset)
                .await
                .unwrap();
        }

        // For now maybe just read the whole index?
        // Will maybe need a bloom filter to return quickly for non-resident keys.
        // for (key, ts, start, len) in index {}

        index_file.flush().await.unwrap();
        index_file.sync_all().await.unwrap();

        tokio::fs::remove_file(pathbuf.with_extension("write_log"))
            .await
            .unwrap();

        file.flush().await.unwrap();
        file.sync_all().await.unwrap();

        let summary = Summary {
            first_key,
            last_key,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            path: pathbuf.clone(),
            index: RwLock::new(None),
        };

        let serialized_summary = bincode::serialize(&summary).unwrap();
        tokio::fs::write(&summary_path, serialized_summary)
            .await
            .unwrap();

        // FIXME: can we not clone so many pathbufs?
        let mut dest = data_path.clone();
        let mut index_dest = index_path.clone();
        index_dest.set_extension("index");
        dest.set_extension("sstable");

        tokio::fs::rename(&data_path, &dest).await.unwrap();
        tokio::fs::rename(&index_path, &index_dest).await.unwrap();
        tokio::fs::rename(&summary_path, summary_path.with_extension("summary"))
            .await
            .unwrap();

        Ok(SSTable::from_file(pathbuf.as_path()).await)
    }
}

impl SSTable {
    pub async fn get(&mut self, key: &str) -> Result<Bytes, Error> {
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
        let (duration, position, len) = idx_read.as_ref().unwrap().get(key).unwrap();

        let now = Instant::now();
        // let mut sstable = tokio::fs::File::open(&self.path.with_extension("sstable"))
        //     .await
        //     .unwrap();
        // sstable.seek(SeekFrom::Start(*position)).await.unwrap();
        // let mut data = BytesMut::zeroed((*len) as usize);
        // sstable.read_exact(&mut data).await.unwrap();

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
        Ok(Bytes::copy_from_slice(data))
    }

    pub(crate) async fn compact(tables: &[SSTable]) -> Result<SSTable, Error> {
        // FIXME: can we run some sort of progress marker to avoid duplicating long-running work, if the process has to restart?
        // Or is it good enough to just seek the source files to after the last complete entry in the compacted index?
        // For now, assume we just need to restart
        fixme("validate all tables belong to the same table path");
        if tables.is_empty() {
            return Err(Error::NoTablesInCompaction);
        }

        let uuid = MemTable::new_uuid(&timestamp());
        let new_path = tables
            .first()
            .unwrap()
            .inner
            .summary
            .path
            .with_file_name(&uuid);

        let index_paths = tables
            .iter()
            .map(|x| x.inner.summary.path.with_extension("index"))
            .collect::<Vec<_>>();

        let mut index_iterators = vec![];

        for index_path in index_paths {
            let reader = BufReader::new(tokio::fs::File::open(index_path).await.unwrap());
            let stream = IndexRows {
                reader,
                internal_buf: Vec::new(),
            };
            index_iterators.push(stream);
        }

        let mut interleaver = CompactionIndexInterleaver::new_from_streams(index_iterators);

        let mut sstable_path = new_path.with_extension("writing_sstable");
        let mut new_index_path = new_path.with_extension("writing_index");

        let mut options = OpenOptions::new();
        options.create(true);
        options.write(true);

        let mut key = fixme("0".to_owned());
        let mut first_key: Option<String> = None;

        let mut sstable_file = options.open(&sstable_path).await.unwrap();
        let mut index_file = options.open(&new_index_path).await.unwrap();
        let mut offset = 0;
        while let Some((table_index, row)) = interleaver.next_row().await {
            tracing::debug!("Compaction row: {row:?}");

            if first_key.is_none() {
                first_key = Some(row.key.clone());
            }
            key = row.key;
            let ts = row.timestamp;
            let start = row.start as usize;
            let end = start + row.len as usize;
            let data = &(tables.get(table_index).unwrap().inner.mmap)[start..end];

            SSTable::write_key_and_value(
                &key,
                &ts,
                data,
                &mut sstable_file,
                &mut index_file,
                &mut offset,
            )
            .await
            .unwrap();
        }

        index_file.flush().await.unwrap();
        index_file.sync_all().await.unwrap();

        sstable_file.flush().await.unwrap();
        sstable_file.sync_all().await.unwrap();

        let summary = Summary {
            first_key: first_key.unwrap(),
            last_key: key,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            path: new_path.with_extension(""),
            index: RwLock::new(None),
        };

        let summary_path = new_path.with_extension("writing_summary");
        let serialized_summary = bincode::serialize(&summary).unwrap();
        tokio::fs::write(&summary_path, serialized_summary)
            .await
            .unwrap();

        tokio::fs::rename(&sstable_path, sstable_path.with_extension("sstable"))
            .await
            .unwrap();
        tokio::fs::rename(&new_index_path, new_index_path.with_extension("index"))
            .await
            .unwrap();
        tokio::fs::rename(&summary_path, summary_path.with_extension("summary"))
            .await
            .unwrap();

        Ok(SSTable::from_file(new_path.as_path()).await)
    }
}

enum IndexInterleaverState {
    Active(Option<IndexRow>),
    Done,
}

// yields values from multiple indexes in alphabetical order, discarding all but the newest timestamp for duplicates.
pin_project! {
    struct CompactionIndexInterleaver {
        streams: Vec<IndexRows>,
        states: Vec<IndexInterleaverState>,
        buf: Vec<(usize, IndexRow)>,
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
    fn new_from_streams(streams: Vec<IndexRows>) -> Self {
        let states = (0..streams.len())
            .map(|_| IndexInterleaverState::Active(None))
            .collect();

        Self {
            streams,
            states,
            buf: vec![],
        }
    }

    async fn next_row(&mut self) -> Option<(usize, IndexRow)> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next_row(cx)).await
    }

    fn poll_next_row(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<(usize, IndexRow)>> {
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
                            Some(row) => {
                                tracing::debug!(
                                    "poll_next_row: {index} :: --> Poll::Ready(Some({row:?}))"
                                );
                                this.states[index] = IndexInterleaverState::Active(Some(
                                    row.expect("FIXME don't unwrap this"),
                                ));
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
                println!(">>>> DROPPING DUPLICATE {row:?}");
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

        Poll::Ready(Some((tail, index_row)))
    }
}

#[derive(Debug)]
struct IndexRow {
    key: String,
    timestamp: Duration,
    start: u64,
    len: u64,
}

pin_project! {
    struct IndexRows {
        #[pin]
        reader: BufReader<File>,
        internal_buf: Vec<u8>,
    }
}

impl IndexRows {
    async fn next_row(&mut self) -> Result<Option<IndexRow>, Error> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next_row(cx)).await
    }

    fn poll_next_row(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IndexRow>, Error>> {
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
                                dbg!(("extending", &len));
                                this.internal_buf.extend_from_slice(slice);
                                this.reader.as_mut().consume(len);

                                // no more bytes to fetch
                                if len == 0 {
                                    return Poll::Ready(Ok(None));
                                }
                            }

                            Poll::Ready(Err(e)) => {
                                todo!()
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
    type Item = Result<IndexRow, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_row(cx).map(Result::transpose)
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

    println!("read key");
    std::io::Read::read_exact(&mut reader, &mut key)?;
    println!("read secs");
    std::io::Read::read_exact(&mut reader, &mut secs)?;
    println!("read nanos");
    std::io::Read::read_exact(&mut reader, &mut nanos)?;
    println!("read start");
    std::io::Read::read_exact(&mut reader, &mut start)?;
    println!("read len");
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
        start: start,
        len: len,
    })
}

impl Summary {
    #[deprecated = "not useful for large indexes - we don't want to load the whole thing into memory"]
    async fn load_index(&self) -> LoadedIndex {
        let mut index_path = self.path.to_owned();
        index_path.set_extension("index");
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

    #[cfg(fixme)]
    #[tokio::test]
    async fn test_memtable_to_disk() {
        let tempdir = TempDir::new("test_memtable_to_disk").unwrap();

        let mut memtable = MemTable::new(tempdir.path()).await.unwrap();

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

        let fetched_798 = sstable.get(&format!("key/for/0000798")).await.unwrap();
        let fetched_124 = sstable.get(&format!("key/for/0000124")).await.unwrap();
        let fetched_432 = sstable.get(&format!("key/for/0000432")).await.unwrap();

        assert_eq!(fetched_124, data_124);
        assert_eq!(fetched_432, data_432);
        assert_eq!(fetched_798, data_798);

        let time = Instant::now();

        for i in 0..1000 {
            println!("{i}");
            let (ts, fetched_124) = sstable.get(&format!("key/for/0000124")).await.unwrap();
        }

        println!("1000 fetches in {:?}", time.elapsed());
        // let fetched_432 = summary.get(format!("key/for/432")).await.unwrap();
        // let fetched_124 = summary.get(format!("key/for/124")).await.unwrap();

        let index_path = sstable.summary.path.with_extension("index");
        let reader = BufReader::new(tokio::fs::File::open(index_path).await.unwrap());
        let mut stream = IndexRows { reader };

        while let Some(index_row) = stream.next().await {
            println!("{index_row:?}");
        }
    }

    #[test]
    fn test_u32_bytes() {
        dbg!(1073741824u64.to_be_bytes());
    }

    #[tokio::test]
    async fn test_this_index() {
        let path = PathBuf::from(
            "/Users/zach/data/table_0_test/1667358803-0770610000-MRyEyEZqSzebE8ir.index",
        );

        let file = tokio::fs::File::open(&path).await.unwrap();
        let reader = BufReader::new(file);
        let mut stream = IndexRows {
            reader,
            internal_buf: vec![],
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
