use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, Bytes, BytesMut};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    time::Instant,
};
use uuid::Uuid;

use crate::{error::Error, fixme, fixme_msg};

pub struct Topic {
    pub path: PathBuf,
}

impl Topic {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Topic { path: path.into() }
    }
}

/// represents an sstable that has already been persisted.
/// FIXME: should this be memory-mapped?
pub struct SSTable {
    pub summary: Summary,
    pub mmap: Mmap,
    // pub path: PathBuf,
    // pub uuid: Uuid,
}

pub struct MemTable {
    data: HashMap<String, (Duration, Bytes)>,
    uuid: String,
    write_log: File,
    memsize: usize,
}

impl MemTable {
    pub(crate) async fn insert(
        &mut self,
        key: String,
        timestamp: Duration,
        data: Bytes,
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

        // self.write_log.
        self.memsize += data.len();
        self.data.insert(key, (timestamp, data));

        Ok(())
    }

    pub(crate) fn get(&self, key: &str) -> Option<&(Duration, Bytes)> {
        self.data.get(key)
    }

    pub(crate) async fn new(path: &Path) -> Result<Self, Error> {
        let uuid = Uuid::new_v4().to_string();
        let table_name = path.file_name().unwrap().to_str().unwrap();
        let mut write_log_path = path.with_file_name(format!("{table_name}_{uuid}"));
        write_log_path.set_extension("write_log");

        let mut opts = tokio::fs::OpenOptions::new();
        opts.append(true).create(true);
        let write_log = opts.open(&write_log_path).await?;

        Ok(Self {
            data: Default::default(),
            uuid: uuid,
            write_log,
            memsize: 0,
        })
    }
}

impl SSTable {
    pub fn new(summary: Summary, mmap: Mmap) -> Self {
        // fixme: timestamp based v7?
        // let uuid = Uuid::new_v4();
        // let options = OptionOptions::new().create()
        // let f = tokio::fs::File::open(&path).await;
        SSTable { summary, mmap }
    }

    pub async fn from_file(path: &Path) -> Self {
        let summary_path = path.with_extension("summary");
        let summary_file = tokio::fs::read(&summary_path).await.unwrap();
        let summary: Summary = bincode::deserialize(&summary_file).unwrap();

        let mmap_path = path.with_extension("sstable");
        let file = std::fs::File::open(&mmap_path).unwrap();

        fixme("does this need to be on spawn blocking?");
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };

        SSTable::new(summary, mmap)
    }
}

pub fn rand_guid(n: usize) -> String {
    use rand::{distributions::Alphanumeric, Rng};

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
    index: Option<LoadedIndex>,
}

impl MemTable {
    pub async fn to_persistent_storage(mut self, path: &Path) -> Result<SSTable, String> {
        let mut keys = self.data.keys().cloned().collect::<Vec<String>>();
        keys.sort();

        let uuid = Uuid::new_v4().to_string();
        let pathbuf = path.join(&uuid).to_owned();
        let mut data_path = pathbuf.clone();
        let mut summary_path = pathbuf.clone();
        data_path.set_extension("writing_sstable");
        summary_path.set_extension("writing_summary");
        let mut options = OpenOptions::new();
        options.create(true);
        options.write(true);
        let mut file = options.open(&data_path).await.unwrap();

        let mut offset = 0usize;

        let mut index: Vec<(String, u64, u64)> = vec![];

        let first_key = keys.first().cloned().unwrap();
        let last_key = keys.last().cloned().unwrap();

        // write all data in order
        for key in keys {
            let (ts, bytes) = self.data.remove(&key).unwrap();
            let ts_secs: u64 = ts.as_secs();
            let ts_nanos: u32 = ts.subsec_nanos();
            let mut ts_buf = BytesMut::new();

            ts_buf.extend_from_slice(ts_secs.to_be_bytes().as_slice());
            ts_buf.extend_from_slice(ts_nanos.to_be_bytes().as_slice());
            let mut chain = ts_buf.chain(bytes);

            let len = chain.remaining();

            file.write_all_buf(&mut chain).await.unwrap();

            index.push((key, offset as u64, len as u64));
            offset += len;
        }

        let mut index_path = pathbuf.clone();
        index_path.set_extension("writing_index");

        // For now maybe just read the whole index?
        // Will maybe need a bloom filter to return quickly for non-resident keys.
        let mut index_file = options.open(&index_path).await.unwrap();
        for (key, start, len) in index {
            let keylen = key.len() as u16; // FIXME: enforce key len
            index_file.write_u16(keylen).await.unwrap();
            index_file.write_all(key.as_bytes()).await.unwrap();
            index_file.write_u64(start).await.unwrap();
            index_file.write_u64(len).await.unwrap();
        }

        index_file.flush().await.unwrap();
        index_file.sync_all().await.unwrap();

        file.flush().await.unwrap();
        file.sync_all().await.unwrap();

        let summary = Summary {
            first_key,
            last_key,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            path: pathbuf.clone(),
            index: None,
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
    pub async fn get(&mut self, key: &str) -> Result<(Duration, Bytes), String> {
        let now = Instant::now();

        // this wrapper speeds things up by like 20x
        fixme("where does this belong?");
        if self.summary.index.is_none() {
            let entries = self.summary.load_index().await;
            self.summary.index = Some(entries);
        }

        let (position, len) = self.summary.index.as_ref().unwrap().get(key).unwrap();

        let now = Instant::now();
        // let mut sstable = tokio::fs::File::open(&self.path.with_extension("sstable"))
        //     .await
        //     .unwrap();
        // sstable.seek(SeekFrom::Start(*position)).await.unwrap();
        // let mut data = BytesMut::zeroed((*len) as usize);
        // sstable.read_exact(&mut data).await.unwrap();

        let start = *position as usize;
        let end = (position + len) as usize;
        let data = &self.mmap[start..end];

        let mut ts_data = Bytes::copy_from_slice(data);
        let data = ts_data.split_off(std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
        let mut reader = BufReader::new(ts_data.as_ref());

        let seconds = reader.read_u64().await.unwrap();
        let nanoseconds = reader.read_u32().await.unwrap();

        let timestamp = Duration::new(seconds, nanoseconds);

        Ok((timestamp, data))
    }
}

impl Summary {
    #[deprecated = "not useful for large indexes - we don't want to load the whole thing into memory"]
    async fn load_index(&self) -> LoadedIndex {
        let mut index_path = self.path.to_owned();
        index_path.set_extension("index");
        let index_f = tokio::fs::File::open(&index_path).await.unwrap();

        // this wrapper speeds things up by like 20x
        let mut index = BufReader::new(index_f);

        let mut entries: HashMap<String, (u64, u64)> = HashMap::new();

        loop {
            let keylen = match index.read_u16().await {
                Ok(kl) => kl,
                Err(e) => break,
            };
            let mut key = BytesMut::zeroed(keylen as usize);
            index.read_exact(&mut key).await.unwrap();
            let position = index.read_u64().await.unwrap();
            let len = index.read_u64().await.unwrap();

            entries.insert(String::from_utf8(key.to_vec()).unwrap(), (position, len));
        }

        entries
    }
}

type LoadedIndex = HashMap<String, (u64, u64)>;

pub struct KeySpace {
    root: PathBuf, // e.g. [/unified_reports/:hour_timestamp/]
    shard_id: u16,
    memtable: MemTable,
    // FIXME
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use rand::distributions::Standard;
    use rand::Rng;
    use tempdir::TempDir;
    use tokio::time::Instant;

    pub fn rand_bytes(n: usize) -> Bytes {
        rand::thread_rng().sample_iter(&Standard).take(n).collect()
    }

    fn timestamp() -> Duration {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
    }

    #[tokio::test]
    async fn test_memtable_to_disk() {
        let tempdir = TempDir::new("test_memtable_to_disk").unwrap();

        let mut memtable = MemTable::new(tempdir.path()).await.unwrap();

        println!("tempdir={tempdir:?}");

        for i in 0..1000 {
            let len = (i + 10) * 10;
            let data = rand_bytes(len);
            let key = format!("key/for/{i}");
            memtable.insert(key, timestamp(), data).await.unwrap();
            println!("{i} {len}");
        }

        let data_798 = memtable.data.get(&format!("key/for/798")).unwrap().clone();
        let data_432 = memtable.data.get(&format!("key/for/432")).unwrap().clone();
        let data_124 = memtable.data.get(&format!("key/for/124")).unwrap().clone();

        println!("data generated");

        let mut sstable = memtable
            .to_persistent_storage(tempdir.path())
            .await
            .unwrap();

        // println!("summary={summary:?}");

        let fetched_798 = sstable.get(&format!("key/for/798")).await.unwrap();
        let fetched_124 = sstable.get(&format!("key/for/124")).await.unwrap();
        let fetched_432 = sstable.get(&format!("key/for/432")).await.unwrap();

        assert_eq!(fetched_124, data_124);
        assert_eq!(fetched_432, data_432);
        assert_eq!(fetched_798, data_798);

        let time = Instant::now();

        for i in 0..1000 {
            println!("{i}");
            let (ts, fetched_124) = sstable.get(&format!("key/for/124")).await.unwrap();
        }

        println!("1000 fetches in {:?}", time.elapsed());
        // let fetched_432 = summary.get(format!("key/for/432")).await.unwrap();
        // let fetched_124 = summary.get(format!("key/for/124")).await.unwrap();
    }

    #[test]
    fn test_be_bytes() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let seconds = now.as_secs();

        println!("{seconds}u64 = {:?}", seconds.to_be_bytes());
        println!("1u16 = {:?}", 260u16.to_be_bytes());
    }
}
