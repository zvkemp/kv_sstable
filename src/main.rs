use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
};

use bytes::{buf::Reader, Buf, Bytes, BytesMut};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

pub mod error;
pub mod index;
pub mod sstable;
pub mod table;

#[tokio::main]
async fn main() {
    let mut datafile = Datafile::new("/Users/zach/append_only_1.data").await;

    let d1 = Data {
        key: "data1".into(),
        value: Bytes::from(vec![0, 1, 2, 3, 4, 5]),
        timestamp: 0,
    };
    let d2 = Data {
        key: "data2".into(),
        value: Bytes::from(vec![0, 1, 2, 3, 4, 5, 6, 7]),
        timestamp: 0,
    };

    datafile.store(d1.clone()).await.unwrap();
    datafile.store(d2.clone()).await.unwrap();
    let d2g = datafile.get("data2".into()).await.unwrap();
    let d1g = datafile.get("data1".into()).await.unwrap();

    assert_eq!(d1, d1g);
    assert_eq!(d2, d2g);
}

#[derive(Debug)]
struct Datafile {
    path: PathBuf,
    file: tokio::fs::File,
    len: usize,
    keys: HashMap<String, (usize, usize)>,
}

// build it around time series data for ease of management.
#[derive(Debug, Clone, Eq, PartialEq)]
struct Data {
    key: String,
    value: Bytes,
    timestamp: usize,
}

type Result<T> = std::result::Result<T, String>;

// track number of bytes in a file.
// track number of bytes appended to the file; each key would point to current offset.
impl Datafile {
    async fn new(path: impl AsRef<Path>) -> Datafile {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path.as_ref())
            .await
            .unwrap();

        let len = file.metadata().await.unwrap().len() as usize;
        Datafile {
            path: path.as_ref().to_owned(),
            file,
            len,
            keys: Default::default(),
        }
    }

    async fn store(&mut self, data: Data) -> Result<()> {
        // fixme; what sort of buffers?
        self.file.write_all(data.value.as_ref()).await.unwrap();
        let offset = self.len;
        let len = data.value.len();
        self.file.flush().await.unwrap();
        self.track_key(data.key, offset, len).await;
        self.len += len;
        Ok(())
    }

    async fn track_key(&mut self, key: String, offset: usize, len: usize) {
        println!("track_key {key} {offset} {len}");
        self.keys.insert(key, (offset, len));
    }

    async fn get(&mut self, key: String) -> Result<Data> {
        let (offset, len) = dbg!(&self.keys).get(&key).unwrap();
        let mut file = tokio::fs::File::open(&self.path).await.unwrap();

        println!("seeking to {offset}");
        file.seek(SeekFrom::Start(*offset as u64)).await.unwrap();
        let mut bytes = BytesMut::zeroed(*len);
        // let mut reader = bytes.reader();
        println!("buf len {}", (&mut bytes).len());
        file.read_exact(&mut bytes).await.unwrap();

        dbg!(&bytes);
        Ok(Data {
            key,
            value: bytes.into(),
            timestamp: 0,
        })
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[deprecated = "FIXME"]
pub fn fixme<T>(arg: T) -> T {
    arg
}

#[deprecated = "FIXME"]
pub fn fixme_msg<T>(arg: T, _msg: &'static str) -> T {
    arg
}
