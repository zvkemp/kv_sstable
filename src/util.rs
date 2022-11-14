use bytes::Bytes;
use md5::{Digest, Md5};
use rand::{
    distributions::{Alphanumeric, Standard},
    Rng,
};
use tokio_stream::Stream;

use crate::{
    error::Error,
    sstable::index::{CompactionIndexInterleaver, DynIndexStream, IndexRow, IndexRows},
};
use std::{
    path::Path,
    pin::Pin,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub async fn list_keys_in_indexes(
    index_paths: impl Iterator<Item = &Path>,
) -> Pin<Box<dyn Stream<Item = Result<(usize, IndexRow), Error>>>> {
    let mut streams = vec![];

    for path in index_paths {
        streams.push(Box::new(IndexRows::from_path(path).await) as DynIndexStream);
    }

    Box::pin(CompactionIndexInterleaver::new_from_streams(streams))
}

pub type Checksum = [u8; 16];

pub(crate) fn md5sum(data: impl AsRef<[u8]>) -> Checksum {
    // return fixme_msg([0; 16], "the real md5 makes things very very very slow");
    let mut hasher = Md5::new();
    hasher.update(data);
    let hash = hasher.finalize();
    hash.as_slice().try_into().unwrap()
}

pub fn timestamp() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
}

pub fn rand_guid(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

pub fn rand_bytes(n: usize) -> Bytes {
    rand::thread_rng().sample_iter(&Standard).take(n).collect()
}

#[derive(Clone, Debug)]
pub struct Uuid {
    timestamp: Duration,
    generation: usize,
    guid: String,
}

impl Uuid {
    pub fn new_with_generation(generation: usize) -> Self {
        let timestamp = timestamp();
        let guid = rand_guid(16);

        Self {
            timestamp,
            generation,
            guid,
        }
    }

    pub(crate) fn for_shutdown() -> Uuid {
        Self {
            timestamp: Duration::from_secs(0),
            generation: 0,
            guid: "SHUTDOWN".to_owned(),
        }
    }

    pub(crate) fn generation(&self) -> usize {
        self.generation
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

pub fn murmur2(data: &[u8]) -> u32 {
    let length = data.len();
    let seed: u32 = 0x9747b28c;

    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    let m = 0x5bd1e995;
    let r = 24;

    // Initialize the hash to a random value
    let mut h: u32 = seed ^ length as u32;
    let length4 = length / 4;

    for i in 0..length4 {
        let i4 = i * 4;
        let mut k: u32 = (data[i4] as u32 & 0xff)
            + ((data[i4 + 1] as u32 & 0xff) << 8)
            + ((data[i4 + 2] as u32 & 0xff) << 16)
            + ((data[i4 + 3] as u32 & 0xff) << 24);
        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
    }

    // Handle the last few bytes of the input array
    match length % 4 {
        3 => {
            h ^= (data[(length & !3) + 2] as u32 & 0xff) << 16;
        }
        2 => {
            h ^= (data[(length & !3) + 1] as u32 & 0xff) << 8;
        }
        1 => {
            h ^= data[length & !3] as u32 & 0xff;
            h = h.wrapping_mul(m);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(m);
    h ^= h >> 15;

    h as u32
}
