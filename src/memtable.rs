use std::{
    collections::HashMap,
    io::ErrorKind as IoErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use bytes::{Bytes, BytesMut};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    time::Instant,
};

use crate::{
    error::Error,
    fixme, fixme_msg,
    sstable::{SSTable, SSTableWriter, WRITE_LOG},
    util::{md5sum, Checksum, Uuid},
};

pub struct MemTable {
    pub(crate) data: HashMap<String, (Duration, Bytes)>,
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
        let checksum = md5sum(&data);
        self.insert_inner(key, timestamp, data, true, &checksum)
            .await
    }

    async fn insert_inner(
        &mut self,
        key: String,
        timestamp: Duration,
        data: Bytes,
        // this will be false if we are recovering a write log
        write_to_log: bool,
        checksum: &Checksum,
    ) -> Result<(), Error> {
        if let Some((prev_ts, prev_data)) = self.data.get(&key) {
            match prev_ts.cmp(&timestamp) {
                std::cmp::Ordering::Greater => {
                    // keep the 'old' data because it has a newer timestamp
                    // FIXME: do we need to write-log it?
                    return Err(Error::NewerDataAlreadyHere);
                }
                std::cmp::Ordering::Equal => {
                    if data == prev_data {
                        fixme_msg(println!("data eq"), "why is this happening?");
                        return Ok(());
                    } else {
                        return Err(Error::NewerDataAlreadyHere);
                    }
                }
                std::cmp::Ordering::Less => {
                    self.memsize -= prev_data.len();
                }
            }
        }

        if write_to_log {
            self.write_log(&key, &timestamp, &data, checksum).await?;
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
        checksum: &Checksum,
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
        let write_log = self.write_log.as_mut().ok_or(Error::MemTableClosed)?;
        let mut buf = BufWriter::new(&mut *write_log);

        buf.write_u8(key.len() as u8).await.unwrap();
        buf.write_all(key.as_bytes()).await.unwrap();
        buf.write_u64(timestamp.as_secs()).await.unwrap();
        buf.write_u32(timestamp.subsec_nanos()).await.unwrap();
        buf.write_u64(data.len() as u64).await.unwrap();
        buf.write_all(data.as_ref()).await.unwrap();
        buf.write_all(checksum.as_slice()).await.unwrap();
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
                write_log_path.set_extension(WRITE_LOG);

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
        path: &Path,
        write_logs: Vec<PathBuf>,
    ) -> Result<MemTable, Error> {
        // This combines found write logs into a new log.
        let mut new_table = MemTable::new(Some(path)).await?;

        // This could balloon recursively if there are any errors in here.
        for write_log in write_logs {
            let uuid = write_log.file_stem().unwrap().to_string_lossy();

            let file = tokio::fs::File::open(&write_log).await?;
            let mut reader = BufReader::new(file);

            loop {
                // could have stopped writing anywhere in a non-clean shutdown, so just process until EOF
                match Self::from_write_logs_loop_inner(&mut reader).await {
                    Ok((key, timestamp, data, checksum)) => {
                        match new_table
                            .insert_inner(key, timestamp, data, true, &checksum)
                            .await
                        {
                            Ok(_) | Err(Error::NewerDataAlreadyHere { .. }) => {}
                            Err(e) => {
                                tracing::error!(
                                    "Could not insert recovered log line into memtable; err={e:?}"
                                );
                            }
                        }
                    }

                    Err(Error::WriteLogEOF) => {
                        // Expected EOF.
                        break;
                    }

                    Err(Error::UnexpectedEOF { .. }) => {
                        tracing::error!("Unexpected EOF in write log recovery {uuid}");
                        break;
                    }

                    Err(e) => {
                        tracing::error!("Error in write log recovery {uuid}; err={e:?}");
                    }
                }
            }

            // let mv_target = write_log.with_extension("write_log_dropped");
            // tokio::fs::rename(write_log, mv_target).await?;
            tokio::fs::remove_file(write_log).await?;
        }

        Ok(new_table)
    }

    pub async fn dump_write_log_to_println(path: &Path) {
        let file = tokio::fs::File::open(path).await.unwrap();
        let mut reader = BufReader::new(file);

        loop {
            // could have stopped writing anywhere in a non-clean shutdown, so just process until EOF
            match Self::from_write_logs_loop_inner(&mut reader).await {
                Ok((key, timestamp, data, checksum)) => {
                    let data = String::from_utf8_lossy(&data);
                    println!("key={key} timestamp={timestamp:?} checksum={checksum:?}");
                    println!("{data}");
                }

                Err(Error::WriteLogEOF) => {
                    // Expected EOF.
                    break;
                }

                Err(Error::UnexpectedEOF { .. }) => {
                    tracing::error!("Unexpected EOF in write log recovery {path:?}");
                    break;
                }

                Err(e) => {
                    tracing::error!("Error in write log recovery {path:?}; err={e:?}");
                }
            }
        }
    }

    async fn from_write_logs_loop_inner(
        reader: &mut BufReader<File>,
    ) -> Result<(String, Duration, Bytes, Checksum), Error> {
        // buf.write_u8(key.len() as u8).await.unwrap();
        let keylen = match reader.read_u8().await {
            Ok(kl) => kl,
            Err(e) => {
                if e.kind() == IoErrorKind::UnexpectedEof {
                    return Err(Error::WriteLogEOF);
                } else {
                    Err(e)?
                }
            }
        };
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

        let mut checksum = [0u8; 16];
        reader.read_exact(&mut checksum).await?;

        let trailer = vec![reader.read_u8().await?, reader.read_u8().await?];

        if trailer != "\r\n".as_bytes() {
            return Err(Error::UnexpectedEOF { source: None });
        }

        let data: Bytes = data.into();

        let key = String::from_utf8(key.to_vec()).map_err(|_| Error::InvalidKey)?;
        let checksum2 = md5sum(&data);

        if checksum2 != checksum {
            // return Err(std::io::Error::new(
            //     IoErrorKind::InvalidData,
            //     Error::InvalidChecksum,
            // ));
            return Err(Error::InvalidChecksum {
                key,
                table_path: "<write_log>".into(),
            });
        }

        let entry = (key, Duration::new(secs, nanos), data, checksum);
        Ok(entry)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) async fn sync_write_log(&mut self) -> Result<(), Error> {
        // return Ok(()); // FIXME:
        if let Some(write_log) = self.write_log.as_mut() {
            write_log.flush().await.unwrap();
            write_log.sync_all().await.unwrap();
        }

        Ok(())
    }

    // FIXME: should path come from self?
    pub(crate) async fn to_persistent_storage(&self, path: &Path) -> Result<SSTable, Error> {
        let mut keys = self.data.keys().cloned().collect::<Vec<String>>();
        keys.sort();

        let table_writer = SSTableWriter::from_uuid(self.uuid.clone());

        // This MemTable needs to remain available for live queries during this process,
        // so there are a few things copied here. `bytes.clone()` should be efficient however.
        let data_stream = keys.into_iter().map(|key| {
            let (ts, bytes) = self.data.get(&key).unwrap();
            Ok((*ts, key, bytes.clone()))
        });

        table_writer
            .write_data(path, tokio_stream::iter(data_stream))
            .await
    }
}
