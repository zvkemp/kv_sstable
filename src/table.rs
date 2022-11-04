use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        oneshot, Semaphore,
    },
    task::JoinHandle,
    time::Instant,
};
use tokio_stream::Stream;
use tracing::{debug, info};

use crate::{
    error::Error,
    fixme, fixme_msg,
    sstable::{
        index::{CompactionIndexInterleaver, IndexRow, IndexRows},
        MemTable, SSTable,
    },
};

pub struct Table {
    path: PathBuf,
    live_memtable: MemTable,
    previous_memtable: Option<Arc<MemTable>>,
    sstables: GenerationalSSTables,
    memtable_size_limit: usize,
    sender: Option<Sender<Event>>,
    flush_semaphore: Arc<Semaphore>,
    is_shutting_down: bool,
    flush_handle: Option<JoinHandle<Result<(), Error>>>,
    compaction_task: Option<JoinHandle<Result<(), Error>>>,
    compaction_count: usize,
    writable: bool,
}

// Table should be able to provide access to data in the following order:
// - live memtable
// - previous memtable (current being persisted to disk)
// - sstables in order from newest to oldest

#[derive(Debug)]
pub enum Event {
    Get {
        key: String,
        reply_to: ReplyToGet,
    },
    Put {
        key: String,
        data: Bytes,
        // NOTE: this timestamp should be recorded in the _first_ node to accept the request
        // FIXME: we will need to carefully synchronize server time drift issues.
        // Or not - as long as the comparisons are consistent on all servers, LWW should work out?
        timestamp: Duration,
        reply_to: EmptyReplyTo,
    },
    MemTableFlushed(Result<SSTable, Error>),
    Tick, //
    Shutdown,
    ShutdownFinished,
    // compact a specific generation
    CompactGen {
        generation: usize,
        threshold: usize,
    },
    // compact the newest generation that meets the threshold.
    // NOTE: compacting older generations is unlikely to have a great effect in reclaimed space,
    // as most updates should be in the newer gens.
    Compact {
        threshold: usize,
    },
    CompactionFinished(SSTable, Vec<SSTable>),
    DropTable,
}

pub enum Reply {
    Data(Bytes),
    Empty,
}

pub type ReplyToGet = oneshot::Sender<Result<(Duration, Bytes), Error>>;
pub type EmptyReplyTo = oneshot::Sender<Result<(), Error>>;

impl Table {
    // each live table runs an event loop
    async fn run(mut self, mut receiver: Receiver<Event>) -> Result<(), Error> {
        fixme("listen for a shutdown");
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.tick().await?;
                }

                event = receiver.recv() => {
                    self.handle_event(event).await?
                }
            }
        }
    }

    pub fn spawn(mut self) -> Result<(Sender<Event>, JoinHandle<Result<(), Error>>), Error> {
        // FIXME: should these channels just go in new?
        let (sender, receiver) = tokio::sync::mpsc::channel(24);
        self.sender = Some(sender.clone());
        let handle = tokio::spawn(async move { self.run(receiver).await });

        Ok((sender, handle))
    }

    pub async fn readonly(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(Error::Other {
                description: "directory does not exist".into(),
            });
        }

        Self::new_inner(path, 0, false).await
    }

    // FIXME: pub makes no sense, but sometimes we want to expose writable
    pub async fn new_inner(
        path: impl AsRef<Path>,
        memtable_size_limit: usize,
        writable: bool,
    ) -> Result<Self, Error> {
        tokio::fs::create_dir_all(&path).await.unwrap();

        let mut entries = tokio::fs::read_dir(&path).await.unwrap();
        let mut write_logs: Vec<PathBuf> = vec![];
        let mut sstable_paths: Vec<PathBuf> = vec![];
        loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    if entry.path().is_file() {
                        match entry.path().extension().and_then(|x| x.to_str()) {
                            Some("write_log") => write_logs.push(entry.path().to_owned()),
                            _ => (),
                        }
                    } else if entry.path().is_dir() {
                        fixme("validate directory somehow?");

                        if entry.path().extension().and_then(|x| x.to_str()) == Some("writing") {
                            tracing::error!(
                                "Found a partially-written sstable at {:?}",
                                entry.path()
                            );
                            continue;
                        }

                        sstable_paths.push(entry.path().to_owned())
                    }
                }
                Ok(None) => break,
                Err(e) => Err(e)?,
            }
        }

        // todo!("FIXME");

        tracing::debug!("found write_logs: {:?}", write_logs);
        tracing::debug!("found sstables: {:?}", sstable_paths);

        fixme("if we just load logs into the 'live' memtable instead, we can avoid branching for readonly tables");
        let mut sstables = if writable {
            MemTable::recover_write_logs(write_logs).await?
        } else {
            vec![]
        };

        for path in sstable_paths {
            sstables.push(SSTable::from_dir(&path).await);
        }

        sstables.sort_by(|a, b| a.uuid_string().cmp(&b.uuid_string()));

        let mut generational_sstables = GenerationalSSTables::default();

        // partition the sstables into their generations.
        for sstable in sstables {
            debug!("loaded sstable {}", sstable.uuid());

            generational_sstables.push(sstable);
        }

        Ok(Table {
            path: path.as_ref().to_owned(),
            live_memtable: MemTable::new(writable.then_some(path.as_ref())).await?,
            previous_memtable: None,
            sstables: generational_sstables,
            memtable_size_limit,
            sender: None,
            flush_semaphore: Arc::new(Semaphore::new(1)),
            flush_handle: None,
            is_shutting_down: false,
            compaction_task: None,
            compaction_count: 0,
            writable,
        })
    }

    pub async fn new(path: impl AsRef<Path>, memtable_size_limit: usize) -> Result<Self, Error> {
        Self::new_inner(path, memtable_size_limit, true).await
    }

    pub async fn list_keys(&self) -> Pin<Box<dyn Stream<Item = (usize, IndexRow)>>> {
        let mut streams = vec![];

        for generation in self.sstables.inner.iter() {
            for sstable in generation.iter() {
                streams.push(IndexRows::from_path(sstable.index_path()).await)
            }
        }

        Box::pin(CompactionIndexInterleaver::new_from_streams(streams))
    }

    async fn tick(&mut self) -> Result<(), Error> {
        tracing::trace!("tick!");
        if self.should_flush() {
            self.flush_live_memtable(false).await?;
        }

        self.sender
            .as_ref()
            .unwrap()
            .send(Event::Compact { threshold: 5 })
            .await
            .unwrap();

        Ok(())
    }

    async fn flush_live_memtable(&mut self, for_shutdown: bool) -> Result<(), Error> {
        if self.live_memtable.is_empty() {
            return Ok(());
        }

        let permit = self.flush_semaphore.clone().acquire_owned().await.unwrap();
        tracing::warn!("flushing live_memtable");
        fixme("check previous state of flush_handle");
        // if self.previous_memtable.is_some() {
        //     // maybe just await previous shutdown?
        //     todo!("memtables not flushing quickly enough; need backpressure on writes");
        //     todo!("two of these may come in quick succession if during shutdown");
        // }

        let mut new_table = if self.is_shutting_down {
            MemTable::new_for_shutdown()
        } else {
            MemTable::new(Some(self.path.as_ref())).await?
        };

        std::mem::swap(&mut new_table, &mut self.live_memtable);
        new_table.close_write_log().await.unwrap();

        let prev_table = Arc::new(new_table);
        self.previous_memtable = Some(prev_table.clone());
        let sender = self.sender.as_ref().unwrap().clone();
        let path = self.path.clone();
        let handle = tokio::spawn(async move {
            let result = prev_table.to_persistent_storage(&path).await;
            sender.send(Event::MemTableFlushed(result)).await.unwrap();
            drop(permit);
            Ok(())
        });

        if for_shutdown {
            return handle.await.unwrap();
        } else {
            self.flush_handle = Some(handle);
        }

        Ok(())
    }

    async fn handle_event(&mut self, event: Option<Event>) -> Result<(), Error> {
        // Any errors that bubble up to this point will cause the event loop to exit.
        // Please send client-type errors back through the channel, or log them as needed.
        match event {
            Some(Event::Get { key, reply_to }) => self.handle_get(key, reply_to).await,
            Some(Event::Put {
                key,
                data,
                timestamp,
                reply_to,
            }) => self.handle_put(key, data, timestamp, reply_to).await,

            Some(Event::MemTableFlushed(inner_res)) => match inner_res {
                Ok(sstable) => {
                    info!("MemTableFlushed; new_summary={}", sstable.uuid());
                    self.sstables.push(sstable);
                    self.previous_memtable = None;
                    Ok(())
                }
                Err(e) => Err(e).unwrap(),
            },
            Some(Event::Tick) => {
                self.tick().await?;
                Ok(())
            }
            Some(Event::CompactGen {
                generation,
                threshold,
            }) => self.compact_gen(generation, threshold).await,
            Some(Event::Compact { threshold }) => self.compact(threshold).await,
            Some(Event::CompactionFinished(new_sstable, old_sstables)) => {
                tracing::info!("Compaction finished; new_sstable = {new_sstable:?}");
                self.finalize_compaction(new_sstable, old_sstables).await
            }
            Some(Event::Shutdown) => {
                self.shutdown().await?;
                self.sender
                    .as_ref()
                    .unwrap()
                    .send(Event::ShutdownFinished)
                    .await
                    .unwrap();

                Ok(())
            }
            Some(Event::DropTable) => {
                tracing::warn!("DROPPING TABLE AT {:?}", self.path);
                self.shutdown().await?;
                self.drop_data().await?;
                self.sender
                    .as_ref()
                    .unwrap()
                    .send(Event::ShutdownFinished)
                    .await
                    .unwrap();

                Ok(())
            }

            // This should be the last event in the channel.
            Some(Event::ShutdownFinished) => Err(Error::OkShutdown),
            None => Err(Error::Closed),
        }
    }

    async fn compact(&mut self, threshold: usize) -> Result<(), Error> {
        if let Some(candidate) = self
            .sstables
            .inner
            .iter()
            .enumerate()
            .find(|(_, v)| v.len() >= threshold)
            .map(|(i, _)| i)
        {
            self.compact_gen(candidate, threshold).await
        } else {
            Ok(())
        }
    }

    async fn compact_gen(&mut self, gen: usize, threshold: usize) -> Result<(), Error> {
        if self.is_shutting_down {
            tracing::error!("can't compact during shutdown :(");
            return Ok(());
        }

        self.compaction_count += 1;

        fixme("this may be contentious between generations");
        if self.compaction_task.is_some() {
            if self.compaction_task.as_ref().unwrap().is_finished() {
                self.compaction_task.take().unwrap().await.unwrap().unwrap();
            } else {
                tracing::error!("Compaction in progress, can't start another one");
                return Ok(());
            }
        }

        if self.sstables.length_of(gen) < threshold {
            return Ok(());
        }

        let sender = self.sender.clone().unwrap();
        let to_compact = self
            .sstables
            .get(gen)
            .iter()
            .map(|x| x._clone())
            .collect::<Vec<_>>();

        tracing::warn!("Spawning compaction for {:?} generation {gen}", self.path);
        let handle = tokio::spawn(async move {
            let now = Instant::now();
            match SSTable::compact(&to_compact).await {
                Ok(sstable) => sender
                    .send(Event::CompactionFinished(sstable, to_compact))
                    .await
                    .unwrap(),
                Err(e) => {
                    todo!()
                }
            }

            tracing::warn!("Compaction finished in {:?}", now.elapsed());

            Ok(())
        });

        self.compaction_task = Some(handle);

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        self.is_shutting_down = true;
        self.flush_live_memtable(true).await?;

        dbg!(&self.path);

        if let Some(task) = self.compaction_task.take() {
            tracing::warn!("awaiting live compaction...");
            // FIXME: clean up compaction
            task.await.unwrap().unwrap()
        }

        Ok(())
    }

    async fn drop_data(&mut self) -> Result<(), Error> {
        if let Some(task) = self.compaction_task.take() {
            fixme("allow compaction to be cancelled");
        }

        dbg!(self.sstables.decommission().await);

        tokio::fs::remove_dir_all(&self.path).await?;

        Ok(())
    }

    async fn handle_get(&mut self, key: String, reply_to: ReplyToGet) -> Result<(), Error> {
        fixme("is it always correct to short-circuit if data was found in the live table? NO");

        tracing::debug!("TABLE::get[{key}]");
        let response = if let Some((ts, bytes)) = self.live_memtable.get(&key).cloned() {
            Ok((ts, bytes))
        } else if let Some((ts, bytes)) = self
            .previous_memtable
            .as_ref()
            .and_then(|prev| prev.get(&key).cloned())
        {
            Ok((ts, bytes))
        } else {
            self.find_in_sstables(key.as_str()).await
        };

        if let Err(_e) = reply_to.send(response) {
            tracing::error!("error sending reply to oneshot channel");
        }

        Ok(())
    }

    async fn find_in_sstables(&mut self, key: &str) -> Result<(Duration, Bytes), Error> {
        self.sstables.find(key).await
    }

    async fn handle_put(
        &mut self,
        key: String,
        data: Bytes,
        timestamp: Duration,
        reply_to: EmptyReplyTo,
    ) -> Result<(), Error> {
        tracing::debug!("TABLE::put[{key}@{timestamp:?}]");
        // FIXME: check if reply_to is opened, or if we timed out client-side
        let res = if !self.writable {
            Err(Error::MemTableClosed)
        } else {
            if let Some(found_ts) = self.find_greater_timestamp_for_key(&key, &timestamp).await {
                tracing::warn!("Ignoring late-arriving key={key}@{timestamp:?}");
                return Ok(());
            }

            self.live_memtable.insert(key, timestamp, data).await
        };
        // println!("sending reply");
        let _ = reply_to.send(res);

        if self.should_flush() {
            self.sender
                .as_ref()
                .unwrap()
                .send(Event::Tick)
                .await
                .unwrap();
        }

        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.live_memtable.memsize() > 0
            && (self.live_memtable.memsize() >= self.memtable_size_limit
                || self.live_memtable.last_insert_at.elapsed() > Duration::from_secs(120))
    }

    async fn finalize_compaction(
        &mut self,
        new_sstable: SSTable,
        old_sstables: Vec<SSTable>,
    ) -> Result<(), Error> {
        self.sstables.push(new_sstable);

        for old_sstable in old_sstables {
            let removed = self.sstables.pop_front(old_sstable.generation());

            if !removed.ptr_eq(&old_sstable) {
                panic!("hmm - something didn't line up");
            }

            drop(removed);

            old_sstable.decommission().await.unwrap();
        }

        Ok(())
    }

    #[deprecated = "FIXME: explicitly test this logic"]
    pub(crate) async fn find_greater_timestamp_for_key(
        &mut self,
        key: &str,
        timestamp: &Duration,
    ) -> Option<Duration> {
        if self.previous_memtable.is_some() {
            if let Some((found_ts, _)) = self.previous_memtable.as_ref().unwrap().get(key) {
                if found_ts >= timestamp {
                    return Some(*found_ts);
                }
            }
        }

        self.sstables
            .find_greater_timestamp_for_key(key, timestamp)
            .await
    }

    // pub fn get(&self)
}

// Just a vec of vecs, sstables sorted into their generation ids. Newer generations are lower.
#[derive(Default)]
struct GenerationalSSTables {
    inner: Vec<Vec<SSTable>>,
}

impl GenerationalSSTables {
    fn push(&mut self, sstable: SSTable) {
        while self.inner.len() < sstable.generation() + 1 {
            self.inner.push(vec![]);
        }

        fixme("check ordering on these; it's error-prone to have to rev the sstable iterator");
        self.inner
            .get_mut(sstable.generation())
            .expect("while loop should have pushed a vec here")
            .push(sstable);
    }

    fn get(&self, arg: usize) -> &[SSTable] {
        self.inner.get(arg).map(|g| g.as_slice()).unwrap_or(&[])
    }

    fn pop_front(&mut self, generation: usize) -> SSTable {
        self.inner
            .get_mut(generation)
            .expect("FIXME: no unwrap")
            .remove(0)
    }

    async fn find(&mut self, key: &str) -> Result<(Duration, Bytes), Error> {
        fixme("this isn't quite correct - need to find all sstables where the key is in range, then compare the results by timestamp");
        // what assumptions can we make about generational residency?
        fixme("track key ranges (max/min) on the generations");
        for generation in self.inner.iter_mut() {
            for sstable in generation.iter_mut().rev() {
                match sstable.get(key).await {
                    Ok(res) => {
                        return Ok(res);
                    }
                    Err(Error::KeyNotInRange) | Err(Error::DataNotFound { .. }) => {}
                    Err(e) => Err(e)?,
                }
            }
        }
        // FIXME: at this point should we detach to a task?
        Err(Error::DataNotFound { key: key.into() })
    }

    fn length_of(&self, gen: usize) -> usize {
        self.inner.get(gen).map(|x| x.len()).unwrap_or_default()
    }

    // generation count
    fn generation_count(&self) -> usize {
        self.inner.len()
    }

    async fn decommission(&mut self) -> Result<(), Error> {
        for mut generation in self.inner.drain(0..) {
            for sstable in generation.drain(0..) {
                sstable.decommission().await;
            }
        }

        Ok(())
    }

    async fn find_greater_timestamp_for_key(
        &mut self,
        key: &str,
        timestamp: &Duration,
    ) -> Option<Duration> {
        // where to short-circuit?
        // We only need to look in tables where the _max_ timestamp >= timestamp.
        for generation in self.inner.iter_mut() {
            for sstable in generation.iter_mut().rev() {
                if let Some(res) = sstable.find_timestamp_greater_than(key, timestamp).await {
                    return Some(res);
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::Table;

    #[tokio::test]
    async fn test_table_1() {
        let table = Table::new("unified_reports/1000", 1000);
    }
}
