use std::{
    cmp::Ordering,
    fmt::Debug,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
    vec::Drain,
};

use bytes::Bytes;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender},
        oneshot, OwnedSemaphorePermit, Semaphore,
    },
    task::JoinHandle,
    time::Instant,
};
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, info};

use crate::{
    error::Error,
    fixme, fixme_msg,
    memtable::MemTable,
    sstable::{
        index::{CompactionIndexInterleaver, DynIndexStream, IndexRow, IndexRows},
        SSTable,
    },
};

pub struct Table {
    path: PathBuf,
    live_memtable: MemTable,
    previous_memtable: Option<Arc<MemTable>>,
    sstables: GenerationalSSTables,
    memtable_size_limit: usize,
    // NOTE: Only use this in detached tasks, which you must not await in the main event loop.
    client_sender: Option<Sender<Event>>,
    internal_sender: Option<UnboundedSender<Event>>,
    // flush_semaphore: Arc<Semaphore>,
    is_shutting_down: bool,
    flush_handle: Option<JoinHandle<Result<SSTable, Error>>>,
    compaction_task: Option<JoinHandle<Result<(), Error>>>,
    compaction_count: usize,
    writable: bool,
    live_streams: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("path", &self.path)
            .field("memtable_size_limit", &self.memtable_size_limit)
            .field("is_shutting_down", &self.is_shutting_down)
            .field("compaction_count", &self.compaction_count)
            .field("writable", &self.writable)
            .field("live_streams", &self.live_streams)
            .field("", &"...")
            .finish()
    }
}

// Table should be able to provide access to data in the following order:
// - live memtable
// - previous memtable (current being persisted to disk)
// - sstables in order from newest to oldest

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
    MemTableFlushed,
    Tick(&'static str), //
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
    PrintSelf,
    ListKeys {
        reply_to: ReplyToListKeys,
        newer_than: Option<Duration>,
    },
    StreamAll {
        reply_to: Sender<StreamData>,
        newer_than: Option<Duration>,
    },
}

pub enum Reply {
    Data(Bytes),
    Empty,
}

pub enum StreamData {
    Data(String, Duration, Bytes),
    Done,
}

impl std::fmt::Debug for StreamData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Data(key, timestamp, _data) => f
                .debug_tuple("Data")
                .field(key)
                .field(timestamp)
                .field(&"Bytes<...>")
                .finish(),
            Self::Done => write!(f, "Done"),
        }
    }
}

impl Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get { key, reply_to } => f
                .debug_struct("Get")
                .field("key", key)
                .field("reply_to", reply_to)
                .finish(),
            Self::Put {
                key,
                data,
                timestamp,
                reply_to,
                ..
            } => f
                .debug_struct("Put")
                .field("key", key)
                .field("data", data)
                .field("timestamp", timestamp)
                .field("reply_to", reply_to)
                .finish(),
            Self::MemTableFlushed => f.debug_tuple("MemTableFlushed").finish(),
            Self::Tick(_) => write!(f, "Tick"),
            Self::Shutdown => write!(f, "Shutdown"),
            Self::ShutdownFinished => write!(f, "ShutdownFinished"),
            Self::CompactGen {
                generation,
                threshold,
            } => f
                .debug_struct("CompactGen")
                .field("generation", generation)
                .field("threshold", threshold)
                .finish(),
            Self::Compact { threshold } => f
                .debug_struct("Compact")
                .field("threshold", threshold)
                .finish(),
            Self::CompactionFinished(arg0, arg1) => f
                .debug_tuple("CompactionFinished")
                .field(arg0)
                .field(arg1)
                .finish(),
            Self::DropTable => write!(f, "DropTable"),
            Self::PrintSelf => write!(f, "PrintSelf"),
            Self::ListKeys {
                reply_to,
                newer_than,
            } => f
                .debug_struct("ListKeys")
                .field("reply_to", &"Sender<...>")
                .field("newer_than", newer_than)
                .finish(),
            Self::StreamAll {
                reply_to,
                newer_than,
            } => f
                .debug_struct("StreamAll")
                .field("reply_to", &"Sender<...>")
                .field("newer_than", newer_than)
                .finish(),
        }
    }
}

pub type ReplyToGet = oneshot::Sender<Result<(Duration, Bytes), Error>>;
pub type EmptyReplyTo = oneshot::Sender<Result<(), Error>>;
pub type ReplyToListKeys = oneshot::Sender<Result<ListKeysStream, Error>>;

pub type ListKeysStream = Pin<Box<dyn Stream<Item = Result<IndexRow, Error>> + Send>>;

pub type TableSender = Sender<Event>;
pub type TableHandle = JoinHandle<Result<(), Error>>;
pub type TableSpawn = (TableSender, TableHandle);

#[derive(Debug, Copy, Clone)]
pub enum MissingTableBehavior {
    Create,
    Error,
}

#[derive(Copy, Debug, Clone)]
pub struct TableOptions {
    pub missing_table_behavior: MissingTableBehavior,
    pub writable: bool,
    pub memtable_size_limit: usize,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            missing_table_behavior: MissingTableBehavior::Create,
            writable: true,
            memtable_size_limit: 8 * 1024 * 1024,
        }
    }
}

impl Table {
    // each live table runs an event loop
    async fn run(
        mut self,
        mut client_receiver: Receiver<Event>,
        mut internal_receiver: UnboundedReceiver<Event>,
    ) -> Result<(), Error> {
        fixme("listen for a shutdown");
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.tick("select").await?;
                }

                event = client_receiver.recv() => {
                    self.handle_event(event).await?
                }

                event = internal_receiver.recv() => {
                    self.handle_event(event).await?
                }
            }
        }
    }

    pub fn spawn(mut self) -> Result<TableSpawn, Error> {
        // FIXME: should these channels just go in new?
        let (client_sender, client_receiver) = tokio::sync::mpsc::channel(24);
        let (internal_sender, internal_receiver) = tokio::sync::mpsc::unbounded_channel();

        self.client_sender = Some(client_sender.clone());
        self.internal_sender = Some(internal_sender);
        let handle =
            tokio::spawn(async move { self.run(client_receiver, internal_receiver).await });

        Ok((client_sender, handle))
    }

    #[deprecated = "what is the reason for this?"]
    pub async fn readonly(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        if !path.is_dir() {
            return Err(Error::Other {
                description: "directory does not exist".into(),
            });
        }

        Self::new_inner(
            path,
            TableOptions {
                missing_table_behavior: MissingTableBehavior::Error,
                writable: false,
                memtable_size_limit: 0,
            },
        )
        .await
    }

    // FIXME: pub makes no sense, but sometimes we want to expose writable
    pub async fn new_inner(path: impl AsRef<Path>, options: TableOptions) -> Result<Self, Error> {
        if matches!(options.missing_table_behavior, MissingTableBehavior::Error) {
            if !path.as_ref().is_dir() {
                return Err(Error::TableDoesNotExist);
            }
        }

        tokio::fs::create_dir_all(&path).await?;

        let mut entries = tokio::fs::read_dir(&path).await?;
        let mut write_logs: Vec<PathBuf> = vec![];
        let mut sstable_paths: Vec<PathBuf> = vec![];
        loop {
            match entries.next_entry().await {
                Ok(Some(entry)) => {
                    if entry.path().is_file() {
                        if let Some("write_log") = entry.path().extension().and_then(|x| x.to_str())
                        {
                            write_logs.push(entry.path().to_owned());
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

        let mut recovered_memtable =
            MemTable::recover_write_logs(path.as_ref(), write_logs).await?;
        let mut sstables = vec![];

        for path in sstable_paths {
            fixme("we might have 'dangling' indexes and summaries, which should stick around until streams have finished.");
            if path.join("sstable").is_file() {
                sstables.push(SSTable::from_dir(&path).await);
            }
        }

        sstables.sort_by_key(|a| a.uuid_string());

        let mut generational_sstables = GenerationalSSTables::default();

        // partition the sstables into their generations.
        for sstable in sstables {
            debug!("loaded sstable {}", sstable.uuid());

            generational_sstables.push(sstable);
        }

        if !options.writable {
            recovered_memtable.close_write_log().await?;
        }

        Ok(Table {
            path: path.as_ref().to_owned(),
            live_memtable: recovered_memtable,
            previous_memtable: None,
            sstables: generational_sstables,
            memtable_size_limit: options.memtable_size_limit,
            client_sender: None,
            internal_sender: None,
            // flush_semaphore: Arc::new(Semaphore::new(1)),
            flush_handle: None,
            is_shutting_down: false,
            compaction_task: None,
            compaction_count: 0,
            writable: options.writable,
            live_streams: vec![],
        })
    }

    pub async fn new(path: impl AsRef<Path>, options: TableOptions) -> Result<Self, Error> {
        Self::new_inner(path, options).await
    }

    /// This uses the Compaction index interleaver to list keys in alphabetical order,
    /// omitting duplicates (newest timestamp wins)
    pub async fn list_keys(&self, newer_than: Option<Duration>) -> ListKeysStream {
        let newer_than = newer_than.unwrap_or_default();

        let mut streams = vec![];
        // This is not ideal, but we should be ignoring everything but the keys.
        fn memtable_to_stream(memtable: &MemTable) -> DynIndexStream {
            let mut rows = vec![];
            memtable.data.iter().for_each(|(key, (ts, _value))| {
                rows.push(IndexRow {
                    key: key.clone(),
                    timestamp: *ts,
                    start: 0,
                    len: 0,
                })
            });

            rows.sort_by(|a, b| a.key.cmp(&b.key));

            Box::new(tokio_stream::iter(rows.into_iter().map(Ok)))
        }

        streams.push(memtable_to_stream(&self.live_memtable));
        if let Some(previous_memtable) = self.previous_memtable.as_ref() {
            streams.push(memtable_to_stream(previous_memtable));
        }

        for generation in self.sstables.inner.iter() {
            for sstable in generation.inner.iter() {
                if sstable.max_timestamp() >= &newer_than {
                    streams.push(Box::new(IndexRows::from_path(sstable.index_path()).await)
                        as DynIndexStream);
                }
            }
        }

        let interleaver = CompactionIndexInterleaver::new_from_streams(streams);

        Box::pin(interleaver.filter_map(move |res| match res {
            Ok((_, index_row)) => {
                if index_row.timestamp > newer_than {
                    Some(Ok(index_row))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        }))
    }

    pub fn list_component_tables(&self) {
        fixme("list live keys");

        println!("Table {:?}", self.path);
        for (g, generation) in self.sstables.inner.iter().enumerate() {
            println!("Generation {g}\n=============================");
            for (t, table) in generation.inner.iter().enumerate() {
                println!("{t} {}", table.uuid());
                println!("  {}..{}", table.first_key(), table.last_key());
                println!("  {:?}..{:?}", table.min_timestamp(), table.max_timestamp());
            }
        }
    }

    async fn tick(&mut self, src: &'static str) -> Result<(), Error> {
        tracing::debug!("tick! {src}");
        if self.should_flush() {
            self.flush_live_memtable(false).await?;
        } else {
            self.sync_write_log().await?;
        }

        self.live_streams.retain(|t| !t.is_finished());

        self.compact(5)?;

        fixme("clean up dangling indexes if streams is empty");

        tracing::debug!("tick done {src}");
        Ok(())
    }

    async fn flush_live_memtable(&mut self, for_shutdown: bool) -> Result<(), Error> {
        if self.live_memtable.is_empty() {
            return Ok(());
        }

        match self.flush_handle.as_mut() {
            Some(handle) => {
                if handle.is_finished() || for_shutdown {
                    self.finalize_flush().await;
                } else {
                    tracing::warn!("Flush task still active; can't flush again right now");
                    return Ok(());
                }
            }

            None => {}
        }

        // if self.flush_semaphore.available_permits() < 1 {
        //     tracing::warn!("No permits available for memtale flush");
        //     return Ok(());
        // }

        // FIXME: this can deadlock if we're not careful
        // No amount of timeout will fix it.
        // let permit = self.flush_semaphore.clone().acquire_owned().await.unwrap();

        tracing::warn!("flushing live_memtable");
        fixme("check previous state of flush_handle");
        // if self.previous_memtable.is_some() {
        //     // maybe just await previous shutdown?
        //     todo!("memtables not flushing quickly enough; need backpressure on writes");
        //     todo!("two of these may come in quick succession if during shutdown");
        // }
        // How is this possible
        if self.previous_memtable.is_some() {
            panic!("UH OH")
        }

        let mut new_table = if self.is_shutting_down {
            MemTable::new_for_shutdown()
        } else {
            MemTable::new(Some(self.path.as_ref())).await?
        };

        std::mem::swap(&mut new_table, &mut self.live_memtable);
        new_table.close_write_log().await.unwrap();

        let prev_table = Arc::new(new_table);
        self.previous_memtable = Some(prev_table.clone());
        let sender = self.internal_sender.clone().unwrap();
        let path = self.path.clone();

        let handle = tokio::spawn(async move {
            let result = prev_table.to_persistent_storage(&path).await;
            sender.send(Event::MemTableFlushed).unwrap();

            fixme("is the permit still necessary?");
            result.map(|sstable| sstable)
        });

        if for_shutdown {
            let _sstable = handle.await.unwrap().unwrap();
            return Ok(());
        } else {
            self.flush_handle = Some(handle);
        }

        Ok(())
    }

    async fn handle_event(&mut self, event: Option<Event>) -> Result<(), Error> {
        // Any errors that bubble up to this point will cause the event loop to exit.
        // Please send client-type errors back through the channel, or log them as needed.
        fixme("passing reply_to to the handlers is error prone; we should be allowed to `return` from those methods");
        fixme("without dropping the sender");

        // NOTE: IMPORTANT: Do not attempt to send an event into self.client_sender from this task - only use it in detached tasks.
        // Sending events into a full channel will deadlock.
        match event {
            Some(Event::Get { key, reply_to }) => self.handle_get(key, reply_to).await,
            Some(Event::Put {
                key,
                data,
                timestamp,
                reply_to,
            }) => self.handle_put(key, data, timestamp, reply_to).await,

            Some(Event::MemTableFlushed) => {
                self.finalize_flush().await;
                Ok(())
            }
            Some(Event::Tick(src)) => {
                self.tick(src).await?;
                Ok(())
            }
            Some(Event::CompactGen {
                generation,
                threshold,
            }) => self.compact_gen(generation, threshold),
            Some(Event::Compact { threshold }) => self.compact(threshold),
            Some(Event::CompactionFinished(new_sstable, old_sstables)) => {
                tracing::info!("Compaction finished; new_sstable = {new_sstable:?}");
                if let Err(e) = self.finalize_compaction(new_sstable, old_sstables).await {
                    tracing::error!("Error finalizing compaction: {e:?}");
                }

                Ok(())
            }
            Some(Event::Shutdown) => {
                self.shutdown().await?;
                self.internal_sender
                    .as_ref()
                    .unwrap()
                    .send(Event::ShutdownFinished)
                    .unwrap();

                Ok(())
            }
            Some(Event::DropTable) => {
                tracing::warn!("DROPPING TABLE AT {:?}", self.path);
                self.shutdown().await?;
                self.drop_data().await?;

                let sender = self.client_sender.clone();

                tokio::spawn(async move {
                    sender.unwrap().send(Event::ShutdownFinished).await.unwrap();
                });

                Ok(())
            }

            // This should be the last event in the channel.
            Some(Event::ShutdownFinished) => Err(Error::OkShutdown),
            Some(Event::PrintSelf) => {
                self.list_component_tables();
                Ok(())
            }
            Some(Event::ListKeys {
                reply_to,
                newer_than,
            }) => {
                todo!("maybe remove me");
                let stream = self.list_keys(newer_than).await;
                let _ = reply_to.send(Ok(stream));

                Ok(())
            }
            Some(Event::StreamAll {
                reply_to,
                newer_than,
            }) => {
                // This basically snapshots the table at this point in time, then iterates over the keys, returning the data
                // This should return the newest data for any particular key, but not necessary any keys that were new after the stream started.
                // Run it a second time to get the newer keys!
                let mut key_stream = self.list_keys(newer_than).await;
                let sender = self.client_sender.clone().unwrap();
                let self_debug = format!("{:?}", self);
                let task = tokio::spawn(async move {
                    while let Some(index_row_res) = key_stream.next().await {
                        match index_row_res {
                            Ok(index_row) => {
                                let (tx, rx) = oneshot::channel();
                                let _ = sender
                                    .send(Event::Get {
                                        key: index_row.key.clone(),
                                        reply_to: tx,
                                    })
                                    .await;

                                // should we just clone the reply_to Sender?
                                fixme("remove this expect(); if the channel is closed we should just drop the stream");
                                match rx.await {
                                    Ok(Ok((duration, bytes))) => {
                                        tracing::debug!(
                                            "Event::StreamAll producing {}",
                                            index_row.key
                                        );
                                        match reply_to
                                            .send(StreamData::Data(index_row.key, duration, bytes))
                                            .await
                                        {
                                            Ok(_) => {}
                                            Err(_) => {
                                                tracing::error!("StreamData stream closed");
                                                break;
                                            }
                                        }
                                    }

                                    Ok(Err(e)) => {
                                        tracing::warn!("Error in stream: {:?}", e);
                                    }
                                    Err(e) => {
                                        todo!(
                                            "RecvError waiting for response from table {e:?}; {:?}",
                                            self_debug
                                        )
                                    }
                                }
                            }
                            Err(_) => todo!(),
                        }
                    }

                    // If the stream ends without this terminator, then whoops. We probably had an error internally.
                    // Ignore errors here; if the channel is closed, then they won't get this value anyway, and we're
                    // exiting on the next line.
                    let _ = reply_to.send(StreamData::Done).await;

                    drop(reply_to)
                });

                self.live_streams.push(task);

                Ok(())
            }
            None => Err(Error::Closed),
        }
    }

    fn compact(&mut self, threshold: usize) -> Result<(), Error> {
        if let Some(candidate) = self
            .sstables
            .inner
            .iter()
            .enumerate()
            .find(|(_, v)| v.len() >= threshold)
            .map(|(i, _)| i)
        {
            self.compact_gen(candidate, threshold)
        } else {
            Ok(())
        }
    }

    fn compact_gen(&mut self, gen: usize, threshold: usize) -> Result<(), Error> {
        if self.is_shutting_down {
            tracing::error!("can't compact during shutdown :(");
            return Ok(());
        }

        self.compaction_count += 1;

        fixme("this may be contentious between generations");
        if self.compaction_task.is_some() {
            tracing::error!("Compaction in progress, can't start another one");
            return Ok(());
        }

        if self.sstables.length_of(gen) < threshold {
            return Ok(());
        }

        let sender = self.internal_sender.clone().unwrap();
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

        if let Some(task) = self.compaction_task.take() {
            tracing::warn!("awaiting live compaction...");
            // FIXME: clean up compaction
            fixme("this may deadlock at shutdown, if the channel is full");
            task.await.unwrap().unwrap()
        }

        Ok(())
    }

    async fn drop_data(&mut self) -> Result<(), Error> {
        if let Some(task) = self.compaction_task.take() {
            fixme("allow compaction to be cancelled");
        }

        let _ = fixme_msg(self.sstables.decommission().await, "handle error");

        tracing::warn!("DROP DATA @ {:?}", self.path);
        tokio::fs::remove_dir_all(&self.path).await?;

        Ok(())
    }

    async fn handle_get(&mut self, key: String, reply_to: ReplyToGet) -> Result<(), Error> {
        tracing::debug!("TABLE::get[{key}]");

        // Live memtable is likely to have the most recent data, but it won't always.
        let mut found = self.live_memtable.get(&key).cloned();
        if found.is_some() {
            debug!(
                "found in live_memtable {key} @ {:?}",
                found.as_ref().unwrap().0
            );
        }
        // Also check previous memtable
        if let Some(also_found) = self
            .previous_memtable
            .as_ref()
            .and_then(|prev| prev.get(&key).cloned())
        {
            debug!("also found in previous_memtable {key} @ {:?}", also_found.0);
            swap_compare(&mut found, also_found);
        }

        // Also check sstables
        if let Err(e) = self.find_in_sstables(key.as_str(), &mut found).await {
            // Reply with any errors found checking the sstables.
            let _ = reply_to.send(Err(e));
            return Ok(());
        }

        let response = found.ok_or_else(|| Error::DataNotFound { key });

        if let Err(_e) = reply_to.send(response) {
            tracing::error!("error sending reply to oneshot channel");
        }

        Ok(())
    }

    async fn find_in_sstables(
        &mut self,
        key: &str,
        already_found: &mut Option<(Duration, Bytes)>,
    ) -> Result<(), Error> {
        self.sstables.find_compare(key, already_found).await
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
                Ok(())
            } else {
                match self.live_memtable.insert(key, timestamp, data).await {
                    Ok(_) | Err(Error::NewerDataAlreadyHere) => Ok(()),
                    Err(e) => Err(e),
                }
            }
        };
        let _ = reply_to.send(res);

        if self.should_flush() {
            self.internal_sender
                .as_ref()
                .unwrap()
                .send(Event::Tick("handle_put::should_flush"))
                .unwrap();
        }

        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.live_memtable.memsize() > 0
            && (self.live_memtable.memsize() >= self.memtable_size_limit
                || self.live_memtable.last_insert_at.elapsed() > Duration::from_secs(120))
    }

    async fn sync_write_log(&mut self) -> Result<(), Error> {
        self.live_memtable.sync_write_log().await
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

            old_sstable.decommission().await?;
        }

        if self.compaction_task.is_none() {
            tracing::debug!("{:#?}", self);
        }

        if self.compaction_task.is_some() {
            self.compaction_task
                .take()
                .expect("compaction task should have been here")
                .await
                .unwrap()
                .unwrap();
        } else if !self.is_shutting_down {
            panic!("compaction task was missing during a non-shutdown scenario");
        }

        Ok(())
    }

    #[deprecated = "FIXME: explicitly test this logic"]
    pub(crate) async fn find_greater_timestamp_for_key(
        &mut self,
        key: &str,
        timestamp: &Duration,
    ) -> Option<Duration> {
        if let Some((found_ts, _)) = self.live_memtable.get(key) {
            if found_ts >= timestamp {
                return Some(*found_ts);
            }
        }

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

    async fn finalize_flush(&mut self) {
        if self.flush_handle.is_none() {
            return;
        }

        let handle = self.flush_handle.take().unwrap();
        match handle.await.unwrap() {
            Ok(sstable) => {
                self.previous_memtable = None;
                self.sstables.push(sstable);
            }
            Err(e) => {
                tracing::error!("Error in flush task; err={e:?}");
                Err::<(), _>(e).unwrap();
            }
        }
    }

    // pub fn get(&self)
}

// Just a vec of vecs, sstables sorted into their generation ids. Newer generations are lower.
// FIXME: do we need to track timestamp/key ranges?
#[derive(Debug, Default)]
struct GenerationalSSTables {
    inner: Vec<Generation>,
}

#[derive(Debug)]
pub(crate) struct KeyRange {
    pub(crate) first_key: String,
    pub(crate) last_key: String,
    pub(crate) min_timestamp: Duration,
    pub(crate) max_timestamp: Duration,
}
impl KeyRange {
    fn merge_sstable(&mut self, sstable: &SSTable) {
        if sstable.first_key() < self.first_key.as_str() {
            self.first_key = sstable.first_key().to_owned();
        }

        if sstable.last_key() > self.last_key.as_str() {
            self.last_key = sstable.last_key().to_owned();
        }

        if sstable.min_timestamp() < &self.min_timestamp {
            self.min_timestamp = *sstable.min_timestamp()
        }

        if sstable.max_timestamp() > &self.max_timestamp {
            self.max_timestamp = *sstable.max_timestamp()
        }
    }

    fn covers_key(&self, key: &str) -> bool {
        key >= self.first_key.as_str() && key <= self.last_key.as_str()
    }

    fn covers_timestamp(&self, ts: &Duration) -> bool {
        ts >= &self.min_timestamp && ts <= &self.max_timestamp
    }

    fn covers_newer_timestamp(&self, ts: Option<&Duration>) -> bool {
        match ts {
            None => true,
            Some(inner) => inner <= &self.max_timestamp,
        }
    }
}

#[derive(Default, Debug)]
struct Generation {
    inner: Vec<SSTable>,
    key_range: Option<KeyRange>,
}

impl Generation {
    fn push(&mut self, sstable: SSTable) {
        if self.key_range.is_none() {
            self.key_range = Some(sstable.key_range());
        } else {
            self.key_range.as_mut().map(|key_range| {
                key_range.merge_sstable(&sstable);
            });
        }

        self.inner.push(sstable);
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn as_slice(&self) -> &[SSTable] {
        self.inner.as_slice()
    }

    fn remove(&mut self, index: usize) -> SSTable {
        self.inner.remove(index)
    }

    fn iter_mut(&mut self) -> std::slice::IterMut<SSTable> {
        self.inner.iter_mut()
    }

    fn drain(&mut self, arg: std::ops::RangeFrom<usize>) -> Drain<SSTable> {
        self.inner.drain(arg)
    }

    fn covers_key(&self, key: &str) -> bool {
        self.key_range
            .as_ref()
            .map(|key_range| key_range.covers_key(key))
            .unwrap_or_default()
    }

    fn covers_timestamp(&self, timestamp: &Duration) -> bool {
        self.key_range
            .as_ref()
            .map(|key_range| key_range.covers_timestamp(timestamp))
            .unwrap_or_default()
    }

    fn covers_newer_timestamp(&self, timestamp: Option<&Duration>) -> bool {
        self.key_range
            .as_ref()
            .map(|key_range| key_range.covers_newer_timestamp(timestamp))
            .unwrap_or_default()
    }
}

impl GenerationalSSTables {
    fn push(&mut self, sstable: SSTable) {
        while self.inner.len() < sstable.generation() + 1 {
            self.inner.push(Default::default());
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

    /// Find a key/value in these sstables.
    async fn find(&mut self, key: &str) -> Result<(Duration, Bytes), Error> {
        let mut found = None;
        self.find_compare(&key, &mut found).await?;
        found.ok_or_else(|| Error::DataNotFound { key: key.into() })
    }

    /// Find a key/value in these sstables, with maybe an already-fetched result to compare.
    async fn find_compare(
        &mut self,
        key: &str,
        found: &mut Option<(Duration, Bytes)>,
    ) -> Result<(), Error> {
        // what assumptions can we make about generational residency?
        for generation in self.inner.iter_mut() {
            if generation.covers_key(key)
                && generation.covers_newer_timestamp(found.as_ref().map(|x| &x.0))
            {
                for sstable in generation.iter_mut() {
                    match sstable.get(key).await {
                        Ok(res) => {
                            debug!("found in sstable {} {key} @ {:?}", sstable.uuid(), res.0);
                            swap_compare(found, res);
                        }
                        Err(Error::KeyNotInRange)
                        | Err(Error::KeyNotInXorFilter)
                        | Err(Error::DataNotFound { .. }) => {}
                        Err(e) => Err(e)?,
                    }
                }
            }
        }

        Ok(())

        // found.ok_or_else(|| Error::DataNotFound { key: key.into() })
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
                let _ = fixme_msg(sstable.decommission().await, "handle error");
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
                if let Ok(Some(res)) = sstable.find_timestamp_greater_than(key, timestamp).await {
                    return Some(res);
                }

                fixme("handle error?");
            }
        }

        None
    }

    #[deprecated = "probably not worth having, as we need to re-check on the generations anyway"]
    fn covers_key(&self, key: &str) -> bool {
        self.inner
            .iter()
            .any(|generation| generation.covers_key(key))
    }

    fn covers_timestamp(&self, timestamp: &Duration) -> bool {
        self.inner
            .iter()
            .any(|generation| generation.covers_timestamp(timestamp))
    }

    fn covers_newer_timestamp(&self, timestamp: Option<&Duration>) -> bool {
        self.inner
            .iter()
            .any(|generation| generation.covers_newer_timestamp(timestamp))
    }
}

fn swap_compare(a: &mut Option<(Duration, Bytes)>, b: (Duration, Bytes)) {
    match a {
        Some((ts, _)) => {
            if b.0 >= *ts {
                *a = Some(b);
            }
        }
        None => {
            *a = Some(b);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GenerationalSSTables, Table};
    use crate::sstable::{test_helpers::make_key, SSTable};
    use std::{collections::HashMap, iter::successors, time::Duration};

    #[tokio::test]
    async fn test_table_1() {
        let table = Table::new(
            "unified_reports/1000",
            super::TableOptions {
                missing_table_behavior: super::MissingTableBehavior::Create,
                writable: true,
                memtable_size_limit: 1000,
            },
        );
    }

    fn timestamps_iter(start: Duration, step: Duration) -> impl Iterator<Item = Duration> {
        successors(Some(start), move |n| n.checked_add(step))
    }

    fn timestamps_by_sec(ts: u64) -> impl Iterator<Item = Duration> {
        timestamps_iter(Duration::from_secs(ts), Duration::from_secs(1))
    }

    #[tokio::test]
    async fn test_generations() {
        use tempdir::TempDir;

        use crate::sstable::test_helpers;

        let tmpdir = TempDir::new("test_generations").unwrap();

        let mut generations = GenerationalSSTables::default();
        let timestamps = timestamps_iter(Duration::from_secs(1668185000), Duration::from_secs(1));

        let (sstable_0, inserts_0) = test_helpers::build_test_sstable(
            0,
            tmpdir.path(),
            20..30,
            timestamps_by_sec(1668185010),
        )
        .await;
        let (sstable_1, inserts_1) = test_helpers::build_test_sstable(
            1,
            tmpdir.path(),
            5..25,
            timestamps_by_sec(1668185002),
        )
        .await;
        let (sstable_1_1, inserts_1_1) = test_helpers::build_test_sstable(
            1,
            tmpdir.path(),
            0..20,
            timestamps_by_sec(1668185001),
        )
        .await;
        let (sstable_2, inserts_2) = test_helpers::build_test_sstable(
            2,
            tmpdir.path(),
            0..15,
            timestamps_by_sec(1668185000),
        )
        .await;

        fn track_max_timestamps(
            map: &mut HashMap<String, Duration>,
            data: Vec<(String, Duration)>,
        ) {
            for (key, ts) in data {
                let entry = map.entry(key).or_insert(ts);
                if ts > *entry {
                    *entry = ts
                }
            }
        }

        let mut max_timestamp_by_key = HashMap::new();

        // generations.push(sstable_2);
        let key_000001 = make_key(1);
        assert!(
            !generations.covers_key(key_000001.as_str()),
            "empty table covers nothing"
        );
        assert!(
            !generations.covers_timestamp(&Duration::from_secs(1668185001)),
            "empty table covers nothing"
        );

        async fn assert_data(
            generations: &mut GenerationalSSTables,
            keys: &HashMap<String, Duration>,
        ) {
            for (key, expected_ts) in keys.iter() {
                let (actual_ts, _data) = generations.find(key).await.unwrap();

                println!("{key} @ {actual_ts:?} {_data:?}");
                assert_eq!(actual_ts, *expected_ts, "timestamps differed for key={key}, actual={actual_ts:?}, expected={expected_ts:?}");
            }
        }

        generations.push(sstable_2);
        track_max_timestamps(&mut max_timestamp_by_key, inserts_2);
        assert_data(&mut generations, &max_timestamp_by_key).await;
        assert!(generations.covers_key(key_000001.as_str()),);
        assert!(generations.covers_timestamp(&Duration::from_secs(1668185001)),);
        assert!(!generations.covers_key(&make_key(19)));

        generations.push(sstable_1_1);
        track_max_timestamps(&mut max_timestamp_by_key, inserts_1_1);
        assert_data(&mut generations, &max_timestamp_by_key).await;
        assert!(generations.covers_key(key_000001.as_str()));
        assert!(generations.covers_key(&make_key(19)));
        assert!(generations.covers_timestamp(&Duration::from_secs(1668185020)),);
        assert!(!generations.covers_timestamp(&Duration::from_secs(1668185021)),);

        generations.push(sstable_1);
        track_max_timestamps(&mut max_timestamp_by_key, inserts_1);
        assert_data(&mut generations, &max_timestamp_by_key).await;
        assert!(generations.covers_timestamp(&Duration::from_secs(1668185021)),);
        assert!(generations.covers_key(&make_key(24)));

        generations.push(sstable_0);
        track_max_timestamps(&mut max_timestamp_by_key, inserts_0);
        assert_data(&mut generations, &max_timestamp_by_key).await;
    }
}
