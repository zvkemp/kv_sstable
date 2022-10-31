use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};

use crate::{
    error::Error,
    fixme, fixme_msg,
    sstable::{MemTable, SSTable},
};

pub struct Table {
    path: PathBuf,
    live_memtable: MemTable,
    previous_memtable: Option<Arc<MemTable>>,
    sstables: Vec<SSTable>,
    size_limit: usize,
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
}

pub enum Reply {
    Data(Bytes),
    Empty,
}

pub type ReplyToGet = oneshot::Sender<Result<Bytes, Error>>;
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

    pub fn spawn(self) -> Result<Sender<Event>, Error> {
        let (sender, receiver) = tokio::sync::mpsc::channel(256);
        let handle = tokio::spawn(async move { todo!() });

        Ok(sender)
    }

    pub(crate) async fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Table {
            path: path.as_ref().to_owned(),
            live_memtable: MemTable::new(path.as_ref()).await?,
            previous_memtable: None,
            sstables: vec![],
            size_limit: fixme_msg(1024 * 1024 * 500, "500MB; make configurable"),
        })
    }

    async fn tick(&mut self) -> Result<(), Error> {
        todo!()
        // if self.live_memtable.memsize
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

            None => Err(Error::Closed),
        }
    }

    async fn handle_get(&mut self, key: String, reply_to: ReplyToGet) -> Result<(), Error> {
        fixme("is it always correct to short-circuit if data was found in the live table?");

        let response = if let Some((_ts, bytes)) = self.live_memtable.get(&key).cloned() {
            Ok(bytes)
        } else if let Some((ts, bytes)) = self
            .previous_memtable
            .as_ref()
            .and_then(|prev| prev.get(&key).cloned())
        {
            Ok(bytes)
        } else {
            fixme("also look in the sstables");

            Err(Error::DataNotFound)
        };

        if let Err(e) = reply_to.send(response) {
            tracing::error!("error sending reply to oneshot channel");
        }

        Ok(())
    }

    async fn handle_put(
        &mut self,
        key: String,
        data: Bytes,
        timestamp: Duration,
        reply_to: EmptyReplyTo,
    ) -> Result<(), Error> {
        self.live_memtable.insert(key, timestamp, data).await
    }

    // pub fn get(&self)
}
#[cfg(test)]
mod tests {
    use super::Table;

    #[tokio::test]
    async fn test_table_1() {
        let table = Table::new("unified_reports/1000");
    }
}
