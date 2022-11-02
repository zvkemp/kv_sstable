use std::io;

#[derive(Debug)]
pub enum Error {
    Other { description: String },
    Closed,
    NewerDataAlreadyHere,
    DataNotFound { key: String },
    Io { source: io::Error },
    OkShutdown,
    KeyNotInRange,
    NoTablesInCompaction,
    MemTableClosed,
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::Io { source }
    }
}
