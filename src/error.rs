use std::io;

#[derive(Debug)]
pub enum Error {
    Other { description: String },
    Closed,
    NewerDataAlreadyHere,
    // FIXME: exclude key here?
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
