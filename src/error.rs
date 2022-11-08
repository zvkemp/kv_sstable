use std::{fmt::Display, io, path::PathBuf};

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
    SSTableAlreadyExists { path: PathBuf },
    NoDataWasWritten,
    InvalidUuid { input: String },
    InvalidChecksum,
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::Io { source }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
