use std::io;

#[derive(Debug)]
pub enum Error {
    Other { description: String },
    Closed,
    NewerDataAlreadyHere,
    DataNotFound,
    Io { source: io::Error },
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::Io { source }
    }
}
