use std::{fmt::Display, io};

#[derive(Debug)]
pub enum Error {
    Data,
    Io(io::Error),
    Threading,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Data => f.write_str("copying multiple files to one path; probable data loss"),
            Error::Io(e) => e.fmt(f),
            Error::Threading => f.write_str("thread join failed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(v: io::Error) -> Self {
        Self::Io(v)
    }
}
