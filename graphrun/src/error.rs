use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidArgument,
    AlreadyExists,
    FailedPrecondition,
    Unauthenticated,
    PermissionDenied,
    NotFound,
    Unavailable,
    ResourceExhausted,
    DeadlineExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
    pub path: Option<String>,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            path: None,
            line: None,
            column: None,
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, message)
    }

    pub fn at_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn at_span(mut self, line: usize, column: usize) -> Self {
        self.line = Some(line);
        self.column = Some(column);
        self
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.path, self.line, self.column) {
            (Some(path), Some(line), Some(column)) => {
                write!(
                    f,
                    "{:?} at {path}:{line}:{column}: {}",
                    self.kind, self.message
                )
            }
            (Some(path), _, _) => write!(f, "{:?} at {path}: {}", self.kind, self.message),
            (None, Some(line), Some(column)) => {
                write!(f, "{:?} at {line}:{column}: {}", self.kind, self.message)
            }
            _ => write!(f, "{:?}: {}", self.kind, self.message),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
