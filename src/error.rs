use std::time::Duration;

/// Error type describing various possible conditions for why requests are rejected.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The corresponding entity is completely blocked. New attempts will also result in failures.
    Blocked,

    /// The configured rate-limit has been exceeded. New attempts might succeed after the specified delay.
    RetryAfter(Duration),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Blocked => write!(f, "Entity is blocked"),
            Error::RetryAfter(duration) => {
                write!(f, "Retry after {:.1} seconds", duration.as_secs_f64())
            }
        }
    }
}

impl std::error::Error for Error {}
