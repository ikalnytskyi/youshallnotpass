use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The configured rate-limit has been exceeded. New attempts might succeed after the specified delay.
    RetryAfter(Duration),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::RetryAfter(duration) => {
                write!(f, "Retry after {:.1} seconds", duration.as_secs_f64())
            }
        }
    }
}

impl std::error::Error for Error {}
