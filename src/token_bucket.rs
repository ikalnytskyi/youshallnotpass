use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::Error;

pub struct TokenBucket<'a> {
    time_per_token: usize,
    interval: Duration,
    last_replenished_at: Mutex<Option<Instant>>,
    clock: &'a (dyn Fn() -> Instant + Sync),
}

impl<'a> TokenBucket<'a> {
    /// Create a new TokenBucket with `limit` tokens generated with fixed
    /// rate over the specified `interval` of time.
    ///
    /// Tokens can be consumed all at once or over time. Specifying the
    /// `limit` (or `interval`) of 0 has a meaning of blocking a given
    /// entity: no tokens can be consumed in that case, regardless of how
    /// much time passes or how many attempts are performed.
    ///
    /// ```
    /// use std::time::Duration;
    /// use youshallnotpass::TokenBucket;
    ///
    /// // create a bucket that allows to consume 3 tokens every 60 seconds
    /// let bucket = TokenBucket::new(3, Duration::from_secs(60));
    /// assert!(bucket.consume(1).is_ok());
    /// assert!(bucket.consume(1).is_ok());
    /// assert!(bucket.consume(1).is_ok());
    /// assert!(bucket.consume(1).is_err());
    /// ```
    pub fn new(limit: usize, interval: Duration) -> Self {
        TokenBucket::with_timer(limit, interval, &Instant::now)
    }

    /// Same as [`TokenBucket::new()`], but allows to override the internal clock,
    /// which is mainly useful in tests.
    pub(crate) fn with_timer(
        limit: usize,
        interval: Duration,
        clock: &'a (dyn Fn() -> Instant + Sync),
    ) -> Self {
        TokenBucket {
            time_per_token: if limit > 0 {
                interval.as_nanos() as usize / limit
            } else {
                0
            },
            interval,
            last_replenished_at: Mutex::new(None),
            clock,
        }
    }

    /// Try to consume the specified number of `tokens` from the bucket.
    ///
    /// If the bucket has the sufficient number of tokens available, they are *consumed*
    /// and `Ok(())` is returned.
    ///
    /// If the bucket has fewer tokens available, the internal state is *not* modified,
    /// and [`Error::RetryAfter`] is returned. The error will specify how much time the
    /// caller has to wait before trying to call [`TokenBucket::consume()`] with the
    /// same arguments again. Retrying the operation earlier will result in the same error.
    ///
    /// If the bucket has a limit of 0 tokens, [`Error::Blocked`] is always returned instead,
    /// regardless of how much time the caller waits between attempts.
    ///
    /// ```
    /// use std::time::Duration;
    /// use youshallnotpass::{TokenBucket, Error};
    ///
    /// // create a new bucket that allows to consume 3 tokens every 60 seconds
    /// let bucket = TokenBucket::new(3, Duration::from_secs(60));
    /// assert!(bucket.consume(1).is_ok());
    /// assert!(bucket.consume(1).is_ok());
    /// assert!(bucket.consume(1).is_ok());
    /// assert!(matches!(bucket.consume(1), Err(Error::RetryAfter(duration))));
    ///
    /// // create a new bucket that does not allow to consume any tokens
    /// let bucket = TokenBucket::new(0, Duration::from_secs(60));
    /// assert!(matches!(bucket.consume(1), Err(Error::Blocked)));
    /// ```
    pub fn consume(&self, tokens: usize) -> Result<(), Error> {
        if self.time_per_token == 0 {
            return Err(Error::Blocked);
        }

        let now = (self.clock)();
        let mut lock = self.last_replenished_at.lock().unwrap();

        let interval_start = now.checked_sub(self.interval).unwrap_or(now);
        let token_delay = Duration::from_nanos((tokens * self.time_per_token) as u64);
        let last_replenished_at = lock.unwrap_or(interval_start);

        let required_time = std::cmp::max(interval_start, last_replenished_at) + token_delay;
        if required_time > now {
            Err(Error::RetryAfter(required_time - now))
        } else {
            *lock = Some(required_time);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    #[test]
    fn new() {
        let bucket = TokenBucket::new(3, Duration::from_secs(60));

        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        // we don't mock time in this test case, so checking the retry-after delay would be unreliable
        assert!(matches!(bucket.consume(1), Err(Error::RetryAfter(_))));
    }

    #[test]
    fn blocked_limit() {
        let bucket = TokenBucket::new(0, Duration::from_secs(60));

        // tokens are not being added to the bucket; the entity is effectively blocked,
        // and retries are useless
        assert_eq!(bucket.consume(1), Err(Error::Blocked));
        assert_eq!(bucket.consume(1), Err(Error::Blocked));
        assert_eq!(bucket.consume(1), Err(Error::Blocked));
    }

    #[test]
    fn blocked_duration() {
        let bucket = TokenBucket::new(42, Duration::from_secs(0));

        // tokens are not being added to the bucket; the entity is effectively blocked,
        // and retries are useless
        assert_eq!(bucket.consume(1), Err(Error::Blocked));
        assert_eq!(bucket.consume(1), Err(Error::Blocked));
        assert_eq!(bucket.consume(1), Err(Error::Blocked));
    }

    #[test]
    fn capacity_is_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();
        let bucket = TokenBucket::with_timer(1, Duration::from_secs(1), &clock);

        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );
    }

    #[test]
    fn capacity_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();
        let bucket = TokenBucket::with_timer(3, Duration::from_secs(1), &clock);

        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );
    }

    #[test]
    fn period_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();
        let bucket = TokenBucket::with_timer(1, Duration::from_secs(3), &clock);

        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_secs(3)))
        );

        *now.lock().unwrap() += Duration::from_secs(2);
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );

        *now.lock().unwrap() += Duration::from_secs(3);
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_secs(3)))
        );
    }

    #[test]
    fn consume_over_time() {
        let t0 = Instant::now();
        let now = Mutex::new(t0);
        let clock = || *now.lock().unwrap();
        let bucket = TokenBucket::with_timer(4, Duration::from_secs(1), &clock);

        // consume first token
        *now.lock().unwrap() = t0;
        assert_eq!(bucket.consume(1), Ok(()));

        // consume second token
        *now.lock().unwrap() = t0 + Duration::from_millis(50);
        assert_eq!(bucket.consume(1), Ok(()));

        // consume third & fourth tokens
        *now.lock().unwrap() = t0 + Duration::from_millis(150);
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));

        // ensure we are out of tokens
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_millis(100)))
        );

        // one token is not yet replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(249);
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_millis(1)))
        );

        // one token is replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(250);
        assert_eq!(bucket.consume(1), Ok(()));

        // ensure we are out of tokens again
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_millis(250)))
        );

        // two tokens are replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(750);
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_millis(250)))
        );
    }

    #[test]
    fn consume_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();
        let bucket = TokenBucket::with_timer(3, Duration::from_secs(1), &clock);

        // consume all tokens at once
        assert_eq!(bucket.consume(3), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );

        // sequentially consume tokens
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(bucket.consume(2), Ok(()));
        assert_eq!(
            bucket.consume(2),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );

        // two tokens are replenished
        *now.lock().unwrap() += Duration::from_millis(700);
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(bucket.consume(1), Ok(()));
        assert_eq!(
            bucket.consume(1),
            Err(Error::RetryAfter(Duration::from_nanos(299_999_998)))
        );
    }
}
