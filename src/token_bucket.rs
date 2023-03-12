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
    pub fn new(limit: usize, interval: Duration) -> Self {
        TokenBucket::with_timer(limit, interval, &Instant::now)
    }

    pub(crate) fn with_timer(
        limit: usize,
        interval: Duration,
        clock: &'a (dyn Fn() -> Instant + Sync),
    ) -> Self {
        assert!(limit > 0, "limit must be a positive integer");

        TokenBucket {
            time_per_token: interval.as_nanos() as usize / limit,
            interval,
            last_replenished_at: Mutex::new(None),
            clock,
        }
    }

    pub fn consume(&self, tokens: usize) -> Result<(), Error> {
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
        assert!(bucket.consume(1).is_err());
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
