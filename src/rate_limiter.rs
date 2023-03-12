use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::TokenBucket;

pub struct RateLimiter<'a, K> {
    buckets: HashMap<K, TokenBucket<'a>>,
    clock: &'a (dyn Fn() -> Instant + Sync),
}

impl<'a, K: Eq + Hash> RateLimiter<'a, K> {
    pub fn new() -> Self {
        Self::with_timer(&Instant::now)
    }

    fn with_timer(clock: &'a (dyn Fn() -> Instant + Sync)) -> Self {
        Self {
            buckets: HashMap::new(),
            clock,
        }
    }

    pub fn set_limit(&mut self, key: K, limit: usize, interval: Duration) {
        self.buckets
            .insert(key, TokenBucket::with_timer(limit, interval, self.clock));
    }

    pub fn consume(&self, key: K, tokens: usize) -> Result<(), Error> {
        self.buckets
            .get(&key)
            .map(|bucket| bucket.consume(tokens))
            .unwrap_or(Ok(()))
    }
}

impl<'a, K: Eq + Hash> Default for RateLimiter<'a, K> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn new() {
        let mut limiter = RateLimiter::new();

        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("B", 1), Ok(()));

        limiter.set_limit("A", 3, Duration::from_secs(60));

        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        // we don't mock time in this test case, so checking the retry-after delay would be unreliable
        assert!(limiter.consume("A", 1).is_err());
    }

    #[test]
    fn capacity_is_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 1, Duration::from_secs(1));

        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );
    }

    #[test]
    fn capacity_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 3, Duration::from_secs(1));

        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );
    }

    #[test]
    fn period_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 1, Duration::from_secs(3));

        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_secs(3)))
        );

        *now.lock().unwrap() += Duration::from_secs(2);
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );

        *now.lock().unwrap() += Duration::from_secs(3);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_secs(3)))
        );
    }

    #[test]
    fn consume_over_time() {
        let t0 = Instant::now();
        let now = Mutex::new(t0);
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 4, Duration::from_secs(1));

        // consume first token
        *now.lock().unwrap() = t0;
        assert_eq!(limiter.consume("A", 1), Ok(()));

        // consume second token
        *now.lock().unwrap() = t0 + Duration::from_millis(50);
        assert_eq!(limiter.consume("A", 1), Ok(()));

        // consume third & fourth tokens
        *now.lock().unwrap() = t0 + Duration::from_millis(150);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));

        // ensure we are out of tokens
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_millis(100)))
        );

        // one token is not yet replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(249);
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_millis(1)))
        );

        // one token is replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(250);
        assert_eq!(limiter.consume("A", 1), Ok(()));

        // ensure we are out of tokens again
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_millis(250)))
        );

        // two tokens are replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(750);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_millis(250)))
        );
    }

    #[test]
    fn consume_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 3, Duration::from_secs(1));

        // consume all tokens at once
        assert_eq!(limiter.consume("A", 3), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );

        // sequentially consume tokens
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 2), Ok(()));
        assert_eq!(
            limiter.consume("A", 2),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );

        // two tokens are replenished
        *now.lock().unwrap() += Duration::from_millis(700);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_nanos(299_999_998)))
        );
    }

    #[test]
    fn multiple_buckets() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 2, Duration::from_secs(1));
        limiter.set_limit("B", 1, Duration::from_secs(2));

        // consume tokens in A and B
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_millis(500)))
        );
        assert_eq!(limiter.consume("B", 1), Ok(()));
        assert_eq!(
            limiter.consume("B", 1),
            Err(Error::RetryAfter(Duration::from_secs(2)))
        );

        // tokens in A are replenished, but not in B
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_millis(500)))
        );
        assert_eq!(
            limiter.consume("B", 1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );

        // tokens in A and B are replenished
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(
            limiter.consume("A", 1),
            Err(Error::RetryAfter(Duration::from_millis(500)))
        );
        assert_eq!(limiter.consume("B", 1), Ok(()));
        assert_eq!(
            limiter.consume("B", 1),
            Err(Error::RetryAfter(Duration::from_secs(2)))
        );
    }

    #[test]
    fn compound_key() {
        #[derive(Eq, PartialEq, Hash)]
        enum MyHttpVerb {
            GET,
            PUT,
        }

        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit((MyHttpVerb::PUT, "/foobar"), 1, Duration::from_secs(1));
        limiter.set_limit((MyHttpVerb::GET, "/foobar"), 3, Duration::from_secs(1));
        limiter.set_limit((MyHttpVerb::GET, "/spam"), 2, Duration::from_secs(1));

        assert_eq!(limiter.consume((MyHttpVerb::GET, "/foobar"), 1), Ok(()));
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/foobar"), 1), Ok(()));
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/foobar"), 1), Ok(()));
        assert_eq!(
            limiter.consume((MyHttpVerb::GET, "/foobar"), 1),
            Err(Error::RetryAfter(Duration::from_nanos(333_333_332)))
        );

        assert_eq!(limiter.consume((MyHttpVerb::PUT, "/foobar"), 1), Ok(()));
        assert_eq!(
            limiter.consume((MyHttpVerb::PUT, "/foobar"), 1),
            Err(Error::RetryAfter(Duration::from_secs(1)))
        );

        assert_eq!(limiter.consume((MyHttpVerb::GET, "/spam"), 1), Ok(()));
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/spam"), 1), Ok(()));
        assert_eq!(
            limiter.consume((MyHttpVerb::GET, "/spam"), 1),
            Err(Error::RetryAfter(Duration::from_millis(500)))
        );
    }
}
