use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

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

    pub fn consume(&self, key: K, tokens: usize) -> bool {
        self.buckets
            .get(&key)
            .map(|bucket| bucket.consume(tokens))
            .unwrap_or(true)
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

        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("B", 1), true);

        limiter.set_limit("A", 3, Duration::from_secs(60));

        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
    }

    #[test]
    fn capacity_is_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 1, Duration::from_secs(1));

        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
    }

    #[test]
    fn capacity_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 3, Duration::from_secs(1));

        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
    }

    #[test]
    fn period_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 1, Duration::from_secs(3));

        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);

        *now.lock().unwrap() += Duration::from_secs(2);
        assert_eq!(limiter.consume("A", 1), false);

        *now.lock().unwrap() += Duration::from_secs(3);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
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
        assert_eq!(limiter.consume("A", 1), true);

        // consume second token
        *now.lock().unwrap() = t0 + Duration::from_millis(50);
        assert_eq!(limiter.consume("A", 1), true);

        // consume third & fourth tokens
        *now.lock().unwrap() = t0 + Duration::from_millis(150);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);

        // ensure we are out of tokens
        assert_eq!(limiter.consume("A", 1), false);

        // one token is not yet replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(249);
        assert_eq!(limiter.consume("A", 1), false);

        // one token is replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(250);
        assert_eq!(limiter.consume("A", 1), true);

        // ensure we are out of tokens again
        assert_eq!(limiter.consume("A", 1), false);

        // two tokens are replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(750);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
    }

    #[test]
    fn weight_gt_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 3, Duration::from_secs(1));

        // consume all tokens at once
        assert_eq!(limiter.consume("A", 3), true);
        assert_eq!(limiter.consume("A", 1), false);

        // sequentially consume tokens
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 2), true);
        assert_eq!(limiter.consume("A", 2), false);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);

        // two tokens are replenished
        *now.lock().unwrap() += Duration::from_millis(700);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
    }

    #[test]
    fn multiple_buckets() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();

        let mut limiter = RateLimiter::with_timer(&clock);
        limiter.set_limit("A", 2, Duration::from_secs(1));
        limiter.set_limit("B", 1, Duration::from_secs(2));

        // consume tokens in A and B
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
        assert_eq!(limiter.consume("B", 1), true);
        assert_eq!(limiter.consume("B", 1), false);

        // tokens in A are replenished, but not in B
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
        assert_eq!(limiter.consume("B", 1), false);

        // tokens in A and B are replenished
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), true);
        assert_eq!(limiter.consume("A", 1), false);
        assert_eq!(limiter.consume("B", 1), true);
        assert_eq!(limiter.consume("B", 1), false);
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

        assert_eq!(limiter.consume((MyHttpVerb::GET, "/foobar"), 1), true);
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/foobar"), 1), true);
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/foobar"), 1), true);
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/foobar"), 1), false);

        assert_eq!(limiter.consume((MyHttpVerb::PUT, "/foobar"), 1), true);
        assert_eq!(limiter.consume((MyHttpVerb::PUT, "/foobar"), 1), false);

        assert_eq!(limiter.consume((MyHttpVerb::GET, "/spam"), 1), true);
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/spam"), 1), true);
        assert_eq!(limiter.consume((MyHttpVerb::GET, "/spam"), 1), false);
    }
}
