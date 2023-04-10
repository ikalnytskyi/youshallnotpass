use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::TokenBucket;

/// An object providing rate limiting functionality.
///
/// A rate limiter controls how frequently some event, such as an HTTP request,
/// is allowed to happen. Rate limiting is commonly used as a defensive measure
/// to protect services from excessive use (intended or not) and maintain their
/// availability.
///
/// A [`RateLimiter`] instance can be used to set how many times an event is
/// allowed to happen (`limit`) within a given period of time (`interval`). If
/// no such policy is set for an event, the event is always allowed.
///
/// Once constructed, a `RateLimiter` instance is safe to be used from multiple
/// threads.
///
/// Under the hood the token bucket algorithm is used. See [`TokenBucket`] for
/// details.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use youshallnotpass::{RateLimiter, Error};
///
/// let limiter = RateLimiter::configure()
///     .limit("A", 2, Duration::from_secs(60))
///     .limit("B", 3, Duration::from_secs(60))
///     .done();
///
/// assert_eq!(limiter.consume("A", 1), Ok(()));
/// assert_eq!(limiter.consume("A", 1), Ok(()));
///
/// assert!(matches!(limiter.consume("A", 1), Err(Error::RetryAfter(_))));
/// assert!(matches!(limiter.consume("B", 5), Err(Error::RetryAfter(_))));
/// ```
pub struct RateLimiter<'a, K> {
    buckets: HashMap<K, TokenBucket<'a>>,
}

impl<'a, K> RateLimiter<'a, K> {
    /// Constructs a new `RateLimiterBuilder` object.
    ///
    /// A returned instance of [`RateLimiterBuilder`] can be used to set
    /// limiting policies and construct an instance of `RateLimiter` via
    /// [`limit`] and [`done`] functions. See documentation of corresponding
    /// functions for details.
    ///
    /// [`limit`]: RateLimiterBuilder::limit
    /// [`done`]: RateLimiterBuilder::done
    ///
    /// # Examples
    ///
    /// ```
    /// use youshallnotpass::RateLimiter;
    ///
    /// let builder = RateLimiter::<&str>::configure();
    /// ```
    #[inline]
    pub fn configure() -> RateLimiterBuilder<'a, K> {
        Self::with_timer(&Instant::now)
    }

    /// Constructs a new `RateLimiterBuilder` object with custom `clock`
    /// function.
    ///
    /// Unlike [`configure`], this function receives custom `clock` function to
    /// be used instead of [`Instant::now`]. It doesn't make sense to provide
    /// custom `clock` unless you want to test the object. That's why this
    /// function is private and not exposed to end users.
    ///
    /// [`configure`]: RateLimiter::configure
    #[inline]
    fn with_timer(clock: &'a (dyn Fn() -> Instant + Sync)) -> RateLimiterBuilder<'a, K> {
        RateLimiterBuilder {
            limits: Vec::new(),
            clock,
        }
    }
}

impl<'a, K: Eq + Hash> RateLimiter<'a, K> {
    /// Tries to consume the specified number of `tokens` from the bucket for a
    /// given event (`key`).
    ///
    /// The `consume` function retrieves a bucket for a given `key`, and
    /// delegates token consumption to [`TokenBucket::consume`] function. Please
    /// see [`TokenBucket`] documentation for details on what's returned by this
    /// function.
    ///
    /// If not `limit` is set, the `consume` function always succeed.
    ///
    /// See [`limit`] for how to setup a limiting policy for a `key`.
    ///
    /// [`limit`]: RateLimiterBuilder::limit
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use youshallnotpass::RateLimiter;
    ///
    /// let limiter = RateLimiter::configure()
    ///     .limit("A", 2, Duration::from_secs(60))
    ///     .done();
    ///
    /// assert!(limiter.consume("A", 1).is_ok());
    /// assert!(limiter.consume("A", 1).is_ok());
    /// assert!(limiter.consume("A", 1).is_err());
    ///
    /// assert!(limiter.consume("B", 1).is_ok());
    /// ```
    pub fn consume(&self, key: K, tokens: usize) -> Result<(), Error> {
        self.buckets
            .get(&key)
            .map(|bucket| bucket.consume(tokens))
            .unwrap_or(Ok(()))
    }
}

/// The builder exposes ability to configure a [`RateLimiter`] instance by
/// setting limiting policies.
pub struct RateLimiterBuilder<'a, K> {
    limits: Vec<(K, usize, Duration)>,
    clock: &'a (dyn Fn() -> Instant + Sync),
}

impl<'a, K> RateLimiterBuilder<'a, K> {
    /// Sets a limiting policy for a `key`.
    ///
    /// The limiting policy sets how many times an event is allowed to happen
    /// (`limit`) within a given period of time (`interval`). Event is vague
    /// term. Thus we use a `key` to uniquely identify an event we want to rate
    /// limit.
    pub fn limit(mut self, key: K, limit: usize, interval: Duration) -> Self {
        self.limits.push((key, limit, interval));
        self
    }
}

impl<'a, K: Eq + Hash> RateLimiterBuilder<'a, K> {
    /// Constructs a [`RateLimiter`] instance with configured limiting policies.
    ///
    /// Once constructed, the `RateLimiter` instance cannot be changed.
    pub fn done(self) -> RateLimiter<'a, K> {
        RateLimiter {
            buckets: self
                .limits
                .into_iter()
                .map(|(key, limit, interval)| {
                    (key, TokenBucket::with_timer(limit, interval, self.clock))
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn new() {
        let limiter = RateLimiter::configure()
            .limit("A", 3, Duration::from_secs(60))
            .done();

        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        assert_eq!(limiter.consume("A", 1), Ok(()));
        // we don't mock time in this test case, so checking the retry-after delay would be unreliable
        assert!(matches!(limiter.consume("A", 1), Err(Error::RetryAfter(_))));
    }

    #[test]
    fn blocked_limit() {
        let limiter = RateLimiter::configure()
            .limit("A", 0, Duration::from_secs(60))
            .done();

        // using a limit of 0 blocks the given entity
        assert_eq!(limiter.consume("A", 1), Err(Error::Blocked));
        assert_eq!(limiter.consume("A", 1), Err(Error::Blocked));
        assert_eq!(limiter.consume("A", 1), Err(Error::Blocked));
    }

    #[test]
    fn blocked_duration() {
        let limiter = RateLimiter::configure()
            .limit("A", 42, Duration::from_secs(0))
            .done();

        assert_eq!(limiter.consume("A", 1), Err(Error::Blocked));
        assert_eq!(limiter.consume("A", 1), Err(Error::Blocked));
        assert_eq!(limiter.consume("A", 1), Err(Error::Blocked));
    }

    #[test]
    fn capacity_is_one() {
        let now = Mutex::new(Instant::now());
        let clock = || *now.lock().unwrap();
        let limiter = RateLimiter::with_timer(&clock)
            .limit("A", 1, Duration::from_secs(1))
            .done();

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
        let limiter = RateLimiter::with_timer(&clock)
            .limit("A", 3, Duration::from_secs(1))
            .done();

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
        let limiter = RateLimiter::with_timer(&clock)
            .limit("A", 1, Duration::from_secs(3))
            .done();

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
        let limiter = RateLimiter::with_timer(&clock)
            .limit("A", 4, Duration::from_secs(1))
            .done();

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
        let limiter = RateLimiter::with_timer(&clock)
            .limit("A", 3, Duration::from_secs(1))
            .done();

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
        let limiter = RateLimiter::with_timer(&clock)
            .limit("A", 2, Duration::from_secs(1))
            .limit("B", 1, Duration::from_secs(2))
            .done();

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
        let limiter = RateLimiter::with_timer(&clock)
            .limit((MyHttpVerb::PUT, "/foobar"), 1, Duration::from_secs(1))
            .limit((MyHttpVerb::GET, "/foobar"), 3, Duration::from_secs(1))
            .limit((MyHttpVerb::GET, "/spam"), 2, Duration::from_secs(1))
            .done();

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
