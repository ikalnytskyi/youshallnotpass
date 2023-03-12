use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct TokenBucket {
    time_per_token: usize,
    interval: Duration,
    last_replenished_at: Mutex<Option<Instant>>,
    clock: Box<dyn Fn() -> Instant + Sync>,
}

impl TokenBucket {
    pub fn new(limit: usize, interval: Duration) -> Self {
        TokenBucket::with_timer(limit, interval, Box::new(Instant::now))
    }

    pub fn with_timer(
        limit: usize,
        interval: Duration,
        clock: Box<dyn Fn() -> Instant + Sync>,
    ) -> Self {
        assert!(limit > 0);

        TokenBucket {
            time_per_token: interval.as_nanos() as usize / limit,
            interval,
            last_replenished_at: Mutex::new(None),
            clock,
        }
    }

    pub fn consume(&self, tokens: usize) -> bool {
        let now = (self.clock)();

        let mut lock = self.last_replenished_at.lock().unwrap();

        let interval_start = now.checked_sub(self.interval).unwrap_or(now);
        let token_delay = Duration::from_nanos((tokens * self.time_per_token) as u64);
        let last_replenished_at = lock.unwrap_or(interval_start);

        let required_time = std::cmp::max(interval_start, last_replenished_at) + token_delay;
        if required_time > now {
            false
        } else {
            *lock = Some(required_time);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    #[test]
    fn capacity_is_one() {
        let now = Arc::new(Mutex::new(Instant::now()));
        let now_moved = now.clone();

        let bucket = TokenBucket::with_timer(
            1,
            Duration::from_secs(1),
            Box::new(move || *now_moved.lock().unwrap()),
        );

        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn capacity_gt_one() {
        let now = Arc::new(Mutex::new(Instant::now()));
        let now_moved = now.clone();

        let bucket = TokenBucket::with_timer(
            3,
            Duration::from_secs(1),
            Box::new(move || *now_moved.lock().unwrap()),
        );

        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn period_gt_one() {
        let now = Arc::new(Mutex::new(Instant::now()));
        let now_moved = now.clone();

        let bucket = TokenBucket::with_timer(
            1,
            Duration::from_secs(3),
            Box::new(move || *now_moved.lock().unwrap()),
        );

        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        *now.lock().unwrap() += Duration::from_secs(2);
        assert_eq!(bucket.consume(1), false);

        *now.lock().unwrap() += Duration::from_secs(3);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn consume_over_time() {
        let t0 = Instant::now();
        let now = Arc::new(Mutex::new(t0.clone()));
        let now_moved = now.clone();

        let bucket = TokenBucket::with_timer(
            4,
            Duration::from_secs(1),
            Box::new(move || *now_moved.lock().unwrap()),
        );

        // consume first token
        *now.lock().unwrap() = t0;
        assert_eq!(bucket.consume(1), true);

        // consume second token
        *now.lock().unwrap() = t0 + Duration::from_millis(50);
        assert_eq!(bucket.consume(1), true);

        // consume third & fourth tokens
        *now.lock().unwrap() = t0 + Duration::from_millis(150);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);

        // ensure we are out of tokens
        assert_eq!(bucket.consume(1), false);

        // one token is not yet replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(249);
        assert_eq!(bucket.consume(1), false);

        // one token is replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(250);
        assert_eq!(bucket.consume(1), true);

        // ensure we are out of tokens again
        assert_eq!(bucket.consume(1), false);

        // two tokens are replenished
        *now.lock().unwrap() = t0 + Duration::from_millis(750);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn weight_gt_one() {
        let now = Arc::new(Mutex::new(Instant::now()));
        let now_moved = now.clone();

        let bucket = TokenBucket::with_timer(
            3,
            Duration::from_secs(1),
            Box::new(move || *now_moved.lock().unwrap()),
        );

        // consume all tokens at once
        assert_eq!(bucket.consume(3), true);
        assert_eq!(bucket.consume(1), false);

        // sequentially consume tokens
        *now.lock().unwrap() += Duration::from_secs(1);
        assert_eq!(bucket.consume(2), true);
        assert_eq!(bucket.consume(2), false);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        // two tokens are replenished
        *now.lock().unwrap() += Duration::from_millis(700);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }
}
