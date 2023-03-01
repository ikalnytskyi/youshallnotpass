use std::time::{Duration, Instant};

pub struct TokenBucket {
    interval: Duration,
    time_per_token: u64,
    last_token_used_at: Option<Instant>,
    clock: Box<dyn Fn() -> Instant>,
}

impl TokenBucket {
    pub fn new(limit: usize, interval: Duration) -> Self {
        Self {
            interval,
            time_per_token: interval.as_nanos() as u64 / limit as u64,
            last_token_used_at: None,
            clock: Box::new(Instant::now),
        }
    }

    pub fn with_timer(mut self, clock: Box<dyn Fn() -> Instant>) -> Self {
        self.clock = clock;
        self
    }

    pub fn consume(&mut self, tokens: usize) -> bool {
        let now = (self.clock)();
        let replenish_delay = Duration::from_nanos(tokens as u64 * self.time_per_token);
        let interval_started_at = now.checked_sub(self.interval).unwrap_or(now);
        let last_token_used_at = self.last_token_used_at.unwrap_or(interval_started_at);

        let required_time =
            std::cmp::max(interval_started_at, last_token_used_at) + replenish_delay;
        if required_time > now {
            false
        } else {
            self.last_token_used_at = Some(required_time);
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn capacity_is_one() {
        let now = Rc::new(RefCell::new(Instant::now()));
        let now_moved = now.clone();

        let mut bucket = TokenBucket::new(1, Duration::from_secs(1))
            .with_timer(Box::new(move || *now_moved.borrow()));

        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        *now.borrow_mut() += Duration::from_secs(1);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn capacity_gt_one() {
        let now = Rc::new(RefCell::new(Instant::now()));
        let now_moved = now.clone();

        let mut bucket = TokenBucket::new(3, Duration::from_secs(1))
            .with_timer(Box::new(move || *now_moved.borrow()));

        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        *now.borrow_mut() += Duration::from_secs(1);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn period_gt_one() {
        let now = Rc::new(RefCell::new(Instant::now()));
        let now_moved = now.clone();

        let mut bucket = TokenBucket::new(1, Duration::from_secs(3))
            .with_timer(Box::new(move || *now_moved.borrow()));

        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        *now.borrow_mut() += Duration::from_secs(2);
        assert_eq!(bucket.consume(1), false);

        *now.borrow_mut() += Duration::from_secs(3);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn consume_over_time() {
        let t0 = Instant::now();
        let now = Rc::new(RefCell::new(t0.clone()));
        let now_moved = now.clone();

        let mut bucket = TokenBucket::new(4, Duration::from_secs(1))
            .with_timer(Box::new(move || *now_moved.borrow()));

        // consume first token
        *now.borrow_mut() = t0;
        assert_eq!(bucket.consume(1), true);

        // consume second token
        *now.borrow_mut() = t0 + Duration::from_millis(50);
        assert_eq!(bucket.consume(1), true);

        // consume third & fourth tokens
        *now.borrow_mut() = t0 + Duration::from_millis(150);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);

        // ensure we are out of tokens
        assert_eq!(bucket.consume(1), false);

        // one token is not yet replenished
        *now.borrow_mut() = t0 + Duration::from_millis(249);
        assert_eq!(bucket.consume(1), false);

        // one token is replenished
        *now.borrow_mut() = t0 + Duration::from_millis(250);
        assert_eq!(bucket.consume(1), true);

        // ensure we are out of tokens again
        assert_eq!(bucket.consume(1), false);

        // two tokens are replenished
        *now.borrow_mut() = t0 + Duration::from_millis(750);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }

    #[test]
    fn weight_gt_one() {
        let now = Rc::new(RefCell::new(Instant::now()));
        let now_moved = now.clone();

        let mut bucket = TokenBucket::new(3, Duration::from_secs(1))
            .with_timer(Box::new(move || *now_moved.borrow()));

        // consume all tokens at once
        assert_eq!(bucket.consume(3), true);
        assert_eq!(bucket.consume(1), false);

        // sequentially consume tokens
        *now.borrow_mut() += Duration::from_secs(1);
        assert_eq!(bucket.consume(2), true);
        assert_eq!(bucket.consume(2), false);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);

        // two tokens are replenished
        *now.borrow_mut() += Duration::from_millis(700);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), true);
        assert_eq!(bucket.consume(1), false);
    }
}
