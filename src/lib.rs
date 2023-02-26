use std::time::{Duration, Instant};

// QUESTIONS:
// * Do we want to separate capacity from replenish rate?
pub struct TokenBucket {
    capacity: usize,
    period: Duration,
    tokens: usize,
    last_replenished_at: Option<Instant>,
    clock: Box<dyn Fn() -> Instant>,
}

impl TokenBucket {
    pub fn new(capacity: usize, period: Duration) -> Self {
        Self {
            capacity,
            period,
            tokens: capacity,
            last_replenished_at: None,
            clock: Box::new(|| Instant::now()),
        }
    }

    pub fn with_timer(mut self, clock: Box<dyn Fn() -> Instant>) -> Self {
        self.clock = clock;
        self
    }

    pub fn consume(&mut self) -> bool {
        self.consume_weight(1)
    }

    pub fn consume_weight(&mut self, weight: usize) -> bool {
        let now = (self.clock)();
        let last_replenished_at = self.last_replenished_at.unwrap_or(now.clone());
        let tokens_to_replenish = (now.duration_since(last_replenished_at).as_secs_f64()
            / self.period.as_secs_f64()
            * self.capacity as f64) as usize;

        dbg!(now);
        dbg!(last_replenished_at);
        dbg!(now.duration_since(last_replenished_at));

        // last_replenished_at стає новіше ніж now o_O
        self.last_replenished_at =
            Some(last_replenished_at + self.period.saturating_mul(tokens_to_replenish as u32));
        self.tokens = std::cmp::min(
            self.tokens.saturating_add(tokens_to_replenish),
            self.capacity,
        );

        match self.tokens.checked_sub(weight) {
            Some(new_tokens) => {
                self.tokens = new_tokens;
                true
            }
            None => false,
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

        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), false);

        *now.borrow_mut() += Duration::from_secs(1);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), false);
    }

    #[test]
    fn capacity_gt_one() {
        let now = Rc::new(RefCell::new(Instant::now()));
        let now_moved = now.clone();

        let mut bucket = TokenBucket::new(3, Duration::from_secs(1))
            .with_timer(Box::new(move || *now_moved.borrow()));

        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), false);

        *now.borrow_mut() += Duration::from_secs(1);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), false);
    }

    #[test]
    fn period_gt_one() {
        let now = Rc::new(RefCell::new(Instant::now()));
        let now_moved = now.clone();

        let mut bucket = TokenBucket::new(1, Duration::from_secs(3))
            .with_timer(Box::new(move || *now_moved.borrow()));

        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), false);

        *now.borrow_mut() += Duration::from_secs(3);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), false);
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
        assert_eq!(bucket.consume(), true);

        // consume second token
        *now.borrow_mut() = t0 + Duration::from_millis(50);
        assert_eq!(bucket.consume(), true);

        // consume third & fourth tokens
        *now.borrow_mut() = t0 + Duration::from_millis(150);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), true);

        // ensure we are out of tokens
        assert_eq!(bucket.consume(), false);

        // one token is not yet replenished
        *now.borrow_mut() = t0 + Duration::from_millis(249);
        assert_eq!(bucket.consume(), false);

        // one token is replenished
        *now.borrow_mut() = t0 + Duration::from_millis(250);
        assert_eq!(bucket.consume(), true);

        // ensure we are out of tokens again
        assert_eq!(bucket.consume(), false);

        // two tokens are replenished
        *now.borrow_mut() = t0 + Duration::from_millis(750);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), true);
        assert_eq!(bucket.consume(), false);
    }
}
