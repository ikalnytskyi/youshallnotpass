<p align="center">
  <img
    width="350"
    src="https://raw.githubusercontent.com/ikalnytskyi/youshallnotpass/readme/assets/logo.jpg"
    alt="YouShallNotPass — the rate limiter"
  />
</p>

<a href="https://savelife.in.ua/en/donate-en/">
  <img
    src="https://raw.githubusercontent.com/ikalnytskyi/youshallnotpass/readme/assets/banner.svg"
    alt="Save Lives in Ukraine"
  />
</a>

## The Rate Limiter

YouShallNotPass is a thread-safe, rate-limiting library for the Rust
programming language. It's framework agnostic and can be manually integrated
into applications. The token bucket algorithm is used under the hood to control
how many events may happen with a given period of time.


## Usage

```rust
use std::time::Duration;
use youshallnotpass::{RateLimiter, Error};

// Create the rate limiter instance with rate limiting rules (aka buckets).
// Each bucket controls how many tokens (aka events) can occur within a given
// period of time.
let limiter = RateLimiter::configure()
    .limit("A", 2, Duration::from_secs(60))
    .limit("B", 3, Duration::from_secs(60))
    .done();

// Consume a given number of tokens from a bucket. In most cases you want to
// consume just one token because you're rate limiting events as they come.
assert_eq!(limiter.consume("A", 1), Ok(()));

// When an event is allowed to occur, Ok() is returned.
assert_eq!(limiter.consume("A", 1), Ok(()));

// When an event is forbidden to occur, Error:RetryAfter() is returned. The
// latter contains a Duration instance to wait before the forbidden event is
// allowed to happen again.
assert!(matches!(limiter.consume("A", 1), Err(Error::RetryAfter(_))));
assert!(matches!(limiter.consume("B", 5), Err(Error::RetryAfter(_))));
```
