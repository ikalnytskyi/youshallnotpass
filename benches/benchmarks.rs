use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use youshallnotpass::TokenBucket;

pub fn tokenbucket_consume(c: &mut Criterion) {
    let bucket = TokenBucket::new(10, Duration::from_secs(600));
    c.bench_function("TokenBucket::consume(1)", |b| {
        b.iter(|| bucket.consume(black_box(1)))
    });
}

criterion_group!(benches, tokenbucket_consume);
criterion_main!(benches);
