use std::time::Instant;

use crate::protocol::payloads::Kick;

#[derive(Clone)]
pub struct TokenBucket {
    identifier: &'static str,
    tokens: f32,
    last_refill: Instant,
    rate: f32,
    max: f32,
}

impl TokenBucket {
    pub fn new_metadata(identifier: &'static str) -> Self {
        Self::new(identifier, 4096.0, 8192.0, 8192.0)
    }

    pub fn new(identifier: &'static str, rate: f32, max: f32, burst: f32) -> Self {
        Self {
            identifier,
            tokens: burst,
            last_refill: Instant::now(),
            rate,
            max,
        }
    }

    pub fn try_take(&mut self, count: usize) -> Result<(), Kick> {
        let count = count as f32;

        let now = Instant::now();
        let dt = now.duration_since(self.last_refill).as_secs_f32();
        self.last_refill = now;

        let replenished = (self.tokens + dt * self.rate).min(self.max);

        if self.tokens <= self.max {
            self.tokens = replenished;
        }

        if self.tokens < count {
            return Err(Kick::violation(
                "rate_limited",
                format!("Bandwidth patrol! ({})", self.identifier),
            ));
        }

        self.tokens -= count;

        Ok(())
    }
}
