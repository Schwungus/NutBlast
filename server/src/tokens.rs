use std::time::Instant;

use crate::protocol::payloads::Kick;

pub struct TokenBucket {
    tokens: f32,
    last_refill: Instant,
    rate: f32,
    max: f32,
}

impl TokenBucket {
    pub fn new(rate: f32, max: f32, burst: f32) -> Self {
        Self {
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
        self.tokens = (self.tokens + dt * self.rate).min(self.max);

        if self.tokens < count {
            return Err(Kick::violation("rate_limited", "Bandwidth patrol!"));
        }

        self.tokens -= count;
        Ok(())
    }
}
