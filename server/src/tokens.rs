use std::time::Instant;

pub struct TokenBucket {
    tokens: f32,
    last_refill: Instant,
    rate: f32,
    burst: f32,
}

impl TokenBucket {
    pub fn new(rate: f32, burst: f32) -> Self {
        Self {
            tokens: burst,
            last_refill: Instant::now(),
            rate,
            burst,
        }
    }

    pub fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let dt = now.duration_since(self.last_refill).as_secs_f32();
        self.last_refill = now;
        self.tokens = (self.tokens + dt * self.rate).min(self.burst);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}
