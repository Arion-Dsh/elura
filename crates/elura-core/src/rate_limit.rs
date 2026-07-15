use std::time::Instant;

#[derive(Debug, Clone)]
pub struct TokenBucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    updated_at: Instant,
}

impl TokenBucket {
    pub fn new(rate_per_second: u32, burst: u32) -> Self {
        let capacity = burst.max(1) as f64;
        Self {
            rate: rate_per_second as f64,
            capacity,
            tokens: capacity,
            updated_at: Instant::now(),
        }
    }

    pub fn allow(&mut self) -> bool {
        self.allow_n(1)
    }

    /// Attempts to consume `amount` tokens in one operation.
    ///
    /// This is useful for byte-oriented limits without looping once per byte.
    pub fn allow_n(&mut self, amount: u32) -> bool {
        if self.rate == 0.0 {
            return true;
        }
        let now = Instant::now();
        self.tokens = (self.tokens + now.duration_since(self.updated_at).as_secs_f64() * self.rate)
            .min(self.capacity);
        self.updated_at = now;
        let amount = amount as f64;
        if self.tokens < amount {
            return false;
        }
        self.tokens -= amount;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_consumption_is_atomic() {
        let mut bucket = TokenBucket::new(1, 10);
        assert!(bucket.allow_n(7));
        assert!(!bucket.allow_n(4));
        assert!(bucket.allow_n(3));
    }
}
