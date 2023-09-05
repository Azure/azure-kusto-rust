use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

/// Wrapper around a value that allows for storing when the value was last updated,
/// as well as the period after which it should be refreshed (i.e. expired)
#[derive(Debug, Clone)]
pub struct Cached<T> {
    inner: T,
    last_updated: Instant,
    refresh_period: Duration,
}

impl<T> Cached<T> {
    pub fn new(inner: T, refresh_period: Duration) -> Self {
        Self {
            inner,
            last_updated: Instant::now(),
            refresh_period,
        }
    }

    pub fn get(&self) -> &T {
        &self.inner
    }

    pub fn is_expired(&self) -> bool {
        self.last_updated.elapsed() >= self.refresh_period
    }

    pub fn update(&mut self, inner: T) {
        self.inner = inner;
        self.last_updated = Instant::now();
    }
}

pub type ThreadSafeCachedValue<T> = Arc<RwLock<Cached<T>>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_cached_get() {
        let value = "hello";
        let cached_string = Cached::new(value.to_string(), Duration::from_secs(60));

        assert_eq!(cached_string.get(), value);
    }

    #[test]
    fn test_cached_is_expired() {
        let value = "hello";
        let mut cached_string = Cached::new(value.to_string(), Duration::from_secs(60));

        assert!(!cached_string.is_expired());

        cached_string.last_updated = Instant::now() - Duration::from_secs(61);

        assert!(cached_string.is_expired());
    }

    #[test]
    fn test_cached_update() {
        let value = "hello";
        let mut cached_string = Cached::new(value.to_string(), Duration::from_secs(60));

        assert_eq!(cached_string.get(), value);

        let new_value = "world";
        cached_string.update(new_value.to_string());

        assert!(!cached_string.is_expired());
        assert_eq!(cached_string.get(), new_value);
    }
}
