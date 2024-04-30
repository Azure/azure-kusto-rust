use std::{
    error::Error,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use async_lock::RwLock;

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

#[derive(Debug, Clone)]
pub struct ThreadSafeCachedValue<T>
where
    T: Clone,
{
    cache: Arc<RwLock<Cached<Option<T>>>>,
}

impl<T: Clone> ThreadSafeCachedValue<T> {
    pub fn new(refresh_period: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(Cached::new(None, refresh_period))),
        }
    }

    /// Fetches the latest value, either retrieving from cache if valid, or by executing the callback
    pub async fn get<F, E: Error>(&self, callback: F) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        // First, try to get a value from the cache by obtaining a read lock
        {
            let cache = self.cache.read().await;
            if !cache.is_expired() {
                if let Some(cached_value) = cache.get() {
                    return Ok(cached_value.clone());
                }
            }
        }

        // Obtain a write lock to refresh the cached value
        let mut cache = self.cache.write().await;

        // Again attempt to return from cache, check is done in case another thread
        // refreshed the cached value while we were waiting on the write lock and its now valid
        if !cache.is_expired() {
            if let Some(cached_value) = cache.get() {
                return Ok(cached_value.clone());
            }
        }

        // Fetch new value by executing the callback, update the cache, and return the value
        let fetched_value = callback.await?;
        cache.update(Some(fetched_value.clone()));

        Ok(fetched_value)
    }
}

#[cfg(test)]
mod cached_tests {
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

#[cfg(test)]
mod thread_safe_cached_value_tests {
    use super::*;
    use std::{fmt::Error, sync::Mutex};

    #[derive(Debug)]
    struct MockToken {
        get_token_call_count: Mutex<usize>,
    }

    impl MockToken {
        fn new() -> Self {
            Self {
                get_token_call_count: Mutex::new(0),
            }
        }

        async fn get_new_token(&self) -> Result<usize, Error> {
            // Include an incrementing counter in the token to track how many times the token has been refreshed
            let mut call_count = self.get_token_call_count.lock().unwrap();
            *call_count += 1;
            Ok(call_count.clone())
        }
    }

    #[tokio::test]
    async fn returns_same_value_if_unexpired() -> Result<(), Error> {
        let cache = ThreadSafeCachedValue::new(Duration::from_secs(300));
        let mock_token = MockToken::new();

        let token1 = cache.get(mock_token.get_new_token()).await?;
        let token2 = cache.get(mock_token.get_new_token()).await?;

        assert_eq!(token1, 1);
        assert_eq!(token2, 1);
        Ok(())
    }

    #[tokio::test]
    async fn returns_new_value_if_expired() -> Result<(), Error> {
        let cache = ThreadSafeCachedValue::new(Duration::from_millis(1));
        let mock_token = MockToken::new();

        let token1 = cache.get(mock_token.get_new_token()).await?;
        // Sleep to ensure the token expires
        tokio::time::sleep(Duration::from_secs(1)).await;
        let token2 = cache.get(mock_token.get_new_token()).await?;

        assert_eq!(token1, 1);
        assert_eq!(token2, 2);
        Ok(())
    }
}
