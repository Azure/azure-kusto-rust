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

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn is_expired(&self) -> bool {
        self.last_updated.elapsed() > self.refresh_period
    }

    pub fn update(&mut self, inner: T) {
        self.inner = inner;
        self.last_updated = Instant::now();
    }
}

pub type Refreshing<T> = Arc<RwLock<Cached<T>>>;
