use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::RwLock;

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

    pub fn get_last_updated(&self) -> &Instant {
        &self.last_updated
    }

    pub fn is_expired(&self) -> bool {
        self.last_updated.elapsed() > self.refresh_period
    }

    pub fn update(&mut self, inner: T) {
        self.inner = inner;
        self.last_updated = Instant::now();
    }

    pub fn update_with_time(&mut self, inner: T, last_updated: Instant) {
        self.inner = inner;
        self.last_updated = last_updated;
    }
}

pub type Refreshing<T> = Arc<RwLock<Cached<T>>>;
