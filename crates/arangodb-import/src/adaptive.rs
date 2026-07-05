//! Rate-limit-aware concurrency governor for the import sender pool (PRD §11.3).
//!
//! The sender pool starts at its configured worker count and treats two signals
//! as back-pressure from the server:
//!
//! - a send that returns a rate-limit status (429) or an availability status
//!   (502/503/504) after retries were exhausted, and
//! - a send whose round trip exceeds [`AdaptiveConfig::slow_threshold`] (a
//!   proxy for a server that is retrying 429/503 internally, since those
//!   backoffs inflate the observed latency).
//!
//! On either signal the governor multiplicatively halves the number of
//! concurrent in-flight sends (down to [`AdaptiveConfig::min_concurrency`]);
//! after [`AdaptiveConfig::recover_after`] with no further congestion it grows
//! the limit back one slot at a time. Throughput therefore stays positive under
//! load instead of collapsing into a retry storm.
//!
//! The governor is an *additional* gate on top of the global in-flight-byte
//! semaphore; when the effective limit equals the worker count it is a no-op,
//! so a healthy import behaves exactly as before.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

/// Default round-trip time above which a single send is treated as congestion.
pub const DEFAULT_SLOW_THRESHOLD: Duration = Duration::from_secs(5);

/// Default quiet period the governor waits before growing the limit by one.
pub const DEFAULT_RECOVER_AFTER: Duration = Duration::from_secs(10);

/// Tuning for the [`AdaptiveLimiter`].
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// When `false`, the limit is pinned at `max_concurrency` (no throttling);
    /// metrics are still collected.
    pub enabled: bool,
    /// Upper bound on concurrent sends (normally the worker count).
    pub max_concurrency: usize,
    /// Floor the limit never drops below.
    pub min_concurrency: usize,
    /// A send slower than this counts as a congestion signal.
    pub slow_threshold: Duration,
    /// Time without congestion before the limit grows by one slot.
    pub recover_after: Duration,
}

impl AdaptiveConfig {
    /// Builds a config for `workers` senders with the default thresholds.
    #[must_use]
    pub fn new(enabled: bool, workers: usize) -> Self {
        Self {
            enabled,
            max_concurrency: workers.max(1),
            min_concurrency: 1,
            slow_threshold: DEFAULT_SLOW_THRESHOLD,
            recover_after: DEFAULT_RECOVER_AFTER,
        }
    }
}

/// A point-in-time snapshot of what the governor observed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchingMetrics {
    /// The effective concurrency limit at the time of the snapshot.
    pub final_concurrency: usize,
    /// The lowest limit the governor ever dropped to.
    pub min_concurrency_seen: usize,
    /// Sends that returned HTTP 429 after retries were exhausted.
    pub rate_limited_429: u64,
    /// Sends that returned HTTP 502/503/504 after retries were exhausted.
    pub rate_limited_503: u64,
    /// Sends whose latency exceeded the slow threshold.
    pub slow_sends: u64,
    /// Mean successful-send round-trip time, in milliseconds.
    pub avg_rtt_ms: u64,
}

#[derive(Debug)]
struct Inner {
    limit: usize,
    in_flight: usize,
    last_congestion: Instant,
    min_limit_seen: usize,
    rate_limited_429: u64,
    rate_limited_503: u64,
    slow_sends: u64,
    rtt_sum: Duration,
    rtt_count: u64,
}

/// A resizable concurrency limiter driven by send outcomes.
#[derive(Debug)]
pub struct AdaptiveLimiter {
    config: AdaptiveConfig,
    inner: Mutex<Inner>,
    /// Woken whenever a slot frees or the limit grows.
    slot_available: Notify,
}

impl AdaptiveLimiter {
    /// Creates a limiter starting at `max_concurrency`.
    #[must_use]
    pub fn new(config: AdaptiveConfig) -> Arc<Self> {
        let start = config.max_concurrency.max(config.min_concurrency).max(1);
        Arc::new(Self {
            inner: Mutex::new(Inner {
                limit: start,
                in_flight: 0,
                last_congestion: Instant::now(),
                min_limit_seen: start,
                rate_limited_429: 0,
                rate_limited_503: 0,
                slow_sends: 0,
                rtt_sum: Duration::ZERO,
                rtt_count: 0,
            }),
            slot_available: Notify::new(),
            config,
        })
    }

    /// Waits until an in-flight slot is available, then claims it.
    ///
    /// The caller MUST pair each `acquire` with exactly one [`record_success`]
    /// or [`record_error`] to release the slot.
    ///
    /// [`record_success`]: AdaptiveLimiter::record_success
    /// [`record_error`]: AdaptiveLimiter::record_error
    pub async fn acquire(&self) {
        loop {
            // Register interest *before* checking, so a release between the
            // check and the await cannot be missed.
            let notified = self.slot_available.notified();
            {
                let mut inner = self.inner.lock().await;
                if inner.in_flight < inner.limit {
                    inner.in_flight += 1;
                    return;
                }
            }
            notified.await;
        }
    }

    /// Releases a slot after a successful send with round-trip time `rtt`.
    pub async fn record_success(&self, rtt: Duration) {
        let mut inner = self.inner.lock().await;
        inner.in_flight = inner.in_flight.saturating_sub(1);
        inner.rtt_sum += rtt;
        inner.rtt_count += 1;
        if self.config.enabled && rtt >= self.config.slow_threshold {
            inner.slow_sends += 1;
            self.throttle(&mut inner);
        } else {
            self.maybe_recover(&mut inner);
        }
        drop(inner);
        self.slot_available.notify_waiters();
    }

    /// Releases a slot after a failed send, throttling on rate-limit/availability
    /// statuses. `status` is the HTTP status when the error carried one.
    pub async fn record_error(&self, status: Option<u16>) {
        let mut inner = self.inner.lock().await;
        inner.in_flight = inner.in_flight.saturating_sub(1);
        match status {
            Some(429) => {
                inner.rate_limited_429 += 1;
                self.throttle(&mut inner);
            }
            Some(502..=504) => {
                inner.rate_limited_503 += 1;
                self.throttle(&mut inner);
            }
            _ => {}
        }
        drop(inner);
        self.slot_available.notify_waiters();
    }

    /// Halves the limit (down to the floor) and records a congestion timestamp.
    fn throttle(&self, inner: &mut Inner) {
        if !self.config.enabled {
            return;
        }
        let reduced = (inner.limit / 2).max(self.config.min_concurrency);
        inner.limit = reduced;
        inner.min_limit_seen = inner.min_limit_seen.min(reduced);
        inner.last_congestion = Instant::now();
    }

    /// Grows the limit by one slot when the recovery window has elapsed.
    fn maybe_recover(&self, inner: &mut Inner) {
        if !self.config.enabled || inner.limit >= self.config.max_concurrency {
            return;
        }
        if inner.last_congestion.elapsed() >= self.config.recover_after {
            inner.limit += 1;
            // Reset the clock so growth is paced one slot per window.
            inner.last_congestion = Instant::now();
            self.slot_available.notify_waiters();
        }
    }

    /// Returns the current effective concurrency limit (for tests/metrics).
    pub async fn current_limit(&self) -> usize {
        self.inner.lock().await.limit
    }

    /// Snapshots the observed metrics.
    pub async fn metrics(&self) -> BatchingMetrics {
        let inner = self.inner.lock().await;
        let avg_rtt_ms = (inner.rtt_sum.as_millis() as u64)
            .checked_div(inner.rtt_count)
            .unwrap_or(0);
        BatchingMetrics {
            final_concurrency: inner.limit,
            min_concurrency_seen: inner.min_limit_seen,
            rate_limited_429: inner.rate_limited_429,
            rate_limited_503: inner.rate_limited_503,
            slow_sends: inner.slow_sends,
            avg_rtt_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(enabled: bool) -> AdaptiveConfig {
        AdaptiveConfig {
            enabled,
            max_concurrency: 8,
            min_concurrency: 1,
            // Tiny windows so tests do not sleep.
            slow_threshold: Duration::from_millis(50),
            recover_after: Duration::from_millis(0),
        }
    }

    #[tokio::test]
    async fn starts_at_max_and_is_noop_when_disabled() {
        let limiter = AdaptiveLimiter::new(config(false));
        assert_eq!(limiter.current_limit().await, 8);
        // A rate-limit error must not reduce the limit while disabled.
        limiter.acquire().await;
        limiter.record_error(Some(429)).await;
        assert_eq!(limiter.current_limit().await, 8);
    }

    #[tokio::test]
    async fn rate_limit_halves_concurrency_down_to_floor() {
        let limiter = AdaptiveLimiter::new(config(true));
        for expected in [4usize, 2, 1, 1] {
            limiter.acquire().await;
            limiter.record_error(Some(503)).await;
            assert_eq!(limiter.current_limit().await, expected);
        }
        let metrics = limiter.metrics().await;
        assert_eq!(metrics.rate_limited_503, 4);
        assert_eq!(metrics.min_concurrency_seen, 1);
    }

    #[tokio::test]
    async fn slow_send_throttles_then_recovers() {
        let limiter = AdaptiveLimiter::new(config(true));
        limiter.acquire().await;
        limiter.record_success(Duration::from_millis(100)).await; // slow
        assert_eq!(limiter.current_limit().await, 4);
        assert_eq!(limiter.metrics().await.slow_sends, 1);

        // recover_after is zero, so each fast send grows the limit by one.
        for expected in [5usize, 6, 7, 8, 8] {
            limiter.acquire().await;
            limiter.record_success(Duration::from_millis(1)).await;
            assert_eq!(limiter.current_limit().await, expected);
        }
    }

    #[tokio::test]
    async fn non_rate_limit_error_does_not_throttle() {
        let limiter = AdaptiveLimiter::new(config(true));
        limiter.acquire().await;
        limiter.record_error(Some(400)).await;
        assert_eq!(limiter.current_limit().await, 8);
    }

    #[tokio::test]
    async fn acquire_blocks_until_slot_released() {
        let mut cfg = config(true);
        cfg.max_concurrency = 1;
        let limiter = AdaptiveLimiter::new(cfg);
        limiter.acquire().await; // fills the only slot

        let other = Arc::clone(&limiter);
        let waiter = tokio::spawn(async move { other.acquire().await });

        // The second acquire cannot complete until the slot is released.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        limiter.record_success(Duration::from_millis(1)).await;
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should finish after release")
            .unwrap();
    }
}
