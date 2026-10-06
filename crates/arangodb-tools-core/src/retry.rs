//! Retry classification and a backoff-driven retry helper.
//!
//! A single retry policy is intended to wrap *all* HTTP operations so no code
//! path can silently skip retries (a weakness of the reference C++ tools).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::Error;

/// Classifies whether an error is worth retrying.
pub trait Retryable {
    /// Returns `true` if the operation that produced this error may succeed on
    /// a subsequent attempt.
    fn is_retryable(&self) -> bool;

    /// Returns `true` if the server answered and reported the error, as
    /// opposed to a transport or local failure.
    ///
    /// Defaults to `false` so existing implementors keep compiling; counting
    /// nothing is the honest default for an error type that cannot tell.
    fn is_server_error(&self) -> bool {
        false
    }
}

impl Retryable for Error {
    fn is_server_error(&self) -> bool {
        matches!(self, Error::Http { .. })
    }

    fn is_retryable(&self) -> bool {
        match self {
            Error::Connection(_) => true,
            Error::Io(err) => matches!(
                err.kind(),
                std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::WouldBlock
            ),
            // 429 Too Many Requests and 5xx gateway/availability statuses are
            // transient; other statuses (including most 4xx) are not.
            Error::Http { status, .. } => {
                matches!(status, 429 | 502 | 503 | 504)
            }
            _ => false,
        }
    }
}

/// What the retry loop actually did, shared across cloned policies.
///
/// `ProgressSnapshot` has always carried `retries` and `server_errors`, but
/// nothing incremented them, so every progress event ever emitted reported a
/// hard `0` for both. They were the shape of a measurement without being one.
/// A policy carrying one of these makes them real.
#[derive(Debug, Default)]
pub struct RetryStats {
    /// Retry attempts made (the first attempt is not a retry).
    pub retries: AtomicU64,
    /// Errors the server itself reported, whether or not they were retried.
    pub server_errors: AtomicU64,
}

impl RetryStats {
    /// Creates zeroed stats.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Retry attempts recorded so far.
    #[must_use]
    pub fn retries(&self) -> u64 {
        self.retries.load(Ordering::Relaxed)
    }

    /// Server-reported errors recorded so far.
    #[must_use]
    pub fn server_errors(&self) -> u64 {
        self.server_errors.load(Ordering::Relaxed)
    }
}

/// Configuration for exponential backoff with optional full jitter.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first).
    pub max_attempts: u32,
    /// Base delay used for the first backoff interval.
    pub base_delay: Duration,
    /// Upper bound on any single backoff interval.
    pub max_delay: Duration,
    /// Growth factor applied to the backoff each attempt (e.g. `2.0` doubles).
    /// Values `<= 1.0` disable growth (every interval is `base_delay`).
    pub multiplier: f64,
    /// Whether to apply full jitter to the backoff interval.
    pub jitter: bool,
    /// Optional shared counters recording what this policy's retries did.
    pub stats: Option<Arc<RetryStats>>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
            stats: None,
        }
    }
}

impl RetryPolicy {
    /// Computes the backoff delay before the given 1-based `attempt`.
    ///
    /// Uses exponential growth (`base * multiplier^(attempt-1)`) capped at
    /// `max_delay`, with full jitter applied when [`RetryPolicy::jitter`] is
    /// set.
    #[must_use]
    pub fn backoff(&self, attempt: u32) -> Duration {
        let capped = self.uncapped_backoff(attempt).min(self.max_delay);
        if self.jitter {
            let nanos = capped.as_nanos().min(u128::from(u64::MAX)) as u64;
            let bound = nanos.max(1);
            Duration::from_nanos(pseudo_random() % bound)
        } else {
            capped
        }
    }

    /// The exponentially grown (but not yet jittered) delay, before the
    /// `max_delay` cap is applied.
    fn uncapped_backoff(&self, attempt: u32) -> Duration {
        // A multiplier of exactly 2.0 over integer-nanosecond base delays is
        // represented exactly by `f64` for all realistic values, so this stays
        // precise for the common case while supporting arbitrary factors.
        let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        let factor = self.multiplier.max(1.0).powi(exponent);
        let base_nanos = self.base_delay.as_nanos() as f64;
        let raw = base_nanos * factor;
        if !raw.is_finite() || raw >= u64::MAX as f64 {
            self.max_delay
        } else {
            Duration::from_nanos(raw as u64)
        }
    }
}

/// Runs `op`, retrying on retryable errors according to `policy`.
///
/// Returns the first successful value, or the last error once attempts are
/// exhausted or a non-retryable error is encountered.
pub async fn retry<T, E, F, Fut>(policy: &RetryPolicy, mut op: F) -> Result<T, E>
where
    E: Retryable + std::fmt::Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if let Some(stats) = &policy.stats {
                    if err.is_server_error() {
                        stats.server_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if attempt >= policy.max_attempts || !err.is_retryable() {
                    if attempt > 1 {
                        tracing::warn!(
                            attempts = attempt,
                            error = ?err,
                            "giving up after retryable failures"
                        );
                    }
                    return Err(err);
                }
                if let Some(stats) = &policy.stats {
                    stats.retries.fetch_add(1, Ordering::Relaxed);
                }
                let delay = policy.backoff(attempt);
                tracing::debug!(
                    attempt,
                    max_attempts = policy.max_attempts,
                    delay_ms = delay.as_millis() as u64,
                    error = ?err,
                    "retrying after transient error"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Cheap, dependency-free pseudo-random source for jitter only.
///
/// This is not cryptographically secure and is used solely to spread out
/// retry timing; correctness never depends on its output.
fn pseudo_random() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    // SplitMix64 finalizer.
    let mut x = seed ^ 0x9E37_79B9_7F4A_7C15;
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    /// A policy with stats records every retry it performs.
    ///
    /// `ProgressSnapshot.retries` existed long before anything incremented it,
    /// so this pins the wiring rather than the arithmetic: without the
    /// recording in the loop these assertions read zero, which is exactly the
    /// bug they exist to prevent returning.
    #[tokio::test]
    async fn retry_stats_record_attempts_and_server_errors() {
        let stats = Arc::new(RetryStats::new());
        let policy = RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            jitter: false,
            stats: Some(Arc::clone(&stats)),
            ..RetryPolicy::default()
        };

        let attempts = std::sync::atomic::AtomicU64::new(0);
        let result: Result<(), Error> = retry(&policy, || async {
            // 503 is retryable and server-reported.
            attempts.fetch_add(1, Ordering::Relaxed);
            Err(Error::http(503, "unavailable", crate::ErrorContext::new()))
        })
        .await;

        assert!(result.is_err(), "all attempts fail");
        assert_eq!(attempts.load(Ordering::Relaxed), 4, "four attempts made");
        assert_eq!(stats.retries(), 3, "three of the four were retries");
        assert_eq!(
            stats.server_errors(),
            4,
            "every failure was server-reported"
        );
    }

    /// A transport failure is counted as a retry but not as a server error:
    /// the server never answered.
    #[tokio::test]
    async fn transport_failures_are_not_counted_as_server_errors() {
        let stats = Arc::new(RetryStats::new());
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            jitter: false,
            stats: Some(Arc::clone(&stats)),
            ..RetryPolicy::default()
        };

        let result: Result<(), Error> = retry(&policy, || async {
            Err(Error::Connection("refused".to_string()))
        })
        .await;

        assert!(result.is_err());
        assert_eq!(stats.retries(), 2, "two retries after the first attempt");
        assert_eq!(
            stats.server_errors(),
            0,
            "a connection failure is not a server-reported error"
        );
    }

    /// A call that succeeds first time records nothing.
    #[tokio::test]
    async fn a_clean_run_records_no_retries() {
        let stats = Arc::new(RetryStats::new());
        let policy = RetryPolicy {
            stats: Some(Arc::clone(&stats)),
            ..RetryPolicy::default()
        };
        let result: Result<u8, Error> = retry(&policy, || async { Ok(7) }).await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(stats.retries(), 0);
        assert_eq!(stats.server_errors(), 0);
    }

    /// Stats are shared across clones, so a policy handed to a client still
    /// reports into the caller's counters.
    #[test]
    fn stats_survive_cloning_the_policy() {
        let stats = Arc::new(RetryStats::new());
        let policy = RetryPolicy {
            stats: Some(Arc::clone(&stats)),
            ..RetryPolicy::default()
        };
        let clone = policy.clone();
        clone
            .stats
            .as_ref()
            .expect("clone keeps stats")
            .retries
            .fetch_add(5, Ordering::Relaxed);
        assert_eq!(
            stats.retries(),
            5,
            "the clone writes into the same counters"
        );
    }

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug)]
    struct Transient;
    impl Retryable for Transient {
        fn is_retryable(&self) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct Fatal;
    impl Retryable for Fatal {
        fn is_retryable(&self) -> bool {
            false
        }
    }

    fn no_jitter() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            multiplier: 2.0,
            jitter: false,
            stats: None,
        }
    }

    #[tokio::test]
    async fn succeeds_after_transient_failures() {
        let calls = AtomicU32::new(0);
        let result: Result<u32, Transient> = retry(&no_jitter(), || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(Transient)
                } else {
                    Ok(42)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn stops_immediately_on_non_retryable() {
        let calls = AtomicU32::new(0);
        let result: Result<(), Fatal> = retry(&RetryPolicy::default(), || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(Fatal) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exhausts_attempts() {
        let calls = AtomicU32::new(0);
        let result: Result<(), Transient> = retry(&no_jitter(), || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(Transient) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn backoff_grows_and_caps() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: false,
            stats: None,
        };
        assert_eq!(policy.backoff(1), Duration::from_millis(100));
        assert_eq!(policy.backoff(2), Duration::from_millis(200));
        assert_eq!(policy.backoff(3), Duration::from_millis(400));
        assert!(policy.backoff(20) <= Duration::from_secs(1));
    }

    #[test]
    fn multiplier_controls_growth() {
        let tripling = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(60),
            multiplier: 3.0,
            jitter: false,
            stats: None,
        };
        assert_eq!(tripling.backoff(1), Duration::from_millis(10));
        assert_eq!(tripling.backoff(2), Duration::from_millis(30));
        assert_eq!(tripling.backoff(3), Duration::from_millis(90));

        // A multiplier at or below 1.0 keeps every interval at the base delay.
        let flat = RetryPolicy {
            multiplier: 1.0,
            jitter: false,
            ..tripling
        };
        assert_eq!(flat.backoff(1), Duration::from_millis(10));
        assert_eq!(flat.backoff(5), Duration::from_millis(10));
    }

    #[test]
    fn jitter_stays_within_bound() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: true,
            stats: None,
        };
        for _ in 0..1000 {
            assert!(policy.backoff(20) <= Duration::from_secs(1));
        }
    }

    #[test]
    fn http_status_classification() {
        assert!(Error::http(503, "x", crate::ErrorContext::new()).is_retryable());
        assert!(Error::http(429, "x", crate::ErrorContext::new()).is_retryable());
        assert!(!Error::http(404, "x", crate::ErrorContext::new()).is_retryable());
        assert!(!Error::config("bad").is_retryable());
        assert!(Error::connection("reset").is_retryable());
    }
}
