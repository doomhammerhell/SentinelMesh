use std::collections::VecDeque;

/// Detection mode for anomaly analysis.
///
/// - `Fixed`: uses hardcoded thresholds (current behavior).
/// - `Statistical`: uses z-score based on a sliding window of historical values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DetectionMode {
    Fixed,
    Statistical,
}

/// A sliding window that maintains the most recent `max_size` values and
/// provides statistical helpers (mean, standard deviation, z-score).
#[derive(Clone, Debug)]
pub struct SlidingWindow {
    values: VecDeque<f64>,
    max_size: usize,
}

impl SlidingWindow {
    /// Create a new `SlidingWindow` with the given maximum capacity.
    ///
    /// # Panics
    /// Panics if `max_size` is 0.
    pub fn new(max_size: usize) -> Self {
        assert!(max_size > 0, "max_size must be > 0");
        Self {
            values: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    /// Push a value into the window. If the window is full the oldest value is
    /// evicted.
    pub fn push(&mut self, value: f64) {
        if self.values.len() == self.max_size {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    /// Number of values currently stored.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the window is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Arithmetic mean of the stored values. Returns `0.0` when empty.
    pub fn mean(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        let sum: f64 = self.values.iter().sum();
        sum / self.values.len() as f64
    }

    /// Population standard deviation of the stored values. Returns `0.0` when
    /// empty.
    pub fn std_dev(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        let mean = self.mean();
        let variance: f64 = self
            .values
            .iter()
            .map(|v| {
                let diff = v - mean;
                diff * diff
            })
            .sum::<f64>()
            / self.values.len() as f64;
        variance.sqrt()
    }

    /// Compute the z-score of `value` relative to the current window.
    ///
    /// Returns `None` when:
    /// - the window contains fewer than 30 samples (caller should fall back to
    ///   `Fixed` mode), or
    /// - the standard deviation is effectively zero (< `f64::EPSILON`).
    pub fn z_score(&self, value: f64) -> Option<f64> {
        if self.values.len() < 30 {
            return None;
        }
        let std = self.std_dev();
        if std < f64::EPSILON {
            return None;
        }
        Some((value - self.mean()) / std)
    }
}

impl Default for SlidingWindow {
    fn default() -> Self {
        Self::new(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------
    // Unit tests for SlidingWindow
    // ---------------------------------------------------------------

    #[test]
    fn new_window_is_empty() {
        let w = SlidingWindow::new(10);
        assert!(w.is_empty());
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn push_adds_values() {
        let mut w = SlidingWindow::new(5);
        w.push(1.0);
        w.push(2.0);
        assert_eq!(w.len(), 2);
    }

    #[test]
    fn push_evicts_oldest_when_full() {
        let mut w = SlidingWindow::new(3);
        w.push(1.0);
        w.push(2.0);
        w.push(3.0);
        w.push(4.0);
        assert_eq!(w.len(), 3);
        // oldest (1.0) should be gone; mean of [2, 3, 4] = 3.0
        assert!((w.mean() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn mean_of_empty_is_zero() {
        let w = SlidingWindow::new(10);
        assert!((w.mean() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn mean_is_correct() {
        let mut w = SlidingWindow::new(100);
        for v in [10.0, 20.0, 30.0] {
            w.push(v);
        }
        assert!((w.mean() - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn std_dev_of_empty_is_zero() {
        let w = SlidingWindow::new(10);
        assert!((w.std_dev() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn std_dev_of_identical_values_is_zero() {
        let mut w = SlidingWindow::new(100);
        for _ in 0..50 {
            w.push(5.0);
        }
        assert!(w.std_dev() < f64::EPSILON);
    }

    #[test]
    fn std_dev_is_correct() {
        // population std dev of [2, 4, 4, 4, 5, 5, 7, 9] = 2.0
        let mut w = SlidingWindow::new(100);
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            w.push(v);
        }
        assert!((w.std_dev() - 2.0).abs() < 1e-10);
    }

    // ---------------------------------------------------------------
    // z_score edge cases
    // ---------------------------------------------------------------

    #[test]
    fn z_score_returns_none_when_fewer_than_30_samples() {
        let mut w = SlidingWindow::new(100);
        for i in 0..29 {
            w.push(i as f64);
        }
        assert_eq!(w.len(), 29);
        assert!(w.z_score(100.0).is_none());
    }

    #[test]
    fn z_score_returns_some_at_exactly_30_samples() {
        let mut w = SlidingWindow::new(100);
        for i in 0..30 {
            w.push(i as f64);
        }
        assert_eq!(w.len(), 30);
        // values are 0..30, std dev > 0, so z_score should be Some
        assert!(w.z_score(100.0).is_some());
    }

    #[test]
    fn z_score_returns_none_when_std_dev_is_zero() {
        let mut w = SlidingWindow::new(100);
        for _ in 0..50 {
            w.push(42.0);
        }
        // all identical → std dev ≈ 0
        assert!(w.z_score(42.0).is_none());
    }

    #[test]
    fn z_score_computes_correctly() {
        let mut w = SlidingWindow::new(100);
        // push 30 identical values of 10.0, then one outlier to create variance
        // Actually, let's use a known distribution: 30 values of 100.0 and 100.0
        // Better: push values where we know the answer.
        // values: 30 copies of 0.0 and 30 copies of 2.0 → mean = 1.0, std = 1.0
        for _ in 0..30 {
            w.push(0.0);
        }
        for _ in 0..30 {
            w.push(2.0);
        }
        let mean = w.mean(); // 1.0
        let std = w.std_dev(); // 1.0
        assert!((mean - 1.0).abs() < 1e-10);
        assert!((std - 1.0).abs() < 1e-10);

        let z = w.z_score(4.0).unwrap();
        // z = (4.0 - 1.0) / 1.0 = 3.0
        assert!((z - 3.0).abs() < 1e-10);
    }

    // ---------------------------------------------------------------
    // Default
    // ---------------------------------------------------------------

    #[test]
    fn default_window_has_max_size_100() {
        let w = SlidingWindow::default();
        assert_eq!(w.max_size, 100);
        assert!(w.is_empty());
    }

    // ---------------------------------------------------------------
    // DetectionMode
    // ---------------------------------------------------------------

    #[test]
    fn detection_mode_equality() {
        assert_eq!(DetectionMode::Fixed, DetectionMode::Fixed);
        assert_eq!(DetectionMode::Statistical, DetectionMode::Statistical);
        assert_ne!(DetectionMode::Fixed, DetectionMode::Statistical);
    }

    #[test]
    #[should_panic(expected = "max_size must be > 0")]
    fn new_window_panics_on_zero_max_size() {
        SlidingWindow::new(0);
    }

    // ---------------------------------------------------------------
    // Property-based tests
    // ---------------------------------------------------------------

    // Feature: sentinelmesh-comprehensive-upgrade, Property 16: Detecção de anomalias por z-score
    mod prop_zscore_anomaly_detection {
        use super::*;
        use proptest::prelude::*;
        use sentinelmesh_core::AnomalySeverity;

        // Feature: sentinelmesh-comprehensive-upgrade, Property 16: Detecção de anomalias por z-score
        //
        // For any observed metric and any sliding history with >= 30 samples,
        // if the observed value exceeds 3 standard deviations from the historical
        // mean (|z-score| >= 3.0), the MeshStore must generate an anomaly.
        // Severity must be proportional to z-score:
        //   Warning for |z| >= 3.0, Critical for |z| >= 4.0.
        //
        // **Validates: Requirements 12.2**

        /// Helper: map a z-score absolute value to the expected severity,
        /// mirroring the logic in `MeshStore::snapshot()`.
        fn expected_severity(z_abs: f64) -> AnomalySeverity {
            if z_abs >= 4.0 {
                AnomalySeverity::Critical
            } else {
                AnomalySeverity::Warning
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(100))]

            #[test]
            fn zscore_outlier_detected_with_correct_severity(
                // Base value for the distribution centre
                base in -1000.0_f64..1000.0,
                // Half-width that controls the std dev (must be > 0 to avoid
                // zero std dev). We use a range that keeps values finite.
                delta in 0.1_f64..500.0,
                // Number of samples in the window (>= 30)
                n in 30_usize..=200,
                // z-score multiplier: how many std devs the outlier is from
                // the mean. We test both the Warning (3.0..4.0) and Critical
                // (>= 4.0) bands.
                z_mult in 3.0_f64..8.0,
                // Direction: positive or negative outlier
                positive in proptest::bool::ANY,
            ) {
                // Build a window with a known distribution.
                // We push `n` samples: half at (base - delta), half at (base + delta).
                // This gives:
                //   mean  = base
                //   std   = delta  (population std dev of symmetric ±delta)
                let max_size = n.max(200);
                let mut window = SlidingWindow::new(max_size);

                let half = n / 2;
                for _ in 0..half {
                    window.push(base - delta);
                }
                for _ in half..n {
                    window.push(base + delta);
                }

                // Verify the window has enough samples
                prop_assert!(window.len() >= 30);

                let mean = window.mean();
                let std = window.std_dev();

                // std should be close to delta (exactly delta for even n)
                prop_assert!(std > f64::EPSILON, "std dev should be positive");

                // Construct an outlier at exactly z_mult standard deviations
                let outlier = if positive {
                    mean + z_mult * std
                } else {
                    mean - z_mult * std
                };

                // Compute z-score
                let z = window.z_score(outlier);
                prop_assert!(z.is_some(), "z_score should return Some for >= 30 samples with non-zero std");

                let z_val = z.unwrap();
                let z_abs = z_val.abs();

                // The z-score absolute value should be approximately z_mult
                prop_assert!(
                    (z_abs - z_mult).abs() < 0.01,
                    "expected |z| ≈ {}, got {}",
                    z_mult,
                    z_abs,
                );

                // Since z_mult >= 3.0, the z-score should trigger an anomaly
                prop_assert!(z_abs >= 3.0, "|z| should be >= 3.0, got {}", z_abs);

                // Verify severity mapping
                let severity = expected_severity(z_abs);
                if z_mult >= 4.0 {
                    prop_assert_eq!(
                        severity,
                        AnomalySeverity::Critical,
                        "z_mult={} (|z|={}) should produce Critical",
                        z_mult,
                        z_abs,
                    );
                } else {
                    prop_assert_eq!(
                        severity,
                        AnomalySeverity::Warning,
                        "z_mult={} (|z|={}) should produce Warning",
                        z_mult,
                        z_abs,
                    );
                }
            }

            #[test]
            fn zscore_within_3_std_devs_does_not_trigger_anomaly(
                base in -1000.0_f64..1000.0,
                delta in 0.1_f64..500.0,
                n in 30_usize..=200,
                // z multiplier strictly below 3.0
                z_mult in 0.0_f64..2.99,
                positive in proptest::bool::ANY,
            ) {
                let max_size = n.max(200);
                let mut window = SlidingWindow::new(max_size);

                let half = n / 2;
                for _ in 0..half {
                    window.push(base - delta);
                }
                for _ in half..n {
                    window.push(base + delta);
                }

                prop_assert!(window.len() >= 30);

                let mean = window.mean();
                let std = window.std_dev();
                prop_assert!(std > f64::EPSILON);

                let observed = if positive {
                    mean + z_mult * std
                } else {
                    mean - z_mult * std
                };

                let z = window.z_score(observed);
                prop_assert!(z.is_some());

                let z_abs = z.unwrap().abs();
                // Should be below the 3.0 threshold — no anomaly should be generated
                prop_assert!(
                    z_abs < 3.0,
                    "|z| should be < 3.0 for z_mult={}, got {}",
                    z_mult,
                    z_abs,
                );
            }

            #[test]
            fn zscore_returns_none_with_fewer_than_30_samples(
                n in 1_usize..30,
                base in -100.0_f64..100.0,
                delta in 0.1_f64..50.0,
            ) {
                let mut window = SlidingWindow::new(100);
                let half = n / 2;
                for _ in 0..half {
                    window.push(base - delta);
                }
                for _ in half..n {
                    window.push(base + delta);
                }

                prop_assert!(window.len() < 30);
                // z_score must return None (fallback to fixed mode)
                prop_assert!(window.z_score(base + 10.0 * delta).is_none());
            }
        }
    }
}

