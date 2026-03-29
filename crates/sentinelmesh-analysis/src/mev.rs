use std::collections::BTreeMap;

use sentinelmesh_core::{Anomaly, AnomalySeverity, MevAuditSummary, TransactionOrderObservation};

/// Compute the Kendall tau distance between two orderings of the same items.
///
/// Both slices must contain the same set of items (though possibly in different
/// order). Returns the number of discordant pairs — pairs `(i, j)` where the
/// relative order differs between the two sequences.
fn kendall_tau_distance(a: &[String], b: &[String]) -> usize {
    // Build a position map for ordering `b`.
    let pos_b: BTreeMap<&str, usize> = b.iter().enumerate().map(|(i, s)| (s.as_str(), i)).collect();

    let n = a.len();
    let mut discordant = 0;
    for i in 0..n {
        for j in (i + 1)..n {
            // Look up positions of a[i] and a[j] in b.
            if let (Some(&bi), Some(&bj)) = (pos_b.get(a[i].as_str()), pos_b.get(a[j].as_str())) {
                // In `a`, item at index i comes before item at index j.
                // In `b`, if bi > bj the pair is discordant.
                if bi > bj {
                    discordant += 1;
                }
            }
        }
    }
    discordant
}

/// Compute the concordance between two orderings of the same items.
///
/// Concordance = 1 - (tau_distance / max_distance) where
/// max_distance = n*(n-1)/2.
///
/// Returns `None` when the orderings have fewer than 2 common items (no pairs
/// to compare).
fn pairwise_concordance(a: &[String], b: &[String]) -> Option<f64> {
    // Find the common items and restrict both orderings to them.
    let set_a: std::collections::BTreeSet<&str> = a.iter().map(String::as_str).collect();
    let set_b: std::collections::BTreeSet<&str> = b.iter().map(String::as_str).collect();
    let common: std::collections::BTreeSet<&str> = set_a.intersection(&set_b).copied().collect();

    let n = common.len();
    if n < 2 {
        return None;
    }

    // Filter each ordering to only common items, preserving order.
    let filtered_a: Vec<String> = a
        .iter()
        .filter(|s| common.contains(s.as_str()))
        .cloned()
        .collect();
    let filtered_b: Vec<String> = b
        .iter()
        .filter(|s| common.contains(s.as_str()))
        .cloned()
        .collect();

    let max_distance = n * (n - 1) / 2;
    if max_distance == 0 {
        return Some(1.0);
    }

    let tau = kendall_tau_distance(&filtered_a, &filtered_b);
    Some(1.0 - (tau as f64 / max_distance as f64))
}

/// Compute the average ordering concordance across all pairs of orderings.
///
/// Each ordering is a slice of transaction signatures as observed by one
/// endpoint for the same slot. The concordance metric is based on the
/// normalised Kendall tau distance.
///
/// Returns `1.0` when there are fewer than 2 orderings (nothing to compare).
pub fn ordering_concordance(orderings: &[&[String]]) -> f64 {
    if orderings.len() < 2 {
        return 1.0;
    }

    let mut total = 0.0;
    let mut count = 0_usize;

    for i in 0..orderings.len() {
        for j in (i + 1)..orderings.len() {
            if let Some(c) = pairwise_concordance(orderings[i], orderings[j]) {
                total += c;
                count += 1;
            }
        }
    }

    if count == 0 {
        1.0
    } else {
        total / count as f64
    }
}

/// Analyse transaction ordering observations grouped by slot and produce an
/// MEV audit summary together with any anomalies detected.
///
/// Slots without transaction data or with observations from a single endpoint
/// are silently skipped (no error generated).
pub fn analyse_mev(
    observations: &[TransactionOrderObservation],
) -> (MevAuditSummary, Vec<Anomaly>) {
    // Group observations by slot.
    let mut by_slot: BTreeMap<u64, Vec<&Vec<String>>> = BTreeMap::new();
    for obs in observations {
        if obs.transaction_signatures.is_empty() {
            continue;
        }
        by_slot
            .entry(obs.slot)
            .or_default()
            .push(&obs.transaction_signatures);
    }

    let mut slots_analyzed = 0_usize;
    let mut slots_with_reordering = 0_usize;
    let mut anomalies = Vec::new();

    for (slot, orderings_refs) in &by_slot {
        // Need at least 2 endpoints to compare.
        if orderings_refs.len() < 2 {
            continue;
        }

        let orderings: Vec<&[String]> = orderings_refs.iter().map(|v| v.as_slice()).collect();
        let concordance = ordering_concordance(&orderings);

        slots_analyzed += 1;

        if concordance < 0.80 {
            slots_with_reordering += 1;
            anomalies.push(Anomaly {
                severity: AnomalySeverity::Warning,
                code: "mev_reordering_suspected".to_owned(),
                summary: format!(
                    "Slot {slot}: transaction ordering concordance is {concordance:.3}, \
                     suggesting possible MEV reordering."
                ),
            });
        }
    }

    let summary = MevAuditSummary {
        slots_analyzed,
        slots_with_reordering,
    };

    (summary, anomalies)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinelmesh_core::TransactionOrderObservation;

    // ---------------------------------------------------------------
    // Unit tests for ordering_concordance
    // ---------------------------------------------------------------

    #[test]
    fn identical_orderings_have_concordance_one() {
        let txs = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let orderings: Vec<&[String]> = vec![&txs, &txs];
        let c = ordering_concordance(&orderings);
        assert!((c - 1.0).abs() < f64::EPSILON, "expected 1.0, got {c}");
    }

    #[test]
    fn reversed_orderings_have_concordance_zero() {
        let a = vec!["x".to_owned(), "y".to_owned(), "z".to_owned()];
        let b = vec!["z".to_owned(), "y".to_owned(), "x".to_owned()];
        let orderings: Vec<&[String]> = vec![&a, &b];
        let c = ordering_concordance(&orderings);
        // 3 items → max_distance = 3, reversed → 3 discordant → concordance = 0
        assert!((c - 0.0).abs() < f64::EPSILON, "expected 0.0, got {c}");
    }

    #[test]
    fn single_ordering_returns_one() {
        let txs = vec!["a".to_owned(), "b".to_owned()];
        let orderings: Vec<&[String]> = vec![&txs];
        let c = ordering_concordance(&orderings);
        assert!((c - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn empty_orderings_returns_one() {
        let orderings: Vec<&[String]> = vec![];
        let c = ordering_concordance(&orderings);
        assert!((c - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn partial_overlap_concordance() {
        // a: [1, 2, 3], b: [1, 3, 2]
        // discordant pairs in common items: (2,3) is swapped → 1 discordant
        // max_distance = 3, concordance = 1 - 1/3 ≈ 0.667
        let a = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let b = vec!["1".to_owned(), "3".to_owned(), "2".to_owned()];
        let orderings: Vec<&[String]> = vec![&a, &b];
        let c = ordering_concordance(&orderings);
        assert!((c - 2.0 / 3.0).abs() < 1e-10, "expected ~0.667, got {c}");
    }

    // ---------------------------------------------------------------
    // Unit tests for analyse_mev
    // ---------------------------------------------------------------

    #[test]
    fn analyse_mev_detects_reordering() {
        let observations = vec![
            TransactionOrderObservation {
                slot: 100,
                transaction_signatures: vec!["a".into(), "b".into(), "c".into()],
            },
            TransactionOrderObservation {
                slot: 100,
                transaction_signatures: vec!["c".into(), "b".into(), "a".into()],
            },
        ];

        let (summary, anomalies) = analyse_mev(&observations);
        assert_eq!(summary.slots_analyzed, 1);
        assert_eq!(summary.slots_with_reordering, 1);
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].code, "mev_reordering_suspected");
        assert_eq!(anomalies[0].severity, AnomalySeverity::Warning);
    }

    #[test]
    fn analyse_mev_no_anomaly_for_identical_orderings() {
        let observations = vec![
            TransactionOrderObservation {
                slot: 200,
                transaction_signatures: vec!["x".into(), "y".into(), "z".into()],
            },
            TransactionOrderObservation {
                slot: 200,
                transaction_signatures: vec!["x".into(), "y".into(), "z".into()],
            },
        ];

        let (summary, anomalies) = analyse_mev(&observations);
        assert_eq!(summary.slots_analyzed, 1);
        assert_eq!(summary.slots_with_reordering, 0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn analyse_mev_skips_single_endpoint() {
        let observations = vec![TransactionOrderObservation {
            slot: 300,
            transaction_signatures: vec!["a".into(), "b".into()],
        }];

        let (summary, anomalies) = analyse_mev(&observations);
        assert_eq!(summary.slots_analyzed, 0);
        assert_eq!(summary.slots_with_reordering, 0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn analyse_mev_skips_empty_transaction_data() {
        let observations = vec![
            TransactionOrderObservation {
                slot: 400,
                transaction_signatures: vec![],
            },
            TransactionOrderObservation {
                slot: 400,
                transaction_signatures: vec![],
            },
        ];

        let (summary, anomalies) = analyse_mev(&observations);
        assert_eq!(summary.slots_analyzed, 0);
        assert_eq!(summary.slots_with_reordering, 0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn analyse_mev_empty_input() {
        let (summary, anomalies) = analyse_mev(&[]);
        assert_eq!(summary.slots_analyzed, 0);
        assert_eq!(summary.slots_with_reordering, 0);
        assert!(anomalies.is_empty());
    }

    #[test]
    fn analyse_mev_multiple_slots() {
        let observations = vec![
            // Slot 1: identical orderings → no anomaly
            TransactionOrderObservation {
                slot: 1,
                transaction_signatures: vec!["a".into(), "b".into()],
            },
            TransactionOrderObservation {
                slot: 1,
                transaction_signatures: vec!["a".into(), "b".into()],
            },
            // Slot 2: reversed → anomaly
            TransactionOrderObservation {
                slot: 2,
                transaction_signatures: vec!["x".into(), "y".into(), "z".into()],
            },
            TransactionOrderObservation {
                slot: 2,
                transaction_signatures: vec!["z".into(), "y".into(), "x".into()],
            },
        ];

        let (summary, anomalies) = analyse_mev(&observations);
        assert_eq!(summary.slots_analyzed, 2);
        assert_eq!(summary.slots_with_reordering, 1);
        assert_eq!(anomalies.len(), 1);
        assert!(anomalies[0].summary.contains("Slot 2"));
    }

    // ---------------------------------------------------------------
    // Property-based tests
    // ---------------------------------------------------------------
    // Feature: sentinelmesh-comprehensive-upgrade, Property 17: Concordância de ordenação MEV e anomalia
    // **Validates: Requirements 13.2, 13.3, 13.4**

    use proptest::prelude::*;

    /// Independent reference implementation of Kendall tau distance for
    /// verification. Counts discordant pairs between two orderings of the
    /// same items.
    fn reference_kendall_tau(a: &[String], b: &[String]) -> usize {
        let pos_b: std::collections::BTreeMap<&str, usize> =
            b.iter().enumerate().map(|(i, s)| (s.as_str(), i)).collect();
        let n = a.len();
        let mut disc = 0;
        for i in 0..n {
            for j in (i + 1)..n {
                if let (Some(&bi), Some(&bj)) = (pos_b.get(a[i].as_str()), pos_b.get(a[j].as_str()))
                {
                    if bi > bj {
                        disc += 1;
                    }
                }
            }
        }
        disc
    }

    /// Independent reference implementation of pairwise concordance.
    fn reference_pairwise_concordance(a: &[String], b: &[String]) -> Option<f64> {
        let set_a: std::collections::BTreeSet<&str> = a.iter().map(String::as_str).collect();
        let set_b: std::collections::BTreeSet<&str> = b.iter().map(String::as_str).collect();
        let common: std::collections::BTreeSet<&str> =
            set_a.intersection(&set_b).copied().collect();
        let n = common.len();
        if n < 2 {
            return None;
        }
        let fa: Vec<String> = a
            .iter()
            .filter(|s| common.contains(s.as_str()))
            .cloned()
            .collect();
        let fb: Vec<String> = b
            .iter()
            .filter(|s| common.contains(s.as_str()))
            .cloned()
            .collect();
        let max_d = n * (n - 1) / 2;
        if max_d == 0 {
            return Some(1.0);
        }
        let tau = reference_kendall_tau(&fa, &fb);
        Some(1.0 - (tau as f64 / max_d as f64))
    }

    /// Independent reference implementation of ordering concordance.
    fn reference_ordering_concordance(orderings: &[&[String]]) -> f64 {
        if orderings.len() < 2 {
            return 1.0;
        }
        let mut total = 0.0;
        let mut count = 0_usize;
        for i in 0..orderings.len() {
            for j in (i + 1)..orderings.len() {
                if let Some(c) = reference_pairwise_concordance(orderings[i], orderings[j]) {
                    total += c;
                    count += 1;
                }
            }
        }
        if count == 0 {
            1.0
        } else {
            total / count as f64
        }
    }

    /// Strategy: generate a base ordering of N unique items (3-8 items),
    /// then produce a shuffled version via proptest's built-in shuffle.
    fn arb_ordering_pair() -> impl Strategy<Value = (Vec<String>, Vec<String>)> {
        (3usize..=8).prop_flat_map(|n| {
            let base: Vec<String> = (0..n).map(|i| format!("tx_{i}")).collect();
            let base_clone = base.clone();
            // Use Just for the base and shuffle for the permuted version
            (Just(base), Just(base_clone).prop_shuffle())
        })
    }

    /// Strategy: generate 2-4 orderings of the same base items for a single
    /// slot, each a permutation of the base.
    fn arb_orderings_for_slot() -> impl Strategy<Value = Vec<Vec<String>>> {
        (3usize..=8).prop_flat_map(|n| {
            let base: Vec<String> = (0..n).map(|i| format!("tx_{i}")).collect();
            prop::collection::vec(Just(base).prop_shuffle(), 2..=4)
        })
    }

    /// Strategy: generate TransactionOrderObservation sets for 1-3 slots,
    /// each with 2-4 endpoint orderings.
    fn arb_mev_observations() -> impl Strategy<Value = Vec<TransactionOrderObservation>> {
        prop::collection::vec((1u64..=1000, arb_orderings_for_slot()), 1..=3).prop_map(
            |slot_data| {
                let mut obs = Vec::new();
                for (slot, orderings) in slot_data {
                    for ordering in orderings {
                        obs.push(TransactionOrderObservation {
                            slot,
                            transaction_signatures: ordering,
                        });
                    }
                }
                obs
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// Property 17a: ordering_concordance matches independent reference
        /// implementation for any pair of orderings.
        #[test]
        fn prop_concordance_matches_reference(
            (base, shuffled) in arb_ordering_pair()
        ) {
            let orderings: Vec<&[String]> = vec![&base, &shuffled];
            let actual = ordering_concordance(&orderings);
            let expected = reference_ordering_concordance(&orderings);
            prop_assert!(
                (actual - expected).abs() < 1e-10,
                "concordance mismatch: actual={actual}, expected={expected}"
            );
        }

        /// Property 17b: ordering_concordance matches reference for
        /// multiple orderings (2-4 endpoints).
        #[test]
        fn prop_concordance_multi_endpoint_matches_reference(
            orderings_owned in arb_orderings_for_slot()
        ) {
            let orderings: Vec<&[String]> =
                orderings_owned.iter().map(Vec::as_slice).collect();
            let actual = ordering_concordance(&orderings);
            let expected = reference_ordering_concordance(&orderings);
            prop_assert!(
                (actual - expected).abs() < 1e-10,
                "multi-endpoint concordance mismatch: actual={actual}, expected={expected}"
            );
        }

        /// Property 17c: identical orderings always yield concordance 1.0.
        #[test]
        fn prop_identical_orderings_concordance_one(
            n in 3usize..=8
        ) {
            let base: Vec<String> =
                (0..n).map(|i| format!("tx_{i}")).collect();
            let orderings: Vec<&[String]> = vec![&base, &base];
            let c = ordering_concordance(&orderings);
            prop_assert!(
                (c - 1.0).abs() < f64::EPSILON,
                "identical orderings should yield 1.0, got {c}"
            );
        }

        /// Property 17d: analyse_mev generates anomaly when concordance < 0.80
        /// and no anomaly when concordance >= 0.80. Also verifies
        /// MevAuditSummary counts.
        #[test]
        fn prop_analyse_mev_anomaly_and_summary(
            observations in arb_mev_observations()
        ) {
            let (summary, anomalies) = analyse_mev(&observations);

            // Group by slot independently to compute expected values.
            let mut by_slot: std::collections::BTreeMap<u64, Vec<&Vec<String>>> =
                std::collections::BTreeMap::new();
            for obs in &observations {
                if !obs.transaction_signatures.is_empty() {
                    by_slot
                        .entry(obs.slot)
                        .or_default()
                        .push(&obs.transaction_signatures);
                }
            }

            let mut expected_analyzed = 0usize;
            let mut expected_reordering = 0usize;

            for (_slot, ords) in &by_slot {
                if ords.len() < 2 {
                    continue;
                }
                expected_analyzed += 1;
                let slices: Vec<&[String]> =
                    ords.iter().map(|v| v.as_slice()).collect();
                let c = reference_ordering_concordance(&slices);
                if c < 0.80 {
                    expected_reordering += 1;
                }
            }

            prop_assert_eq!(
                summary.slots_analyzed,
                expected_analyzed,
                "slots_analyzed mismatch"
            );
            prop_assert_eq!(
                summary.slots_with_reordering,
                expected_reordering,
                "slots_with_reordering mismatch"
            );

            // Every anomaly must be mev_reordering_suspected with Warning
            for a in &anomalies {
                prop_assert_eq!(
                    a.code.as_str(),
                    "mev_reordering_suspected",
                    "unexpected anomaly code: {}",
                    a.code
                );
                prop_assert_eq!(
                    a.severity,
                    AnomalySeverity::Warning,
                    "anomaly severity should be Warning"
                );
            }

            // Number of anomalies must equal slots with reordering
            prop_assert_eq!(
                anomalies.len(),
                expected_reordering,
                "anomaly count should match slots_with_reordering"
            );
        }
    }
}
