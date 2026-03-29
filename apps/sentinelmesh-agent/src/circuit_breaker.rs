use std::collections::HashMap;
use std::time::{Duration, Instant};

use metrics::gauge;
use parking_lot::RwLock;

/// State of an individual endpoint circuit breaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    /// Numeric value for Prometheus gauge: 0=closed, 1=open, 2=half_open.
    fn metric_value(self) -> f64 {
        match self {
            CircuitState::Closed => 0.0,
            CircuitState::Open => 1.0,
            CircuitState::HalfOpen => 2.0,
        }
    }
}

/// Per-endpoint circuit breaker with state machine.
#[derive(Debug)]
pub struct EndpointCircuit {
    state: CircuitState,
    consecutive_failures: u32,
    last_attempt: Instant,
    failure_threshold: u32,
    recovery_interval: Duration,
}

impl EndpointCircuit {
    pub fn new(failure_threshold: u32, recovery_interval: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            last_attempt: Instant::now(),
            failure_threshold,
            recovery_interval,
        }
    }

    pub fn state(&self) -> CircuitState {
        self.state
    }

    /// Returns `true` if the endpoint should be probed in this cycle.
    ///
    /// - `Closed`: always probe.
    /// - `Open`: probe only when `recovery_interval` has elapsed (transitions to `HalfOpen`).
    /// - `HalfOpen`: allow the single verification probe.
    pub fn should_probe(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if self.last_attempt.elapsed() >= self.recovery_interval {
                    self.state = CircuitState::HalfOpen;
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful probe result.
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.last_attempt = Instant::now();
        match self.state {
            CircuitState::HalfOpen => {
                self.state = CircuitState::Closed;
            }
            CircuitState::Open => {
                // Shouldn't normally happen (Open → success without HalfOpen),
                // but handle gracefully.
                self.state = CircuitState::Closed;
            }
            CircuitState::Closed => {}
        }
    }

    /// Record a failed probe result.
    pub fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.last_attempt = Instant::now();
        match self.state {
            CircuitState::Closed => {
                if self.consecutive_failures >= self.failure_threshold {
                    self.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                self.state = CircuitState::Open;
                // Keep consecutive_failures as-is; the circuit just re-opened.
            }
            CircuitState::Open => {
                // Already open — nothing to do.
            }
        }
    }
}

/// Registry of circuit breakers for all known endpoints.
pub struct CircuitBreakerRegistry {
    circuits: RwLock<HashMap<String, EndpointCircuit>>,
    failure_threshold: u32,
    recovery_interval: Duration,
}

impl CircuitBreakerRegistry {
    pub fn new(failure_threshold: u32, recovery_interval: Duration) -> Self {
        Self {
            circuits: RwLock::new(HashMap::new()),
            failure_threshold,
            recovery_interval,
        }
    }

    /// Return the list of endpoint IDs that should be probed this cycle.
    ///
    /// Implements the blackout fallback: when *all* endpoints are `Open`,
    /// every endpoint is returned so the agent never goes completely dark.
    pub fn endpoints_to_probe(&self, all_endpoint_ids: &[String]) -> Vec<String> {
        let mut circuits = self.circuits.write();

        // Ensure every endpoint has a circuit entry.
        for id in all_endpoint_ids {
            circuits
                .entry(id.clone())
                .or_insert_with(|| EndpointCircuit::new(self.failure_threshold, self.recovery_interval));
        }

        let mut allowed: Vec<String> = Vec::new();
        for id in all_endpoint_ids {
            if let Some(circuit) = circuits.get_mut(id) {
                if circuit.should_probe() {
                    allowed.push(id.clone());
                }
            }
        }

        // Blackout fallback: if no endpoint is allowed, probe all.
        if allowed.is_empty() && !all_endpoint_ids.is_empty() {
            tracing::warn!(
                "all {} endpoints are in Open state — blackout fallback: probing all",
                all_endpoint_ids.len()
            );
            allowed = all_endpoint_ids.to_vec();
        }

        // Emit per-endpoint Prometheus metrics.
        for id in all_endpoint_ids {
            if let Some(circuit) = circuits.get(id) {
                gauge!("sentinelmesh_agent_circuit_breaker_state", "endpoint" => id.clone())
                    .set(circuit.state().metric_value());
            }
        }

        allowed
    }

    /// Record a successful probe for the given endpoint.
    pub fn record_success(&self, endpoint_id: &str) {
        let mut circuits = self.circuits.write();
        if let Some(circuit) = circuits.get_mut(endpoint_id) {
            circuit.record_success();
        }
    }

    /// Record a failed probe for the given endpoint.
    pub fn record_failure(&self, endpoint_id: &str) {
        let mut circuits = self.circuits.write();
        if let Some(circuit) = circuits.get_mut(endpoint_id) {
            circuit.record_failure();
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Represents a probe result in a generated sequence.
    #[derive(Clone, Copy, Debug)]
    enum ProbeResult {
        Success,
        Failure,
    }

    fn arb_probe_result() -> impl Strategy<Value = ProbeResult> {
        prop_oneof![Just(ProbeResult::Success), Just(ProbeResult::Failure),]
    }

    fn arb_probe_sequence() -> impl Strategy<Value = Vec<ProbeResult>> {
        prop::collection::vec(arb_probe_result(), 1..50)
    }

    fn arb_failure_threshold() -> impl Strategy<Value = u32> {
        1u32..=10
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_circuit_breaker_state_machine(
            sequence in arb_probe_sequence(),
            failure_threshold in arb_failure_threshold(),
        ) {
            // Feature: sentinelmesh-comprehensive-upgrade, Property 7: Máquina de estados do circuit breaker
            // **Validates: Requirements 6.1, 6.3**
            //
            // For any endpoint and any sequence of probe results, the circuit breaker
            // must transition correctly:
            // (a) Closed → Open after exactly failure_threshold consecutive failures
            // (b) Open → HalfOpen after recovery_interval (use Duration::ZERO)
            // (c) HalfOpen → Closed on success, HalfOpen → Open on failure

            // Use Duration::ZERO so Open→HalfOpen transition happens immediately
            // when should_probe() is called.
            let mut circuit = EndpointCircuit::new(failure_threshold, Duration::ZERO);
            assert_eq!(circuit.state(), CircuitState::Closed);

            let mut consecutive_failures: u32 = 0;

            for result in &sequence {
                let prev_state = circuit.state();

                match result {
                    ProbeResult::Success => {
                        circuit.record_success();
                        consecutive_failures = 0;

                        match prev_state {
                            CircuitState::Closed => {
                                // Success in Closed stays Closed
                                prop_assert_eq!(circuit.state(), CircuitState::Closed);
                            }
                            CircuitState::HalfOpen => {
                                // Success in HalfOpen → Closed
                                prop_assert_eq!(circuit.state(), CircuitState::Closed);
                            }
                            CircuitState::Open => {
                                // Success in Open → Closed (graceful handling)
                                prop_assert_eq!(circuit.state(), CircuitState::Closed);
                            }
                        }
                    }
                    ProbeResult::Failure => {
                        circuit.record_failure();
                        consecutive_failures += 1;

                        match prev_state {
                            CircuitState::Closed => {
                                if consecutive_failures >= failure_threshold {
                                    // Reached threshold → Open
                                    prop_assert_eq!(circuit.state(), CircuitState::Open);
                                } else {
                                    // Below threshold → still Closed
                                    prop_assert_eq!(circuit.state(), CircuitState::Closed);
                                }
                            }
                            CircuitState::HalfOpen => {
                                // Failure in HalfOpen → Open
                                prop_assert_eq!(circuit.state(), CircuitState::Open);
                            }
                            CircuitState::Open => {
                                // Already Open, stays Open
                                prop_assert_eq!(circuit.state(), CircuitState::Open);
                            }
                        }
                    }
                }

                // After processing, if state is Open, call should_probe() to
                // trigger Open → HalfOpen transition (recovery_interval is ZERO).
                if circuit.state() == CircuitState::Open {
                    let probed = circuit.should_probe();
                    // With Duration::ZERO, should_probe() always transitions
                    prop_assert!(probed);
                    prop_assert_eq!(circuit.state(), CircuitState::HalfOpen);
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn prop_circuit_breaker_blackout_fallback(
            num_endpoints in 2usize..=10,
            failure_threshold in 1u32..=5,
        ) {
            // Feature: sentinelmesh-comprehensive-upgrade, Property 8: Fallback de blackout total do circuit breaker
            // **Validates: Requisito 6.5**
            //
            // When all endpoints are in Open state, endpoints_to_probe() must
            // return all endpoints (blackout fallback).

            // Use a long recovery_interval so endpoints stay Open and don't
            // auto-transition to HalfOpen during should_probe().
            let recovery = Duration::from_secs(3600);
            let registry = CircuitBreakerRegistry::new(failure_threshold, recovery);

            let endpoint_ids: Vec<String> = (0..num_endpoints)
                .map(|i| format!("endpoint-{i}"))
                .collect();

            // First call to register all endpoints (they start Closed).
            let _ = registry.endpoints_to_probe(&endpoint_ids);

            // Force all endpoints into Open state by recording enough failures.
            for id in &endpoint_ids {
                for _ in 0..failure_threshold {
                    registry.record_failure(id);
                }
            }

            // Verify all circuits are Open.
            {
                let circuits = registry.circuits.read();
                for id in &endpoint_ids {
                    let circuit = circuits.get(id).unwrap();
                    prop_assert_eq!(circuit.state(), CircuitState::Open);
                }
            }

            // Now call endpoints_to_probe — blackout fallback should return all.
            let probed = registry.endpoints_to_probe(&endpoint_ids);
            prop_assert_eq!(probed.len(), endpoint_ids.len());
            for id in &endpoint_ids {
                prop_assert!(probed.contains(id));
            }
        }
    }
}
