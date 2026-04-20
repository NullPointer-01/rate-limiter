package org.nullpointer.ratelimiter.circuitbreaker;

/**
 * Represents the failure modes of the circuit breaker.
 */
public enum CircuitBreakerMode {
    FAIL_OPEN,
    FAIL_CLOSED
}
