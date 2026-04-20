package org.nullpointer.ratelimiter.circuitbreaker;

/**
 * Represents the three states of the circuit breaker.
 */
public enum CircuitBreakerState {
    OPEN,
    CLOSED,
    HALF_OPEN
}
