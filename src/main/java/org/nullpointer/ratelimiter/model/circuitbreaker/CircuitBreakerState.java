package org.nullpointer.ratelimiter.model.circuitbreaker;

public enum CircuitBreakerState {
    OPEN,
    CLOSED,
    HALF_OPEN
}
