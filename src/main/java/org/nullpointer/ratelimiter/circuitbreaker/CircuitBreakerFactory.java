package org.nullpointer.ratelimiter.circuitbreaker;

/**
 * Factory for creating CircuitBreakerConfig instances with default settings.
 */
public class CircuitBreakerFactory {

    private CircuitBreakerFactory() {
    }

    public static CircuitBreakerConfig defaultCircuitBreakerConfig() {
        return CircuitBreakerConfig.builder().build();
    }
}
