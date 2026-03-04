package org.nullpointer.ratelimiter.factory;

import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerConfig;

public class CircuitBreakerFactory {

    public static CircuitBreakerConfig defaultCircuitBreakerConfig() {
        return CircuitBreakerConfig.builder().build();
    }
}
