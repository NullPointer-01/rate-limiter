package org.nullpointer.ratelimiter.factory;

import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerConfig;
import org.nullpointer.ratelimiter.model.circuitbreaker.CircuitBreakerMode;

import java.util.concurrent.TimeUnit;

public class CircuitBreakerFactory {

    public static CircuitBreakerConfig defaultCircuitBreakerConfig() {
        return new CircuitBreakerConfig(CircuitBreakerMode.FAIL_OPEN, 10, 30, TimeUnit.SECONDS, 50.0, 50.0, 10, 5);
    }
}
