package org.nullpointer.ratelimiter.instrumentation;

import java.util.concurrent.atomic.AtomicLong;

public class RateLimiterMetrics {
    private final AtomicLong total;
    private final AtomicLong allowed;
    private final AtomicLong rejected;

    public RateLimiterMetrics() {
        total = new AtomicLong(0);
        allowed = new AtomicLong(0);
        rejected = new AtomicLong(0);
    }

    public void logAllowed() {
        allowed.incrementAndGet();
        total.incrementAndGet();
    }

    public void logRejected() {
        rejected.incrementAndGet();
        total.incrementAndGet();
    }
}
