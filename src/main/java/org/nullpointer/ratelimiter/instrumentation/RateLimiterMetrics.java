package org.nullpointer.ratelimiter.instrumentation;

import java.util.concurrent.atomic.AtomicLong;

public class RateLimiterMetrics {
    private final AtomicLong total;
    private final AtomicLong allowed;
    private final AtomicLong rejected;
    private final AtomicLong error;

    public RateLimiterMetrics() {
        total = new AtomicLong(0);
        allowed = new AtomicLong(0);
        rejected = new AtomicLong(0);
        error = new AtomicLong(0);
    }

    public void logAllowed() {
        allowed.incrementAndGet();
        total.incrementAndGet();
    }

    public void logRejected() {
        rejected.incrementAndGet();
        total.incrementAndGet();
    }

    public void logError() {
        error.incrementAndGet();
        total.incrementAndGet();
    }

    public double getRejectionRate() {
        return (double) 100 * rejected.get() / total.get();
    }

    public double getErrorRate() {
        return (double) 100 * error.get() / total.get();
    }
}
