package org.nullpointer.ratelimiter.circuitbreaker;

import java.util.concurrent.atomic.LongAdder;

/**
 * A single time bucket holding striped success/error counters
 */
public class TimeBucket {
    private final LongAdder successAdder;
    private final LongAdder errorAdder;
    private final long windowStartNanos;

    public TimeBucket(long windowStartNanos) {
        this.windowStartNanos = windowStartNanos;
        this.successAdder = new LongAdder();
        this.errorAdder = new LongAdder();
    }

    public void incrementSuccess() {
        successAdder.increment();
    }

    public void incrementError() {
        errorAdder.increment();
    }

    public long sumSuccess() {
        return successAdder.sum();
    }

    public long sumError() {
        return errorAdder.sum();
    }

    public long getWindowStartNanos() {
        return windowStartNanos;
    }
}
