package org.nullpointer.ratelimiter.model;

public class RequestTime {
    private final long currentTimeMillis;
    private final long nanoTime;

    public RequestTime(long currentTimeMillis, long nanoTime) {
        this.currentTimeMillis = currentTimeMillis;
        this.nanoTime = nanoTime;
    }

    public long currentTimeMillis() {
        return currentTimeMillis;
    }

    public long nanoTime() {
        return nanoTime;
    }
}
