package org.nullpointer.ratelimiter.model.state;

public class TokenBucketState implements RateLimitState {
    private double availableTokens;
    private long lastRefillNanos;

    public TokenBucketState(double availableTokens, long lastRefillNanos) {
        this.availableTokens = availableTokens;
        this.lastRefillNanos = lastRefillNanos;
    }

    public double getAvailableTokens() {
        return availableTokens;
    }

    public void setAvailableTokens(double availableTokens) {
        this.availableTokens = availableTokens;
    }

    public long getLastRefillNanos() {
        return lastRefillNanos;
    }

    public void setLastRefillNanos(long lastRefillNanos) {
        this.lastRefillNanos = lastRefillNanos;
    }
}
