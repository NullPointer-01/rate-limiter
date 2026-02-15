package org.nullpointer.ratelimiter.model.state;

public class TokenBucketState implements RateLimitState {
    private double availableTokens;
    private long lastRefillTimestamp;

    public TokenBucketState(double availableTokens, long lastRefillTimestamp) {
        this.availableTokens = availableTokens;
        this.lastRefillTimestamp = lastRefillTimestamp;
    }

    public double getAvailableTokens() {
        return availableTokens;
    }

    public void setAvailableTokens(double availableTokens) {
        this.availableTokens = availableTokens;
    }

    public long getLastRefillTimestamp() {
        return lastRefillTimestamp;
    }

    public void setLastRefillTimestamp(long lastRefillTimestamp) {
        this.lastRefillTimestamp = lastRefillTimestamp;
    }
}
