package org.nullpointer.ratelimiter.model.state;

public class TokenBucketState implements RateLimitState {
    private long availableTokens;
    private long lastRefillTimestamp;

    public TokenBucketState(long availableTokens, long lastRefillTimestamp) {
        this.availableTokens = availableTokens;
        this.lastRefillTimestamp = lastRefillTimestamp;
    }

    public long getAvailableTokens() {
        return availableTokens;
    }

    public void setAvailableTokens(long availableTokens) {
        this.availableTokens = availableTokens;
    }

    public long getLastRefillTimestamp() {
        return lastRefillTimestamp;
    }

    public void setLastRefillTimestamp(long lastRefillTimestamp) {
        this.lastRefillTimestamp = lastRefillTimestamp;
    }
}
