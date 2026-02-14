package org.nullpointer.ratelimiter.model;

public class RateLimitResult {
    private final boolean allowed;
    private final long limit;
    private final long remaining;
    private final long resetAtMillis;
    private final long retryAfterMillis;

    private RateLimitResult(Builder builder) {
        this.allowed = builder.allowed;
        this.limit = builder.limit;
        this.remaining = builder.remaining;
        this.resetAtMillis = builder.resetAtMillis;
        this.retryAfterMillis = builder.retryAfterMillis;
    }

    public boolean isAllowed() {
        return allowed;
    }

    public long getRemaining() {
        return remaining;
    }

    public long getResetAtMillis() {
        return resetAtMillis;
    }

    public long getRetryAfterMillis() {
        return retryAfterMillis;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "RateLimitResult{" +
                "allowed=" + allowed +
                ", remaining=" + remaining +
                ", resetAtMillis=" + resetAtMillis +
                ", retryAfterMillis=" + retryAfterMillis +
                '}';
    }

    public static class Builder {
        private boolean allowed;
        private long limit;
        private long remaining;
        private long resetAtMillis;
        private long retryAfterMillis;

        public Builder allowed(boolean allowed) {
            this.allowed = allowed;
            return this;
        }

        public Builder limit(long limit) {
            this.limit = limit;
            return this;
        }

        public Builder remaining(long remaining) {
            this.remaining = remaining;
            return this;
        }

        public Builder resetAtMillis(long resetAtMillis) {
            this.resetAtMillis = resetAtMillis;
            return this;
        }

        public Builder retryAfterMillis(long retryAfterMillis) {
            this.retryAfterMillis = retryAfterMillis;
            return this;
        }

        public RateLimitResult build() {
            return new RateLimitResult(this);
        }
    }
}
