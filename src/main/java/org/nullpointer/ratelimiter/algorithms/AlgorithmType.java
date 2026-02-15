package org.nullpointer.ratelimiter.algorithms;

public enum AlgorithmType {
    TOKEN_BUCKET() {
        @Override
        public RateLimitingAlgorithm getAlgorithm() {
            return new TokenBucketAlgorithm();
        }
    },
    SLIDING_WINDOW() {
        @Override
        public RateLimitingAlgorithm getAlgorithm() {
            return new SlidingWindowAlgorithm();
        }
    },
    FIXED_WINDOW_COUNTER() {
        @Override
        public RateLimitingAlgorithm getAlgorithm() {
            return new FixedWindowCounterAlgorithm();
        }
    },
    SLIDING_WINDOW_COUNTER() {
        @Override
        public RateLimitingAlgorithm getAlgorithm() {
            return new FixedWindowCounterAlgorithm();
        }
    };

    public abstract RateLimitingAlgorithm getAlgorithm();
}
