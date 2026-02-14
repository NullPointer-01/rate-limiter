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
    };

    public abstract RateLimitingAlgorithm getAlgorithm();
}
