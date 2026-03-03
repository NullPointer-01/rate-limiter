package org.nullpointer.ratelimiter.model.config.hierarchical;

import org.nullpointer.ratelimiter.model.config.RateLimitConfig;

public class RateLimitLevel implements Comparable<RateLimitLevel> {
    private final RateLimitScope scope;
    private final RateLimitConfig defaultConfig;

    public RateLimitLevel(RateLimitScope scope, RateLimitConfig defaultConfig) {
        if (scope == null) {
            throw new IllegalArgumentException("Scope cannot be null");
        }
        if (defaultConfig == null) {
            throw new IllegalArgumentException("Default config cannot be null");
        }
        this.scope = scope;
        this.defaultConfig = defaultConfig;
    }

    public RateLimitScope getScope() {
        return scope;
    }

    public RateLimitConfig getDefaultConfig() {
        return defaultConfig;
    }

    @Override
    public int compareTo(RateLimitLevel other) {
        return Integer.compare(this.scope.getOrder(), other.scope.getOrder());
    }
}
