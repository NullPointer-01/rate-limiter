package org.nullpointer.ratelimiter.model.config.hierarchical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;

public class RateLimitLevel implements Comparable<RateLimitLevel> {
    private final RateLimitScope scope;
    private final RateLimitConfig config;
    private final StateRepositoryType stateRepositoryType;

    @JsonCreator
    public RateLimitLevel(@JsonProperty("scope") RateLimitScope scope,
                          @JsonProperty("config") RateLimitConfig config,
                          @JsonProperty("stateRepositoryType") StateRepositoryType stateRepositoryType) {
        if (scope == null) throw new IllegalArgumentException("Scope cannot be null");
        if (config == null) throw new IllegalArgumentException("Config cannot be null");
        if (stateRepositoryType == null) throw new IllegalArgumentException("StateRepositoryType cannot be null");

        this.scope = scope;
        this.config = config;
        this.stateRepositoryType = stateRepositoryType;
    }

    public RateLimitScope getScope() {
        return scope;
    }

    public RateLimitConfig getConfig() {
        return config;
    }

    public StateRepositoryType getStateRepositoryType() {
        return stateRepositoryType;
    }

    @Override
    public int compareTo(RateLimitLevel other) {
        return Integer.compare(this.scope.getOrder(), other.scope.getOrder());
    }
}
