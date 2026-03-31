package org.nullpointer.ratelimiter.model.config.hierarchical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HierarchicalRateLimitPolicy {
    @JsonProperty("levels")
    private final List<RateLimitLevel> levels;

    public HierarchicalRateLimitPolicy() {
        this.levels = new ArrayList<>();
    }

    @JsonCreator
    public HierarchicalRateLimitPolicy(@JsonProperty("levels") List<RateLimitLevel> levels) {
        this.levels = levels != null ? new ArrayList<>(levels) : new ArrayList<>();
    }

    public void addPolicy(RateLimitScope scope, RateLimitConfig defaultConfig) {
        RateLimitLevel level = new RateLimitLevel(scope, defaultConfig);
        levels.add(level);
        Collections.sort(levels);
    }

    public List<RateLimitLevel> getLevels() {
        return Collections.unmodifiableList(levels);
    }

    @JsonIgnore
    public boolean isEmpty() {
        return getLevels().isEmpty();
    }
}