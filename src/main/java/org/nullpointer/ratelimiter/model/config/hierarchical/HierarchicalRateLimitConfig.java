package org.nullpointer.ratelimiter.model.config.hierarchical;

import org.nullpointer.ratelimiter.model.config.RateLimitConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HierarchicalRateLimitConfig {
    private final List<RateLimitLevel> levels;

    public HierarchicalRateLimitConfig() {
        this.levels = new ArrayList<>();
    }

    public void addPolicy(RateLimitScope scope, RateLimitConfig defaultConfig) {
        RateLimitLevel level = new RateLimitLevel(scope, defaultConfig);
        levels.add(level);
        Collections.sort(levels);
    }

    public List<RateLimitLevel> getLevels() {
        return Collections.unmodifiableList(levels);
    }

    public boolean isEmpty() {
        return getLevels().isEmpty();
    }
}
