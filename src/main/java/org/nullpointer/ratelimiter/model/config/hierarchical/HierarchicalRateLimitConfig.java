package org.nullpointer.ratelimiter.model.config.hierarchical;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HierarchicalRateLimitConfig {
    private final List<RateLimitLevel> levels;

    public HierarchicalRateLimitConfig() {
        this.levels = new ArrayList<>();
    }

    public void addLevel(RateLimitConfig config, RateLimitKey key) {
        RateLimitLevel level = new RateLimitLevel(levels.size() + 1, config, key);
        levels.add(level);
    }

    public List<RateLimitLevel> getLevels() {
        return Collections.unmodifiableList(levels);
    }

    public boolean isEmpty() {
        return getLevels().isEmpty();
    }
}
