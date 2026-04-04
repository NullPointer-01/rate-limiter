package org.nullpointer.ratelimiter.model.config.hierarchical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class HierarchicalRateLimitPolicy {
    @JsonProperty("levels")
    private final List<RateLimitLevel> levels;

    public HierarchicalRateLimitPolicy() {
        this.levels = new ArrayList<>();
    }

    // Used only during JSON deserialization
    @JsonCreator
    protected HierarchicalRateLimitPolicy(@JsonProperty("levels") List<RateLimitLevel> levels) {
        this.levels = levels != null ? new ArrayList<>(levels) : new ArrayList<>();
    }

    public void addLevel(RateLimitLevel level) {
        if (level == null) throw new IllegalArgumentException("Level cannot be null");

        levels.add(level);
        Collections.sort(levels);
    }

    public List<RateLimitLevel> getLevels() {
        return Collections.unmodifiableList(levels);
    }

    public Optional<RateLimitLevel> getLevel(RateLimitScope scope) {
        return levels.stream().filter(l -> l.getScope() == scope).findFirst();
    }
}