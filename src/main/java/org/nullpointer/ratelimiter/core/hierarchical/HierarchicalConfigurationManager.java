package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.storage.Store;

public class HierarchicalConfigurationManager extends ConfigurationManager {

    public HierarchicalConfigurationManager(Store store) {
        super(store);
    }

    public void setHierarchicalConfig(RateLimitKey key, HierarchicalRateLimitConfig config) {
        this.store.setHierarchicalConfig(key, config);
    }

    public HierarchicalRateLimitConfig getHierarchicalConfig(RateLimitKey key) {
        HierarchicalRateLimitConfig config = this.store.getHierarchicalConfig(key);
        if (config == null || config.isEmpty()) {
            throw new RateLimitConfigNotFoundException("No rate limit configuration for the key, " + key.toKey());
        }

        return config;
    }

    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        this.store.setHierarchicalState(key, state);
    }

    public RateLimitState getHierarchicalState(RateLimitKey key) {
        return this.store.getHierarchicalState(key);
    }
}
