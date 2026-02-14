package org.nullpointer.ratelimiter.core;

import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.storage.Store;

public class ConfigurationManager {
    private final Store store;

    public ConfigurationManager(Store store) {
        this.store = store;
    }

    public void setConfig(RateLimitKey key, RateLimitConfig config) {
        this.store.setConfig(key, config);
    }

    public void setDefaultConfig(RateLimitConfig config) {
        this.store.setDefaultConfig(config);
    }

    public RateLimitConfig getDefaultConfig() {
        return this.store.getDefaultConfig();
    }

    public RateLimitConfig getConfig(RateLimitKey key) {
        RateLimitConfig config = this.store.getOrDefaultConfig(key);
        if (config == null) {
            throw new RateLimitConfigNotFoundException("No rate limit configuration for the key, " + key.toKey());
        }

        return config;
    }

    public void setState(RateLimitKey key, RateLimitState state) {
        this.store.setState(key, state);
    }

    public RateLimitState getState(RateLimitKey key) {
        return this.store.getState(key);
    }
}