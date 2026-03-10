package org.nullpointer.ratelimiter.core;

import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.storage.config.ConfigStore;
import org.nullpointer.ratelimiter.storage.state.StateStore;

public class ConfigurationManager {
    protected final ConfigStore configStore;
    protected final StateStore stateStore;

    public ConfigurationManager(ConfigStore configStore, StateStore stateStore) {
        this.configStore = configStore;
        this.stateStore = stateStore;
    }

    public void setConfig(RateLimitKey key, RateLimitConfig config) {
        this.configStore.setConfig(key, config);
    }

    public void setDefaultConfig(RateLimitConfig config) {
        this.configStore.setDefaultConfig(config);
    }

    public RateLimitConfig getDefaultConfig() {
        return this.configStore.getDefaultConfig();
    }

    public RateLimitConfig getConfig(RateLimitKey key) {
        RateLimitConfig config = this.configStore.getOrDefaultConfig(key);
        if (config == null) {
            throw new RateLimitConfigNotFoundException("No rate limit configuration for the key, " + key.toKey());
        }

        return config;
    }

    public void setState(RateLimitKey key, RateLimitState state) {
        this.stateStore.setState(key, state);
    }

    public RateLimitState getState(RateLimitKey key) {
        return this.stateStore.getState(key);
    }
}