package org.nullpointer.ratelimiter.core;

import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

public class ConfigurationManager {
    protected final ConfigRepository configRepository;
    protected final StateRepository stateRepository;

    public ConfigurationManager(ConfigRepository configRepository, StateRepository stateRepository) {
        this.configRepository = configRepository;
        this.stateRepository = stateRepository;
    }

    public void setConfig(RateLimitKey key, RateLimitConfig config) {
        this.configRepository.setConfig(key, config);
    }

    public void setDefaultConfig(RateLimitConfig config) {
        this.configRepository.setDefaultConfig(config);
    }

    public RateLimitConfig getDefaultConfig() {
        return this.configRepository.getDefaultConfig();
    }

    public RateLimitConfig getConfig(RateLimitKey key) {
        RateLimitConfig config = this.configRepository.getOrDefaultConfig(key);
        if (config == null) {
            throw new RateLimitConfigNotFoundException("No rate limit configuration for the key, " + key.toKey());
        }

        return config;
    }

    public void setState(RateLimitKey key, RateLimitState state) {
        this.stateRepository.setState(key, state);
    }

    public RateLimitState getState(RateLimitKey key) {
        return this.stateRepository.getState(key);
    }
}