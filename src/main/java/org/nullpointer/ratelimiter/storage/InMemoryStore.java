package org.nullpointer.ratelimiter.storage;

import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStore implements Store {
    private final Map<String, RateLimitConfig> configMap;
    private final Map<String, HierarchicalRateLimitConfig> hierarchicalConfigMap;
    private final Map<String, RateLimitState> stateMap;
    private final Map<String, RateLimitState> hierarchicalStateMap;

    private RateLimitConfig defaultConfig;

    public InMemoryStore() {
        this.configMap = new ConcurrentHashMap<>();
        this.hierarchicalConfigMap = new ConcurrentHashMap<>();
        this.stateMap = new ConcurrentHashMap<>();
        this.hierarchicalStateMap = new ConcurrentHashMap<>();
    }

    @Override
    public void setDefaultConfig(RateLimitConfig config) {
        this.defaultConfig = config;
    }

    @Override
    public RateLimitConfig getDefaultConfig() {
        return this.defaultConfig;
    }

    @Override
    public void setConfig(RateLimitKey key, RateLimitConfig config) {
        this.configMap.put(key.toKey(), config);
    }

    @Override
    public RateLimitConfig getConfig(RateLimitKey key) {
        return this.configMap.get(key.toKey());
    }

    @Override
    public RateLimitConfig getOrDefaultConfig(RateLimitKey key) {
        return this.configMap.getOrDefault(key.toKey(), defaultConfig);
    }

    @Override
    public void setHierarchicalConfig(RateLimitKey key, HierarchicalRateLimitConfig config) {
        this.hierarchicalConfigMap.put(key.toKey(), config);
    }

    @Override
    public HierarchicalRateLimitConfig getHierarchicalConfig(RateLimitKey key) {
        return this.hierarchicalConfigMap.get(key.toKey());
    }

    @Override
    public void setState(RateLimitKey key, RateLimitState state) {
        this.stateMap.put(key.toKey(), state);
    }

    @Override
    public RateLimitState getState(RateLimitKey key) {
        return this.stateMap.get(key.toKey());
    }

    @Override
    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        this.hierarchicalStateMap.put(key.toKey(), state);
    }

    @Override
    public RateLimitState getHierarchicalState(RateLimitKey key) {
        return this.hierarchicalStateMap.get(key.toKey());
    }
}
