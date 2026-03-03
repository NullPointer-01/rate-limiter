package org.nullpointer.ratelimiter.storage;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStore implements Store {
    private final Map<String, RateLimitConfig> configMap;
    private final Map<String, RateLimitConfig> scopedPolicyMap;
    private final Map<String, RateLimitState> stateMap;
    private final Map<String, RateLimitState> hierarchicalStateMap;

    private RateLimitConfig defaultConfig;
    private HierarchicalRateLimitConfig hierarchyPolicy;

    public InMemoryStore() {
        this.configMap = new ConcurrentHashMap<>();
        this.scopedPolicyMap = new ConcurrentHashMap<>();
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
    public void setHierarchyPolicy(HierarchicalRateLimitConfig policy) {
        this.hierarchyPolicy = policy;
    }

    @Override
    public HierarchicalRateLimitConfig getHierarchyPolicy() {
        return this.hierarchyPolicy;
    }

    @Override
    public void setScopedPolicy(RateLimitScope scope, String identifier, RateLimitConfig config) {
        this.scopedPolicyMap.put(toPolicyKey(scope, identifier), config);
    }

    @Override
    public RateLimitConfig getScopedPolicy(RateLimitScope scope, String identifier) {
        return this.scopedPolicyMap.get(toPolicyKey(scope, identifier));
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

    private String toPolicyKey(RateLimitScope scope, String identifier) {
        return scope.getPrefix() + ":" + identifier;
    }
}
