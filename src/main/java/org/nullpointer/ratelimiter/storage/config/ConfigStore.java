package org.nullpointer.ratelimiter.storage.config;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;

public interface ConfigStore {
    void setDefaultConfig(RateLimitConfig config);

    RateLimitConfig getDefaultConfig();

    void setConfig(RateLimitKey key, RateLimitConfig config);

    RateLimitConfig getConfig(RateLimitKey key);

    RateLimitConfig getOrDefaultConfig(RateLimitKey key);

    void setHierarchyPolicy(HierarchicalRateLimitConfig policy);

    HierarchicalRateLimitConfig getHierarchyPolicy();

    void setScopedPolicy(RateLimitScope scope, String identifier, RateLimitConfig config);

    RateLimitConfig getScopedPolicy(RateLimitScope scope, String identifier);
}
