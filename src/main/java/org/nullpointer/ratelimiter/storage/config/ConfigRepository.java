package org.nullpointer.ratelimiter.storage.config;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;

public interface ConfigRepository {
    void setDefaultConfig(RateLimitConfig config);

    RateLimitConfig getDefaultConfig();

    void setConfig(RateLimitKey key, RateLimitConfig config);

    RateLimitConfig getConfig(RateLimitKey key);

    RateLimitConfig getOrDefaultConfig(RateLimitKey key);

    void setHierarchyPolicy(HierarchicalRateLimitPolicy policy);

    HierarchicalRateLimitPolicy getHierarchyPolicy();

    void setScopedConfig(RateLimitScope scope, String identifier, RateLimitConfig config);

    RateLimitConfig getScopedConfig(RateLimitScope scope, String identifier);
}
