package org.nullpointer.ratelimiter.storage.config;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;

import java.util.Optional;

public interface ConfigRepository {
    void setDefaultConfig(RateLimitConfig config);

    RateLimitConfig getDefaultConfig();

    void setConfig(RateLimitKey key, RateLimitConfig config);

    RateLimitConfig getConfig(RateLimitKey key);

    RateLimitConfig getOrDefaultConfig(RateLimitKey key);

    void setPlanPolicy(SubscriptionPlan plan, HierarchicalRateLimitPolicy policy);

    HierarchicalRateLimitPolicy getPlanPolicy(SubscriptionPlan plan);

    void setPlanScopedConfig(SubscriptionPlan plan, RateLimitScope scope, String identifier, RateLimitConfig config);

    Optional<RateLimitConfig> getPlanScopedConfig(SubscriptionPlan plan, RateLimitScope scope, String identifier);
}
