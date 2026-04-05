package org.nullpointer.ratelimiter.storage;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

class InMemoryConfigRepositoryTest {

    @Test
    void storesAndRetrievesDefaultAndSpecificConfig() {
        ConfigRepository store = new InMemoryConfigRepository();
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        store.setDefaultConfig(defaultConfig);

        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        TokenBucketConfig specificConfig = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        store.setConfig(key, specificConfig);

        assertSame(defaultConfig, store.getDefaultConfig());
        assertSame(specificConfig, store.getConfig(key));
        assertSame(specificConfig, store.getOrDefaultConfig(key));
    }

    @Test
    void storesAndRetrievesHierarchyPolicyAndScopedPolicies() {
        ConfigRepository store = new InMemoryConfigRepository();

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(
                RateLimitScope.GLOBAL,
                new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS),
                StateRepositoryType.IN_MEMORY
        ));
        store.setPlanPolicy(SubscriptionPlan.FREE, policy);

        TokenBucketConfig defaultUserConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig overrideUserConfig = new TokenBucketConfig(25, 5, 1, TimeUnit.SECONDS);
        store.setPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "DEFAULT", defaultUserConfig);
        store.setPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "user-1", overrideUserConfig);

        assertSame(policy, store.getPlanPolicy(SubscriptionPlan.FREE));
        assertSame(defaultUserConfig, store.getPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "DEFAULT").orElse(null));
        assertSame(overrideUserConfig, store.getPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "user-1").orElse(null));
    }
}
