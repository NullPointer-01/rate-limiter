package org.nullpointer.ratelimiter.storage;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.storage.config.ConfigStore;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

class InMemoryConfigStoreTest {

    @Test
    void storesAndRetrievesDefaultAndSpecificConfig() {
        ConfigStore store = new InMemoryConfigStore();
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
        ConfigStore store = new InMemoryConfigStore();

        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        store.setHierarchyPolicy(policy);

        TokenBucketConfig defaultUserConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig overrideUserConfig = new TokenBucketConfig(25, 5, 1, TimeUnit.SECONDS);
        store.setScopedPolicy(RateLimitScope.USER, "DEFAULT", defaultUserConfig);
        store.setScopedPolicy(RateLimitScope.USER, "user-1", overrideUserConfig);

        assertSame(policy, store.getHierarchyPolicy());
        assertSame(defaultUserConfig, store.getScopedPolicy(RateLimitScope.USER, "DEFAULT"));
        assertSame(overrideUserConfig, store.getScopedPolicy(RateLimitScope.USER, "user-1"));
    }
}
