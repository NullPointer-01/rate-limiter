package org.nullpointer.ratelimiter.client.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.storage.InMemoryStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalRateLimiterTest {

    private InMemoryStore store;
    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        configManager = new HierarchicalConfigurationManager(store);
        rateLimiter = new HierarchicalRateLimiter(configManager);
    }

    @Test
    void processWithCostDelegatesToEngine() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();
        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), key);
        configManager.setHierarchicalConfig(key, config);

        RateLimitResult r1 = rateLimiter.process(key, 3);
        assertTrue(r1.isAllowed());

        RateLimitResult r2 = rateLimiter.process(key, 3);
        assertFalse(r2.isAllowed());
    }

    @Test
    void processMultiLevelHierarchyRateLimit() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();
        RateLimitKey globalKey = RateLimitKey.builder().setDomain("app").build();
        RateLimitKey userKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), globalKey);
        config.addLevel(new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS), userKey);
        config.addLevel(new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), endpointKey);

        configManager.setHierarchicalConfig(requestKey, config);

        // 5 requests pass
        for (int i = 0; i < 5; i++) {
            assertTrue(rateLimiter.process(requestKey).isAllowed());
        }

        // 6th request fails
        assertFalse(rateLimiter.process(requestKey).isAllowed());
    }

    @Test
    void deniedResultContainsRetryInfo() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();
        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(1, 1, 1, TimeUnit.SECONDS), key);
        configManager.setHierarchicalConfig(key, config);

        rateLimiter.process(key); // exhaust
        RateLimitResult denied = rateLimiter.process(key);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() >= 0);
    }
}
