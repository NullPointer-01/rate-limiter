package org.nullpointer.ratelimiter.client.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.storage.config.ConfigStore;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigStore;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateStore;
import org.nullpointer.ratelimiter.storage.state.StateStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HierarchicalRateLimiterTest {

    private ConfigStore configStore;
    private StateStore stateStore;
    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        configStore = new InMemoryConfigStore();
        stateStore = new InMemoryStateStore();
        configManager = new HierarchicalConfigurationManager(configStore, stateStore);
        rateLimiter = new HierarchicalRateLimiter(configManager);
    }

    @Test
    void processWithCostDelegatesToEngine() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").build();

        RateLimitResult r1 = rateLimiter.process(context, 3);
        assertTrue(r1.isAllowed());

        RateLimitResult r2 = rateLimiter.process(context, 3);
        assertFalse(r2.isAllowed());
    }

    @Test
    void processMultiLevelHierarchyRateLimit() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/data").build();

        // 5 requests pass
        for (int i = 0; i < 5; i++) {
            assertTrue(rateLimiter.process(context).isAllowed());
        }

        // 6th request fails
        assertFalse(rateLimiter.process(context).isAllowed());
    }

    @Test
    void deniedResultContainsRetryInfo() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(1, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").build();

        rateLimiter.process(context); // exhaust capacity
        RateLimitResult denied = rateLimiter.process(context);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() >= 0);
    }
}
