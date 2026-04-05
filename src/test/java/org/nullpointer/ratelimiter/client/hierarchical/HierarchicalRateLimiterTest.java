package org.nullpointer.ratelimiter.client.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalConfigurationManager;
import org.nullpointer.ratelimiter.utils.PlanPolicyLoader;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.factory.StateRepositoryFactory;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HierarchicalRateLimiterTest {

    private ConfigRepository configStore;
    private StateRepository stateStore;
    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        configStore = new InMemoryConfigRepository();
        stateStore = new InMemoryStateRepository();

        StateRepositoryFactory registry = StateRepositoryFactory.getInstance();
        registry.clearRegistry();
        registry.register(StateRepositoryType.IN_MEMORY, stateStore);

        configManager = new HierarchicalConfigurationManager(configStore, stateStore, PlanPolicyLoader.getInstance(), registry);
        rateLimiter = new HierarchicalRateLimiter(configManager);
    }

    @Test
    void processWithCostDelegatesToEngine() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER,
            new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("user1").build();

        RateLimitResult r1 = rateLimiter.process(context, 3);
        assertTrue(r1.isAllowed());

        RateLimitResult r2 = rateLimiter.process(context, 3);
        assertFalse(r2.isAllowed());
    }

    @Test
    void processMultiLevelHierarchyRateLimit() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL,
            new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER,
            new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT,
            new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = RequestContext.builder()
            .plan(SubscriptionPlan.FREE)
            .userId("user1")
            .apiPath("/api/data")
            .build();

        // 5 requests pass
        for (int i = 0; i < 5; i++) {
            assertTrue(rateLimiter.process(context).isAllowed());
        }

        // 6th request fails
        assertFalse(rateLimiter.process(context).isAllowed());
    }

    @Test
    void deniedResultContainsRetryInfo() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER,
            new TokenBucketConfig(1, 1, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("user1").build();

        rateLimiter.process(context); // exhaust capacity
        RateLimitResult denied = rateLimiter.process(context);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() >= 0);
    }
}
