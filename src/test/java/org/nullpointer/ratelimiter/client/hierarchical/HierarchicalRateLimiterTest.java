package org.nullpointer.ratelimiter.client.hierarchical;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.nullpointer.ratelimiter.core.hierarchical.HierarchicalConfigurationManager;
import org.nullpointer.ratelimiter.utils.PlanPolicyLoader;

import java.util.stream.Stream;
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
    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimiter rateLimiter;

    static Stream<String> configFiles() {
        return Stream.of(
                "rate-limiter-test-single-plan.yml",
                "rate-limiter-test-single-plan-atomic.yml",
                "rate-limiter-test-defaults.yml",
                "rate-limiter-test-defaults-atomic.yml"
        );
    }

    private void buildSetup(String configFile) {
        ConfigRepository configStore = new InMemoryConfigRepository();
        StateRepository stateStore = new InMemoryStateRepository();

        StateRepositoryFactory registry = new StateRepositoryFactory();
        registry.register(StateRepositoryType.IN_MEMORY, stateStore);

        configManager = new HierarchicalConfigurationManager(configStore, stateStore,
                PlanPolicyLoader.withConfig(configFile), registry);
        rateLimiter = new HierarchicalRateLimiter(configManager);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void processWithCostDelegatesToEngine(String configFile) {
        buildSetup(configFile);
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void processMultiLevelHierarchyRateLimit(String configFile) {
        buildSetup(configFile);
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void deniedResultContainsRetryInfo(String configFile) {
        buildSetup(configFile);
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
