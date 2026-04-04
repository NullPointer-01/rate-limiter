package org.nullpointer.ratelimiter.factory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.utils.PlanPolicyLoader;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PlanPolicyLoaderTest {

    private PlanPolicyLoader factory;

    @BeforeEach
    void setUp() {
        factory = PlanPolicyLoader.getInstance();
    }

    @Test
    void loadsAllThreePlans() {
        Map<SubscriptionPlan, HierarchicalRateLimitPolicy> policies = factory.getDefaultPolicies();
        assertEquals(3, policies.size());
        assertTrue(policies.containsKey(SubscriptionPlan.FREE));
        assertTrue(policies.containsKey(SubscriptionPlan.PREMIUM));
        assertTrue(policies.containsKey(SubscriptionPlan.ENTERPRISE));
    }

    @Test
    void freePlanHasFourLevels() {
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        assertNotNull(policy);
        assertEquals(4, policy.getLevels().size());
    }

    @Test
    void freePlanLevelsAreOrderedByScope() {
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        var levels = policy.getLevels();
        for (int i = 1; i < levels.size(); i++) {
            assertTrue(levels.get(i - 1).getScope().getOrder() < levels.get(i).getScope().getOrder());
        }
    }

    @Test
    void freePlanGlobalLevelUsesFixedWindowCounter() {
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        RateLimitLevel globalLevel = policy.getLevel(RateLimitScope.REGION).orElseThrow();
        RateLimitConfig config = globalLevel.getConfig();
        assertInstanceOf(FixedWindowCounterConfig.class, config);
        FixedWindowCounterConfig fwc = (FixedWindowCounterConfig) config;
        assertEquals(10000, fwc.getCapacity());
        assertEquals(60000, fwc.getWindowSizeMillis());
    }

    @Test
    void freePlanTenantLevelUsesTokenBucket() {
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        RateLimitLevel tenantLevel = policy.getLevel(RateLimitScope.TENANT).orElseThrow();
        assertInstanceOf(TokenBucketConfig.class, tenantLevel.getConfig());
    }

    @Test
    void freePlanUserLevelUsesSlidingWindowCounter() {
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        RateLimitLevel userLevel = policy.getLevel(RateLimitScope.USER).orElseThrow();
        assertInstanceOf(SlidingWindowCounterConfig.class, userLevel.getConfig());
    }

    @Test
    void premiumPlanHasHigherLimits() {
        HierarchicalRateLimitPolicy free = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        HierarchicalRateLimitPolicy premium = factory.getDefaultPolicies().get(SubscriptionPlan.PREMIUM);

        FixedWindowCounterConfig freeGlobal = (FixedWindowCounterConfig)
                free.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();
        FixedWindowCounterConfig premiumGlobal = (FixedWindowCounterConfig)
                premium.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();

        assertTrue(premiumGlobal.getCapacity() > freeGlobal.getCapacity());
    }

    @Test
    void enterprisePlanHasHighestLimits() {
        HierarchicalRateLimitPolicy premium = factory.getDefaultPolicies().get(SubscriptionPlan.PREMIUM);
        HierarchicalRateLimitPolicy enterprise = factory.getDefaultPolicies().get(SubscriptionPlan.ENTERPRISE);

        FixedWindowCounterConfig premiumGlobal = (FixedWindowCounterConfig)
                premium.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();
        FixedWindowCounterConfig enterpriseGlobal = (FixedWindowCounterConfig)
                enterprise.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();

        assertTrue(enterpriseGlobal.getCapacity() > premiumGlobal.getCapacity());
    }

    @Test
    void allLevelsHaveStateRepositoryTypeResolved() {
        for (SubscriptionPlan plan : SubscriptionPlan.values()) {
            HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(plan);
            assertNotNull(policy);
            for (RateLimitLevel level : policy.getLevels()) {
                assertNotNull(level.getStateRepositoryType(),
                        plan + "/" + level.getScope() + " should have a StateRepositoryType");
            }
        }
    }

    @Test
    void nullConfigPathThrows() {
        assertThrows(IllegalArgumentException.class, () -> PlanPolicyLoader.withConfig(null));
    }

    @Test
    void missingConfigFileThrows() {
        assertThrows(IllegalStateException.class,
                () -> PlanPolicyLoader.withConfig("nonexistent.yml"));
    }

    @Test
    void unknownPlanReturnsNullAfterLoad() {
        PlanPolicyLoader f = PlanPolicyLoader.withConfig("rate-limiter-test-single-plan.yml");
        assertNull(f.getDefaultPolicies().get(SubscriptionPlan.ENTERPRISE));
    }
}
