package org.nullpointer.ratelimiter.factory;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class PlanPolicyLoaderTest {

    static Stream<String> configFiles() {
        return Stream.of(
                "rate-limiter-test-single-plan.yml",
                "rate-limiter-test-single-plan-atomic.yml",
                "rate-limiter-test-defaults.yml",
                "rate-limiter-test-defaults-atomic.yml"
        );
    }

    static Stream<String> multiPlanConfigFiles() {
        return Stream.of(
                "rate-limiter-test-defaults.yml",
                "rate-limiter-test-defaults-atomic.yml"
        );
    }

    static Stream<String> singlePlanConfigFiles() {
        return Stream.of(
                "rate-limiter-test-single-plan.yml",
                "rate-limiter-test-single-plan-atomic.yml"
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void loadsAllThreePlans(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        Map<SubscriptionPlan, HierarchicalRateLimitPolicy> policies = factory.getDefaultPolicies();
        assertEquals(3, policies.size());
        assertTrue(policies.containsKey(SubscriptionPlan.FREE));
        assertTrue(policies.containsKey(SubscriptionPlan.PREMIUM));
        assertTrue(policies.containsKey(SubscriptionPlan.ENTERPRISE));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void freePlanHasFourLevels(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        assertNotNull(policy);
        assertEquals(4, policy.getLevels().size());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void freePlanGlobalLevelUsesFixedWindowCounter(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        RateLimitLevel globalLevel = policy.getLevel(RateLimitScope.REGION).orElseThrow();
        RateLimitConfig config = globalLevel.getConfig();
        assertInstanceOf(FixedWindowCounterConfig.class, config);
        FixedWindowCounterConfig fwc = (FixedWindowCounterConfig) config;
        assertEquals(10000, fwc.getCapacity());
        assertEquals(60000, fwc.getWindowSizeMillis());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void freePlanTenantLevelUsesTokenBucket(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        RateLimitLevel tenantLevel = policy.getLevel(RateLimitScope.TENANT).orElseThrow();
        assertInstanceOf(TokenBucketConfig.class, tenantLevel.getConfig());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void freePlanUserLevelUsesSlidingWindowCounter(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        RateLimitLevel userLevel = policy.getLevel(RateLimitScope.USER).orElseThrow();
        assertInstanceOf(SlidingWindowCounterConfig.class, userLevel.getConfig());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void premiumPlanHasHigherLimits(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy free = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        HierarchicalRateLimitPolicy premium = factory.getDefaultPolicies().get(SubscriptionPlan.PREMIUM);

        FixedWindowCounterConfig freeGlobal = (FixedWindowCounterConfig)
                free.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();
        FixedWindowCounterConfig premiumGlobal = (FixedWindowCounterConfig)
                premium.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();

        assertTrue(premiumGlobal.getCapacity() > freeGlobal.getCapacity());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void enterprisePlanHasHighestLimits(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy premium = factory.getDefaultPolicies().get(SubscriptionPlan.PREMIUM);
        HierarchicalRateLimitPolicy enterprise = factory.getDefaultPolicies().get(SubscriptionPlan.ENTERPRISE);

        FixedWindowCounterConfig premiumGlobal = (FixedWindowCounterConfig)
                premium.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();
        FixedWindowCounterConfig enterpriseGlobal = (FixedWindowCounterConfig)
                enterprise.getLevel(RateLimitScope.REGION).orElseThrow().getConfig();

        assertTrue(enterpriseGlobal.getCapacity() > premiumGlobal.getCapacity());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void allLevelsHaveStateRepositoryTypeResolved(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        for (SubscriptionPlan plan : SubscriptionPlan.values()) {
            HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(plan);
            assertNotNull(policy, plan + " policy should not be null");
            for (RateLimitLevel level : policy.getLevels()) {
                assertNotNull(level.getStateRepositoryType(),
                        plan + "/" + level.getScope() + " should have a StateRepositoryType");
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singlePlanConfigFiles")
    void singlePlanLoadsOnlyFreePlan(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        Map<SubscriptionPlan, HierarchicalRateLimitPolicy> policies = factory.getDefaultPolicies();
        assertEquals(1, policies.size());
        assertTrue(policies.containsKey(SubscriptionPlan.FREE));
        assertNull(policies.get(SubscriptionPlan.PREMIUM));
        assertNull(policies.get(SubscriptionPlan.ENTERPRISE));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singlePlanConfigFiles")
    void singlePlanFreePlanHasOneLevel(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        assertNotNull(policy);
        assertEquals(1, policy.getLevels().size());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singlePlanConfigFiles")
    void singlePlanFreePlanUserLevelUsesTokenBucket(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        RateLimitLevel userLevel = policy.getLevel(RateLimitScope.USER).orElseThrow();
        assertInstanceOf(TokenBucketConfig.class, userLevel.getConfig());
        TokenBucketConfig tbc = (TokenBucketConfig) userLevel.getConfig();
        assertEquals(10, tbc.getCapacity());
        assertEquals(1, tbc.getRefillTokens());
        assertEquals(1000, tbc.getRefillIntervalMillis());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singlePlanConfigFiles")
    void singlePlanAllLevelsHaveStateRepositoryTypeResolved(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        assertNotNull(policy);
        for (RateLimitLevel level : policy.getLevels()) {
            assertNotNull(level.getStateRepositoryType(),
                    "FREE/" + level.getScope() + " should have a StateRepositoryType");
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singlePlanConfigFiles")
    void unknownPlanReturnsNullAfterLoad(String configFile) {
        PlanPolicyLoader f = PlanPolicyLoader.withConfig(configFile);
        assertNull(f.getDefaultPolicies().get(SubscriptionPlan.ENTERPRISE));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void freePlanLevelsAreOrderedByScope(String configFile) {
        PlanPolicyLoader factory = PlanPolicyLoader.withConfig(configFile);
        HierarchicalRateLimitPolicy policy = factory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        var levels = policy.getLevels();
        for (int i = 1; i < levels.size(); i++) {
            assertTrue(levels.get(i - 1).getScope().getOrder() < levels.get(i).getScope().getOrder());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void nullConfigPathThrows(String configFile) {
        assertThrows(IllegalArgumentException.class, () -> PlanPolicyLoader.withConfig(null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void missingConfigFileThrows(String configFile) {
        assertThrows(IllegalStateException.class,
                () -> PlanPolicyLoader.withConfig("nonexistent.yml"));
    }
}
