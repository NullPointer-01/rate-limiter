package org.nullpointer.ratelimiter.core.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.storage.InMemoryStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalRateLimitEngineTest {

    private InMemoryStore store;
    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimitEngine engine;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        configManager = new HierarchicalConfigurationManager(store);
        engine = new HierarchicalRateLimitEngine(configManager);
    }

    @Test
    void singleLevelAllowsWithinLimit() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").build();
        RateLimitResult result = engine.process(context, 1);
        assertTrue(result.isAllowed());
    }

    @Test
    void singleLevelDeniesWhenExhausted() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").build();
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 2).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @Test
    void multiLevelAllowsWhenAllLevelsPermit() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/data").build();
        RateLimitResult result = engine.process(context, 1);
        assertTrue(result.isAllowed());
    }

    @Test
    void multiLevelDeniesWhenMostRestrictiveLevelExhausted() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/data").build();

        // Exhaust endpoint-level limit (capacity = 3)
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        // 4th request denied by endpoint level even though global has capacity
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @Test
    void multiLevelDeniesWhenGlobalLevelExhausted() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        // Global level is the bottleneck (capacity = 2)
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/data").build();
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        // 3rd request denied by global level even though endpoint has capacity
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @Test
    void costIsAppliedToAllLevels() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").build();

        // Cost of 3 consumes from both levels
        RateLimitResult r1 = engine.process(context, 3);
        assertTrue(r1.isAllowed());

        // Another cost of 3 should fail on user level (5 - 3 = 2 remaining, needs 3)
        RateLimitResult r2 = engine.process(context, 3);
        assertFalse(r2.isAllowed());
    }

    @Test
    void throwsWhenConfigNotFound() {
        // No policy set — getHierarchyPolicy() should throw
        RequestContext context = RequestContext.builder().userId("unknown").build();
        assertThrows(RateLimitConfigNotFoundException.class, () -> engine.process(context, 1));
    }

    @Test
    void stateIsInitializedOnFirstRequest() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        // The engine will generate key "user:user1" for USER scope
        RateLimitKey stateKey = new RateLimitKey("user:user1");

        // State should be null before first request
        assertNull(configManager.getState(stateKey));

        RequestContext context = RequestContext.builder().userId("user1").build();
        engine.process(context, 1);

        // State should now be initialized
        assertNotNull(configManager.getState(stateKey));
    }

    @Test
    void separateKeysHaveIndependentState() {
        // One shared policy; different users get different state keys
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext user1 = RequestContext.builder().userId("user1").build();
        RequestContext user2 = RequestContext.builder().userId("user2").build();

        // Exhaust user1
        assertTrue(engine.process(user1, 1).isAllowed());
        assertTrue(engine.process(user1, 1).isAllowed());
        assertFalse(engine.process(user1, 1).isAllowed());

        // user2 should still have capacity
        assertTrue(engine.process(user2, 1).isAllowed());
        assertTrue(engine.process(user2, 1).isAllowed());
        assertFalse(engine.process(user2, 1).isAllowed());
    }

    @Test
    void sharedLevelStateAcrossDifferentRequests() {
        // Two users share the same global-level key ("global:system")
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext user1 = RequestContext.builder().userId("user1").build();
        RequestContext user2 = RequestContext.builder().userId("user2").build();

        // user1 consumes 2 from global
        assertTrue(engine.process(user1, 1).isAllowed());
        assertTrue(engine.process(user1, 1).isAllowed());

        // user2 consumes 1 from global (now at 3/3 used)
        assertTrue(engine.process(user2, 1).isAllowed());

        // user2's next request denied by shared global limit
        assertFalse(engine.process(user2, 1).isAllowed());
    }

    @Test
    void multiAlgorithmLevels() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        // Global level uses TokenBucket
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        // Endpoint level uses FixedWindowCounter
        policy.addPolicy(RateLimitScope.ENDPOINT, new FixedWindowCounterConfig(5, 1, TimeUnit.MINUTES));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/data").build();

        for (int i = 0; i < 5; i++) {
            assertTrue(engine.process(context, 1).isAllowed(), "Request " + (i + 1) + " should be allowed");
        }

        // 6th request should be denied by FixedWindowCounter
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @Test
    void multiLevelHierarchyEnforcement() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(1000, 100, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/data").build();

        // All 5 allowed — endpoint is the bottleneck
        for (int i = 0; i < 5; i++) {
            assertTrue(engine.process(context, 1).isAllowed());
        }

        // 6th denied
        RateLimitResult denied = engine.process(context, 1);
        assertFalse(denied.isAllowed());
    }

    @Test
    void middleLevelDeniesWhileOthersAllow() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));   // plenty
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS));         // bottleneck
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));  // plenty
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/test").build();

        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());

        // Denied by middle (user) level
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @Test
    void noPhantomConsumptionWhenLaterLevelDenies() {
        // Regression test: when a later level denies, earlier levels must NOT lose quota.
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        // Global level: capacity 5
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(5, 1, 10, TimeUnit.SECONDS));
        // Endpoint level: capacity 2 (bottleneck)
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(2, 1, 10, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").apiPath("/api/data").build();

        // Consume 2 requests — both levels should allow
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());

        // 3rd request denied by endpoint level
        assertFalse(engine.process(context, 1).isAllowed());

        // Global level should still have 3 remaining (5 - 2 = 3).
        // Swap policy to GLOBAL-only and verify remaining tokens.
        HierarchicalRateLimitConfig globalOnlyPolicy = new HierarchicalRateLimitConfig();
        globalOnlyPolicy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(5, 1, 10, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(globalOnlyPolicy);

        // Should allow 3 more requests from the global pool
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @Test
    void noPhantomConsumptionOnFirstLevelWhenSecondDenies() {
        // Two-level hierarchy: first level has 10, second level is the bottleneck.
        // Denied requests must not consume from the first level.
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(10, 1, 10, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(1, 1, 10, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext context = RequestContext.builder().userId("user1").build();
        assertTrue(engine.process(context, 1).isAllowed());

        // Next 5 are all denied (user level exhausted)
        for (int i = 0; i < 5; i++) {
            assertFalse(engine.process(context, 1).isAllowed());
        }

        // Global level should have consumed only 1 token (not 6).
        // Swap policy to GLOBAL-only and verify remaining tokens.
        HierarchicalRateLimitConfig globalOnlyPolicy = new HierarchicalRateLimitConfig();
        globalOnlyPolicy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(10, 1, 10, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(globalOnlyPolicy);

        // Should have 9 remaining (10 - 1 = 9)
        for (int i = 0; i < 9; i++) {
            assertTrue(engine.process(context, 1).isAllowed(),
                    "Request " + (i + 1) + " should be allowed, global tokens should not have been phantom-consumed");
        }
        assertFalse(engine.process(context, 1).isAllowed());
    }
}
