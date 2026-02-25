package org.nullpointer.ratelimiter.core.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
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
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();
        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), key);

        configManager.setHierarchicalConfig(key, config);

        RateLimitResult result = engine.process(key, 1);
        assertTrue(result.isAllowed());
    }

    @Test
    void singleLevelDeniesWhenExhausted() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();
        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS), key);

        configManager.setHierarchicalConfig(key, config);

        assertTrue(engine.process(key, 1).isAllowed());
        assertTrue(engine.process(key, 2).isAllowed());
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void multiLevelAllowsWhenAllLevelsPermit() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();
        RateLimitKey serviceKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey userKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), serviceKey);
        config.addLevel(new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS), userKey);
        config.addLevel(new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS), endpointKey);

        configManager.setHierarchicalConfig(requestKey, config);

        RateLimitResult result = engine.process(requestKey, 1);
        assertTrue(result.isAllowed());
    }

    @Test
    void multiLevelDeniesWhenMostRestrictiveLevelExhausted() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();
        RateLimitKey globalKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), globalKey);
        config.addLevel(new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS), endpointKey);

        configManager.setHierarchicalConfig(requestKey, config);

        // Exhaust endpoint-level limit (capacity = 3)
        assertTrue(engine.process(requestKey, 1).isAllowed());
        assertTrue(engine.process(requestKey, 1).isAllowed());
        assertTrue(engine.process(requestKey, 1).isAllowed());
        // 4th request denied by endpoint level even though global has capacity
        assertFalse(engine.process(requestKey, 1).isAllowed());
    }

    @Test
    void multiLevelDeniesWhenGlobalLevelExhausted() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();
        RateLimitKey serviceKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        // Service level is the bottleneck (capacity = 2)
        config.addLevel(new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS), serviceKey);
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), endpointKey);

        configManager.setHierarchicalConfig(requestKey, config);

        assertTrue(engine.process(requestKey, 1).isAllowed());
        assertTrue(engine.process(requestKey, 1).isAllowed());
        // 3rd request denied by global level even though endpoint has capacity
        assertFalse(engine.process(requestKey, 1).isAllowed());
    }

    @Test
    void costIsAppliedToAllLevels() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey globalKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey userKey = RateLimitKey.builder().setUserId("user1").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS), globalKey);
        config.addLevel(new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), userKey);

        configManager.setHierarchicalConfig(requestKey, config);

        // Cost of 3 consumes from both levels
        RateLimitResult r1 = engine.process(requestKey, 3);
        assertTrue(r1.isAllowed());

        // Another cost of 3 should fail on user level (5 - 3 = 2 remaining, needs 3)
        RateLimitResult r2 = engine.process(requestKey, 3);
        assertFalse(r2.isAllowed());
    }

    @Test
    void throwsWhenConfigNotFound() {
        RateLimitKey key = RateLimitKey.builder().setUserId("unknown").build();
        assertThrows(RateLimitConfigNotFoundException.class, () -> engine.process(key, 1));
    }

    @Test
    void stateIsInitializedOnFirstRequest() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey levelKey = RateLimitKey.builder().setUserId("user1").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), levelKey);

        configManager.setHierarchicalConfig(requestKey, config);

        // State should be null before first request
        assertNull(configManager.getHierarchicalState(levelKey));

        engine.process(requestKey, 1);

        // State should now be initialized
        assertNotNull(configManager.getHierarchicalState(levelKey));
    }

    @Test
    void separateKeysHaveIndependentState() {
        RateLimitKey user1Key = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey user2Key = RateLimitKey.builder().setUserId("user2").build();

        HierarchicalRateLimitConfig config1 = new HierarchicalRateLimitConfig();
        config1.addLevel(new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS), user1Key);
        configManager.setHierarchicalConfig(user1Key, config1);

        HierarchicalRateLimitConfig config2 = new HierarchicalRateLimitConfig();
        config2.addLevel(new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS), user2Key);
        configManager.setHierarchicalConfig(user2Key, config2);

        // Exhaust user1
        assertTrue(engine.process(user1Key, 1).isAllowed());
        assertTrue(engine.process(user1Key, 1).isAllowed());
        assertFalse(engine.process(user1Key, 1).isAllowed());

        // user2 should still have capacity
        assertTrue(engine.process(user2Key, 1).isAllowed());
        assertTrue(engine.process(user2Key, 1).isAllowed());
        assertFalse(engine.process(user2Key, 1).isAllowed());
    }

    @Test
    void sharedLevelStateAcrossDifferentRequests() {
        // Two users share the same global-level key
        RateLimitKey serviceKey = RateLimitKey.builder().setDomain("service").build();

        RateLimitKey user1RequestKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey user1LevelKey = RateLimitKey.builder().setUserId("user1").build();

        RateLimitKey user2RequestKey = RateLimitKey.builder().setUserId("user2").build();
        RateLimitKey user2LevelKey = RateLimitKey.builder().setUserId("user2").build();

        HierarchicalRateLimitConfig config1 = new HierarchicalRateLimitConfig();
        config1.addLevel(new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS), serviceKey);
        config1.addLevel(new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS), user1LevelKey);
        configManager.setHierarchicalConfig(user1RequestKey, config1);

        HierarchicalRateLimitConfig config2 = new HierarchicalRateLimitConfig();
        config2.addLevel(new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS), serviceKey);
        config2.addLevel(new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS), user2LevelKey);
        configManager.setHierarchicalConfig(user2RequestKey, config2);

        // user1 consumes 2 from global
        assertTrue(engine.process(user1RequestKey, 1).isAllowed());
        assertTrue(engine.process(user1RequestKey, 1).isAllowed());

        // user2 consumes 1 from global (now at 3/3 used)
        assertTrue(engine.process(user2RequestKey, 1).isAllowed());

        // user2's next request denied by shared global limit
        assertFalse(engine.process(user2RequestKey, 1).isAllowed());
    }

    @Test
    void multiAlgorithmLevels() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();
        RateLimitKey globalKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        // Global level uses TokenBucket
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), globalKey);
        // Endpoint level uses FixedWindowCounter
        config.addLevel(new FixedWindowCounterConfig(5, 1, TimeUnit.MINUTES), endpointKey);

        configManager.setHierarchicalConfig(requestKey, config);

        for (int i = 0; i < 5; i++) {
            assertTrue(engine.process(requestKey, 1).isAllowed(), "Request " + (i + 1) + " should be allowed");
        }

        // 6th request should be denied by FixedWindowCounter
        assertFalse(engine.process(requestKey, 1).isAllowed());
    }

    @Test
    void multiLevelHierarchyEnforcement() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        RateLimitKey globalKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey userKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(1000, 100, 1, TimeUnit.SECONDS), globalKey); // Level 1
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), userKey); // Level 2
        config.addLevel(new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS), endpointKey); // Level 3

        configManager.setHierarchicalConfig(requestKey, config);

        // All 5 allowed — endpoint is the bottleneck
        for (int i = 0; i < 5; i++) {
            assertTrue(engine.process(requestKey, 1).isAllowed());
        }

        // 6th denied
        RateLimitResult denied = engine.process(requestKey, 1);
        assertFalse(denied.isAllowed());
    }

    @Test
    void middleLevelDeniesWhileOthersAllow() {
        RateLimitKey requestKey = RateLimitKey.builder().setUserId("user1").setApi("/api/test").build();

        RateLimitKey globalKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey userKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/test").build();

        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), globalKey);  // plenty
        config.addLevel(new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS), userKey);        // bottleneck
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), endpointKey); // plenty

        configManager.setHierarchicalConfig(requestKey, config);

        assertTrue(engine.process(requestKey, 1).isAllowed());
        assertTrue(engine.process(requestKey, 1).isAllowed());

        // Denied by middle (user) level
        assertFalse(engine.process(requestKey, 1).isAllowed());
    }
}
