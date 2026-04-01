package org.nullpointer.ratelimiter.core.hierarchical;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.storage.config.RedisConfigRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisHierarchicalRateLimitEngineTest {

    private static RedisServer mockServer;
    private static JedisPool pool;

    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimitEngine engine;

    @BeforeAll
    static void startServer() throws IOException {
        mockServer = RedisServer.newRedisServer().start();
        pool = new JedisPool("localhost", mockServer.getBindPort());
    }

    @AfterAll
    static void stopServer() throws IOException {
        pool.close();
        mockServer.stop();
    }

    @BeforeEach
    void setUp() {
        // Flush all Redis state before each test to ensure isolation
        try (Jedis jedis = pool.getResource()) {
            jedis.flushAll();
        }

        JacksonSerializer serializer = new JacksonSerializer();
        configManager = new HierarchicalConfigurationManager(
                new RedisConfigRepository(pool, serializer),
                new RedisStateRepository(pool, serializer)
        );
        engine = new HierarchicalRateLimitEngine(configManager);
    }

    @Test
    void singleUserLevelAllowsWithinLimit() {
        configManager.setHierarchyPolicy(policyWith(
                RateLimitScope.USER, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS)));

        RequestContext ctx = RequestContext.builder().userId("h-user-allow").build();

        assertTrue(engine.process(ctx, 1).isAllowed());
    }

    @Test
    void singleUserLevelDeniesWhenExhausted() {
        configManager.setHierarchyPolicy(policyWith(
                RateLimitScope.USER, new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS)));

        RequestContext ctx = RequestContext.builder().userId("h-user-deny").build();

        assertTrue(engine.process(ctx, 2).isAllowed());
        assertFalse(engine.process(ctx, 2).isAllowed());
    }

    @Test
    void singleUserLevelRemainingDecrementsCorrectly() {
        configManager.setHierarchyPolicy(policyWith(
                RateLimitScope.USER, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS)));

        RequestContext ctx = RequestContext.builder().userId("h-remaining").build();

        RateLimitResult r1 = engine.process(ctx, 1);
        RateLimitResult r2 = engine.process(ctx, 1);
        RateLimitResult r3 = engine.process(ctx, 3);

        assertTrue(r1.getRemaining() > r2.getRemaining());
        assertTrue(r2.getRemaining() > r3.getRemaining());
    }

    @Test
    void globalBottleneckDeniesEvenWithPermissiveUserLimit() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext ctx = RequestContext.builder().userId("h-global-bot").build();

        assertTrue(engine.process(ctx, 1).isAllowed());
        assertTrue(engine.process(ctx, 1).isAllowed());
        assertFalse(engine.process(ctx, 1).isAllowed()); // global exhausted
    }

    @Test
    void userBottleneckDeniesEvenWithPermissiveGlobalLimit() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext ctx = RequestContext.builder().userId("h-user-bot").build();

        assertTrue(engine.process(ctx, 1).isAllowed());
        assertTrue(engine.process(ctx, 1).isAllowed());
        assertFalse(engine.process(ctx, 1).isAllowed()); // user exhausted
    }

    @Test
    void allLevelsMustAllowForRequestToSucceed() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.ENDPOINT, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext ctx = RequestContext.builder()
                .userId("h-all-levels")
                .apiPath("/api/test")
                .httpMethod("GET")
                .build();

        assertTrue(engine.process(ctx, 1).isAllowed());
    }

    @Test
    void differentUsersHaveIndependentUserLevelQuotas() {
        configManager.setHierarchyPolicy(policyWith(
                RateLimitScope.USER, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS)));

        RequestContext user1 = RequestContext.builder().userId("h-isolated-u1").build();
        RequestContext user2 = RequestContext.builder().userId("h-isolated-u2").build();

        engine.process(user1, 2); // exhaust user1

        // user2 is completely unaffected
        assertTrue(engine.process(user2, 1).isAllowed());
        assertTrue(engine.process(user2, 1).isAllowed());
    }

    @Test
    void globalLimitIsSharedAcrossAllUsers() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext user1 = RequestContext.builder().userId("h-shared-u1").build();
        RequestContext user2 = RequestContext.builder().userId("h-shared-u2").build();

        assertTrue(engine.process(user1, 1).isAllowed()); // global: 2 remaining
        assertTrue(engine.process(user2, 1).isAllowed()); // global: 1 remaining
        assertTrue(engine.process(user1, 1).isAllowed()); // global: 0 remaining
        assertFalse(engine.process(user2, 1).isAllowed()); // global exhausted
    }

    @Test
    void premiumUserOverrideConfigGrantsHigherLimit() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        // Default user limit is 2; premium user gets 20
        configManager.setDefaultScopedConfig(RateLimitScope.USER,
                new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS));
        configManager.setOverrideScopedConfig(RateLimitScope.USER, "premium-user",
                new TokenBucketConfig(20, 5, 1, TimeUnit.SECONDS));

        RequestContext regular = RequestContext.builder().userId("regular-user").build();
        RequestContext premium = RequestContext.builder().userId("premium-user").build();

        // Regular user: only 2 allowed
        engine.process(regular, 2);
        assertFalse(engine.process(regular, 1).isAllowed());

        // Premium user: 20 capacity
        for (int i = 0; i < 10; i++) {
            assertTrue(engine.process(premium, 2).isAllowed());
        }
        assertFalse(engine.process(premium, 1).isAllowed());
    }

    @Test
    void retryAfterMillisIsPositiveWhenDeniedAtUserLevel() {
        configManager.setHierarchyPolicy(policyWith(
                RateLimitScope.USER, new TokenBucketConfig(2, 1, 60, TimeUnit.SECONDS)));

        RequestContext ctx = RequestContext.builder().userId("h-retry-user").build();

        engine.process(ctx, 2); // exhaust
        RateLimitResult denied = engine.process(ctx, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0);
    }

    @Test
    void retryAfterMillisIsPositiveWhenDeniedAtGlobalLevel() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(2, 1, 60, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext ctx = RequestContext.builder().userId("h-retry-global").build();

        engine.process(ctx, 2); // exhaust global
        RateLimitResult denied = engine.process(ctx, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0);
    }

    @Test
    void fixedWindowResetsAllowsRequestsAfterWindowExpires() {
        configManager.setHierarchyPolicy(policyWith(
                RateLimitScope.USER, new FixedWindowCounterConfig(3, 100, TimeUnit.MILLISECONDS)));

        RequestContext ctx = RequestContext.builder().userId("h-fw-reset").build();

        long baseMillis = 1000L;
        RequestTime t1 = new RequestTime(baseMillis, baseMillis * 1_000_000L);

        engine.process(ctx, 3, t1); // exhaust
        RateLimitResult denied = engine.process(ctx, 1, t1);
        assertFalse(denied.isAllowed());

        // Advance time past the window expiry without sleeping
        long waitMillis = Math.max(1L, denied.getRetryAfterMillis() + 1);
        long nextMillis = baseMillis + waitMillis;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000L);

        assertTrue(engine.process(ctx, 1, t2).isAllowed());
    }

    @Test
    void costIsAppliedToAllLevels() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));
        configManager.setHierarchyPolicy(policy);

        RequestContext ctx = RequestContext.builder().userId("h-cost").build();

        assertTrue(engine.process(ctx, 3).isAllowed()); // user: 2 remaining, global: 7 remaining
        assertFalse(engine.process(ctx, 3).isAllowed()); // user has only 2, needs 3
    }

    private HierarchicalRateLimitPolicy policyWith(RateLimitScope scope, RateLimitConfig config) {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(scope, config);
        return policy;
    }
}
