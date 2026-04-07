package org.nullpointer.ratelimiter.core.hierarchical;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.factory.StateRepositoryFactory;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryAtomicStateRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.RedisLuaStateRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import org.nullpointer.ratelimiter.utils.PlanPolicyLoader;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RedisLuaHierarchicalRateLimitEngineTest {

    private static JedisPool pool;
    private StateRepositoryFactory registry;
    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimitEngine engine;


    @BeforeAll
    static void startPool() {
        pool = new JedisPool("localhost", 6379);
    }

    @AfterAll
    static void stopPool() {
        pool.close();
    }

    @BeforeEach
    void setUp() {
        try (Jedis jedis = pool.getResource()) {
            jedis.flushAll();
        }

        registry = new StateRepositoryFactory();
        JacksonSerializer serializer = new JacksonSerializer();

        registry.register(StateRepositoryType.REDIS, new RedisStateRepository(pool, serializer));
        registry.register(StateRepositoryType.IN_MEMORY, new InMemoryStateRepository());
        registry.registerAtomic(StateRepositoryType.IN_MEMORY_ATOMIC, new InMemoryAtomicStateRepository());
        registry.registerAtomic(StateRepositoryType.REDIS_ATOMIC, new RedisLuaStateRepository(pool));

        configManager = new HierarchicalConfigurationManager(
                new InMemoryConfigRepository(),
                new InMemoryStateRepository(),
                PlanPolicyLoader.withConfig("rate-limiter-redis-lua.yml"),
                registry
        );
        engine = new HierarchicalRateLimitEngine(configManager);
    }

    private RequestContext userCtx(String userId) {
        return RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .userId(userId)
                .build();
    }

    private void configureSingleLevel(RateLimitScope scope, Object config) {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(scope, (org.nullpointer.ratelimiter.model.config.RateLimitConfig) config, StateRepositoryType.REDIS_ATOMIC));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);
    }

    @Test
    void exactLimitAllowedConcurrentTokenBucket() throws Exception {
        int capacity = 10;
        configureSingleLevel(RateLimitScope.USER,
                new TokenBucketConfig(capacity, 0, 1, TimeUnit.HOURS));

        RequestContext ctx = userCtx("tb-concurrent");
        int threads = capacity * 2; // 20 threads for 10 capacity

        AtomicInteger allowed = new AtomicInteger();
        runConcurrently(threads, () -> {
            if (engine.process(ctx, 1).isAllowed()) allowed.incrementAndGet();
        });

        assertEquals(capacity, allowed.get(),
                "Token bucket: exactly " + capacity + " should be allowed, got " + allowed.get());
    }

    @Test
    void exactLimitAllowedConcurrentFixedWindow() throws Exception {
        int capacity = 10;
        configureSingleLevel(RateLimitScope.USER,
                new FixedWindowCounterConfig(capacity, 60, TimeUnit.SECONDS));

        RequestContext ctx = userCtx("fw-concurrent");
        int threads = capacity * 2;

        AtomicInteger allowed = new AtomicInteger();
        runConcurrently(threads, () -> {
            if (engine.process(ctx, 1).isAllowed()) allowed.incrementAndGet();
        });

        assertEquals(capacity, allowed.get(),
                "Fixed window: exactly " + capacity + " should be allowed, got " + allowed.get());
    }

    @Test
    void exactLimitAllowedConcurrentSlidingWindowCounter() throws Exception {
        int capacity = 10;
        configureSingleLevel(RateLimitScope.USER,
                new SlidingWindowCounterConfig(capacity, 60, TimeUnit.SECONDS));

        RequestContext ctx = userCtx("swc-concurrent");
        int threads = capacity * 2;

        AtomicInteger allowed = new AtomicInteger();
        runConcurrently(threads, () -> {
            if (engine.process(ctx, 1).isAllowed()) allowed.incrementAndGet();
        });

        assertEquals(capacity, allowed.get(),
                "Sliding window counter: exactly " + capacity + " should be allowed, got " + allowed.get());
    }

    @Test
    void exactLimitAllowedConcurrentSlidingWindow() throws Exception {
        int maxCost = 10;
        configureSingleLevel(RateLimitScope.USER,
                new SlidingWindowConfig(maxCost, 60, TimeUnit.SECONDS));

        RequestContext ctx = userCtx("sw-concurrent");
        int threads = maxCost * 2;

        AtomicInteger allowed = new AtomicInteger();
        runConcurrently(threads, () -> {
            if (engine.process(ctx, 1).isAllowed()) allowed.incrementAndGet();
        });

        assertEquals(maxCost, allowed.get(),
                "Sliding window (log): exactly " + maxCost + " should be allowed, got " + allowed.get());
    }

    @Test
    void twoDistributedNodesNoOverConsumption() throws Exception {
        int capacity = 10;
        int threadsPerNode = 10; // each node gets 10 threads; total 20 requests for capacity 10

        // Node 2 — separate engine instance sharing same redis instance
        HierarchicalConfigurationManager configManager2 = new HierarchicalConfigurationManager(
                new InMemoryConfigRepository(),
                new InMemoryStateRepository(),
                PlanPolicyLoader.withConfig("rate-limiter-redis-lua.yml"),
                registry
        );
        HierarchicalRateLimitEngine engine2 = new HierarchicalRateLimitEngine(configManager2);

        configureSingleLevel(RateLimitScope.USER,
                new TokenBucketConfig(capacity, 0, 1, TimeUnit.HOURS));

        // engine2 shares the same configManager2
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER,
                new TokenBucketConfig(capacity, 0, 1, TimeUnit.HOURS),
                StateRepositoryType.REDIS_ATOMIC));
        configManager2.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext ctx = userCtx("dist-user");
        AtomicInteger totalAllowed = new AtomicInteger();

        ExecutorService exec = Executors.newFixedThreadPool(threadsPerNode * 2);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < threadsPerNode; i++) {
            tasks.add(() -> {
                if (engine.process(ctx, 1).isAllowed()) totalAllowed.incrementAndGet();
                return null;
            });
            tasks.add(() -> {
                if (engine2.process(ctx, 1).isAllowed()) totalAllowed.incrementAndGet();
                return null;
            });
        }

        List<Future<Void>> futures = exec.invokeAll(tasks);
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
        for (Future<Void> f : futures) f.get();

        assertEquals(capacity, totalAllowed.get(),
                "Distributed nodes: expected exactly " + capacity + " total allowed, got " + totalAllowed.get());
    }

    @Test
    void noStateLeakAcrossUsers() throws Exception {
        int capacityPerUser = 5;
        int userCount = 4;
        configureSingleLevel(RateLimitScope.USER,
                new TokenBucketConfig(capacityPerUser, 0, 1, TimeUnit.HOURS));

        AtomicInteger totalAllowed = new AtomicInteger();
        ExecutorService exec = Executors.newFixedThreadPool(userCount * capacityPerUser);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int u = 0; u < userCount; u++) {
            final String userId = "isolated-user-" + u;
            for (int r = 0; r < capacityPerUser; r++) {
                tasks.add(() -> {
                    if (engine.process(userCtx(userId), 1).isAllowed()) totalAllowed.incrementAndGet();
                    return null;
                });
            }
        }

        List<Future<Void>> futures = exec.invokeAll(tasks);
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
        for (Future<Void> f : futures) f.get();

        // All users × capacityPerUser should succeed, since keys are independent
        assertEquals(userCount * capacityPerUser, totalAllowed.get(),
                "Each user's quota is independent; all " + (userCount * capacityPerUser) + " should be allowed");
    }

    @Test
    void hierarchicalLevelsDenyAtCorrectLevelUserBeforeTenant() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.TENANT, new TokenBucketConfig(5, 0, 1, TimeUnit.HOURS), StateRepositoryType.REDIS_ATOMIC));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(3, 0, 1, TimeUnit.HOURS), StateRepositoryType.REDIS_ATOMIC));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext ctx = RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .userId("h-user-1")
                .tenantId("h-tenant-1")
                .build();

        assertTrue(engine.process(ctx, 1).isAllowed());  // user: 2 remaining, tenant: 4 remaining
        assertTrue(engine.process(ctx, 1).isAllowed());  // user: 1 remaining, tenant: 3 remaining
        assertTrue(engine.process(ctx, 1).isAllowed());  // user: 0 remaining, tenant: 2 remaining

        RateLimitResult denied = engine.process(ctx, 1); // user exhausted
        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0, "retryAfterMillis should be positive");
    }

    @Test
    void hierarchicalLevelsDenyAtCorrectLevelTenantSharedAcrossUsers() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.TENANT, new TokenBucketConfig(4, 0, 1, TimeUnit.HOURS), StateRepositoryType.REDIS_ATOMIC));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(3, 0, 1, TimeUnit.HOURS), StateRepositoryType.REDIS_ATOMIC));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        String tenant = "shared-tenant";
        RequestContext ctxA = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("ua").tenantId(tenant).build();
        RequestContext ctxB = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("ub").tenantId(tenant).build();

        // userA consumes 3 → tenant has 1 remaining
        assertTrue(engine.process(ctxA, 1).isAllowed());
        assertTrue(engine.process(ctxA, 1).isAllowed());
        assertTrue(engine.process(ctxA, 1).isAllowed());

        // userB uses 1 → tenant exhausted
        assertTrue(engine.process(ctxB, 1).isAllowed());

        // Both users now denied by tenant cap
        assertFalse(engine.process(ctxA, 1).isAllowed());
        assertFalse(engine.process(ctxB, 1).isAllowed());
    }

    @Test
    void fixedWindowResetAllowsRequestsAfterWindowAdvances() {
        configureSingleLevel(RateLimitScope.USER,
                new FixedWindowCounterConfig(3, 100, TimeUnit.MILLISECONDS));

        RequestContext ctx = userCtx("reset-user");
        long baseMs = 1_000L;
        RequestTime t1 = new RequestTime(baseMs, baseMs * 1_000_000L);

        engine.process(ctx, 3, t1); // exhaust
        RateLimitResult denied = engine.process(ctx, 1, t1);
        assertFalse(denied.isAllowed());

        // Advance past the window boundary
        long nextWindowMs = baseMs + denied.getRetryAfterMillis() + 1;
        RequestTime t2 = new RequestTime(nextWindowMs, nextWindowMs * 1_000_000L);

        assertTrue(engine.process(ctx, 1, t2).isAllowed(), "Should be allowed in the new window");
    }

    @Test
    void tokenBucketRefillAllowsRequestsAfterRefillDelay() {
        // 1 token per second, capacity 1 — refills at 1 req/s
        configureSingleLevel(RateLimitScope.USER,
                new TokenBucketConfig(1, 1, 1, TimeUnit.SECONDS));

        RequestContext ctx = userCtx("refill-user");
        long baseMs = 2_000L;
        RequestTime t1 = new RequestTime(baseMs, baseMs * 1_000_000L);

        assertTrue(engine.process(ctx, 1, t1).isAllowed()); // consume the single token
        assertFalse(engine.process(ctx, 1, t1).isAllowed()); // now empty

        // Advance by the retry delay (should be ~1000ms) + 1ms buffer
        RateLimitResult denied = engine.process(ctx, 1, t1);
        long retryMs = denied.getRetryAfterMillis() + 1;
        long nextMs = baseMs + retryMs;
        RequestTime t2 = new RequestTime(nextMs, nextMs * 1_000_000L);

        assertTrue(engine.process(ctx, 1, t2).isAllowed(), "Token should have refilled by now");
    }

    @Test
    void mixedPolicyLuaAndInMemoryRoutesToSynchronizedFallback() throws Exception {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER,     new TokenBucketConfig(10, 0, 1, TimeUnit.HOURS), StateRepositoryType.REDIS_ATOMIC));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, new TokenBucketConfig(5,  0, 1, TimeUnit.HOURS), StateRepositoryType.IN_MEMORY_ATOMIC));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext ctx = RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .userId("mixed-user")
                .apiPath("/v1/test")
                .httpMethod("GET")
                .build();

        // Endpoint capacity is the bottleneck
        int threads = 50;
        AtomicInteger allowed = new AtomicInteger();
        runConcurrently(threads, () -> {
            if (engine.process(ctx, 1).isAllowed()) allowed.incrementAndGet();
        });

        assertEquals(5, allowed.get(),
                "Endpoint capacity should be bottleneck; allowed=" + allowed.get());
    }

    private void runConcurrently(int threadCount, Runnable task) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(threadCount);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            tasks.add(() -> { task.run(); return null; });
        }
        List<Future<Void>> futures = exec.invokeAll(tasks);
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
        for (Future<Void> f : futures) f.get(); // re-throw any assertion errors
    }
}

