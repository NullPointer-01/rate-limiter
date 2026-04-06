package org.nullpointer.ratelimiter.core.hierarchical;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.storage.state.AsyncRedisStateRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryAtomicStateRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import org.nullpointer.ratelimiter.utils.PlanPolicyLoader;

import java.util.stream.Stream;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.factory.StateRepositoryFactory;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;
import org.nullpointer.ratelimiter.utils.RateLimitKeyGenerator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalRateLimitEngineTest {
    private static RedisServer mockServer;
    private static JedisPool pool;

    private StateRepositoryFactory registry;
    private PlanPolicyLoader planFactory;
    private HierarchicalConfigurationManager configManager;
    private HierarchicalRateLimitEngine engine;
    private AsyncRedisStateRepository asyncRedisRepo;

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

        registry = StateRepositoryFactory.getInstance();
        registry.clearRegistry();

        JacksonSerializer serializer = new JacksonSerializer();
        asyncRedisRepo = new AsyncRedisStateRepository(50, pool, serializer);
        asyncRedisRepo.start();

        registry.register(StateRepositoryType.IN_MEMORY, new InMemoryStateRepository());
        registry.register(StateRepositoryType.REDIS, new RedisStateRepository(pool, serializer));
        registry.register(StateRepositoryType.ASYNC_REDIS, asyncRedisRepo);
        registry.registerAtomic(StateRepositoryType.IN_MEMORY_ATOMIC, new InMemoryAtomicStateRepository());
    }

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

    private void buildSetup(String configFile) {
        planFactory = PlanPolicyLoader.withConfig(configFile);
        ConfigRepository configStore = new InMemoryConfigRepository();
        StateRepository stateStore = new RedisStateRepository(pool, new JacksonSerializer());
        configManager = new HierarchicalConfigurationManager(configStore, stateStore, planFactory, registry);
        engine = new HierarchicalRateLimitEngine(configManager);
    }

    @AfterEach
    void tearDown() {
        if (asyncRedisRepo != null) {
            asyncRedisRepo.stop();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void singleLevelAllowsWithinLimit(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.REDIS));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        assertTrue(engine.process(ctx("user1"), 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void singleLevelDeniesWhenExhausted(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1");
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 2).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void multiLevelAllowsWhenAllLevelsPermit(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(50,  5,  1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, new TokenBucketConfig(10,  1,  1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        assertTrue(engine.process(ctx("user1", "/api/data"), 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void multiLevelDeniesWhenMostRestrictiveLevelExhausted(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, new TokenBucketConfig(3,   1,  1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1", "/api/data");
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed()); // endpoint exhausted
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void multiLevelDeniesWhenGlobalLevelExhausted(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(2,   1, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user-1", "/api/data");
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed()); // global exhausted
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void costIsAppliedToAllLevels(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(5,  1, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1");
        assertTrue(engine.process(context, 3).isAllowed()); // user: 2 remaining
        assertFalse(engine.process(context, 3).isAllowed()); // user has only 2, needs 3
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void stateIsInitializedOnFirstRequest(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RateLimitKeyGenerator keyGenerator = new RateLimitKeyGenerator();
        RateLimitKey stateKey = keyGenerator.generate(RateLimitScope.USER, ctx("user1"));

        StateRepository inMemoryRepo = StateRepositoryFactory.getInstance().resolve(StateRepositoryType.IN_MEMORY);
        assertNull(inMemoryRepo.getHierarchicalState(stateKey));

        engine.process(ctx("user1"), 1);

        assertNotNull(inMemoryRepo.getHierarchicalState(stateKey));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void separateKeysHaveIndependentState(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.ASYNC_REDIS));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext user1 = ctx("user1");
        RequestContext user2 = ctx("user2");

        assertTrue(engine.process(user1, 1).isAllowed());
        assertTrue(engine.process(user1, 1).isAllowed());
        assertFalse(engine.process(user1, 1).isAllowed());

        assertTrue(engine.process(user2, 1).isAllowed());
        assertTrue(engine.process(user2, 1).isAllowed());
        assertFalse(engine.process(user2, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void sharedLevelStateAcrossDifferentRequests(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(3,  1,  1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(10, 10, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext user1 = ctx("user1");
        RequestContext user2 = ctx("user2");

        assertTrue(engine.process(user1, 1).isAllowed()); // global: 2 left
        assertTrue(engine.process(user1, 1).isAllowed()); // global: 1 left
        assertTrue(engine.process(user2, 1).isAllowed()); // global: 0 left
        assertFalse(engine.process(user2, 1).isAllowed()); // global exhausted
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void multiAlgorithmLevels(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS),           StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, new FixedWindowCounterConfig(5, 1, TimeUnit.MINUTES), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1", "/api/data");

        for (int i = 0; i < 5; i++) {
            assertTrue(engine.process(context, 1).isAllowed(), "Request " + (i + 1) + " should be allowed");
        }
        assertFalse(engine.process(context, 1).isAllowed()); // FixedWindowCounter exhausted
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void multiLevelHierarchyEnforcement(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(1000, 100, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(100,  10,  1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, new TokenBucketConfig(5,    1,   1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1", "/api/data");

        for (int i = 0; i < 5; i++) {
            assertTrue(engine.process(context, 1).isAllowed());
        }
        assertFalse(engine.process(context, 1).isAllowed()); // endpoint is bottleneck
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void middleLevelDeniesWhileOthersAllow(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(2,   1,  1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY)); // bottleneck
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1", "/api/test");

        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed()); // denied by USER level
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void noPhantomConsumptionWhenLaterLevelDenies(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig globalCfg   = new TokenBucketConfig(5, 1, 10, TimeUnit.SECONDS);
        TokenBucketConfig endpointCfg = new TokenBucketConfig(2, 1, 10, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL,   globalCfg,   StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.ENDPOINT, endpointCfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1", "/api/data");

        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed()); // endpoint denied

        // Swap to global-only policy to verify global consumed only 2 tokens (not 3)
        HierarchicalRateLimitPolicy globalOnlyPolicy = new HierarchicalRateLimitPolicy();
        globalOnlyPolicy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, globalCfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, globalOnlyPolicy);

        // Global should have 3 remaining (5 - 2)
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 1).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void noPhantomConsumptionOnFirstLevelWhenSecondDenies(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig globalCfg = new TokenBucketConfig(10, 1, 10, TimeUnit.SECONDS);
        TokenBucketConfig userCfg   = new TokenBucketConfig(1,  1, 10, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, globalCfg, StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, userCfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1");
        assertTrue(engine.process(context, 1).isAllowed());

        // Next 5 denied — user level exhausted
        for (int i = 0; i < 5; i++) {
            assertFalse(engine.process(context, 1).isAllowed());
        }

        // Switch to global-only; global should have 9 remaining (10 - 1)
        HierarchicalRateLimitPolicy globalOnlyPolicy = new HierarchicalRateLimitPolicy();
        globalOnlyPolicy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, globalCfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, globalOnlyPolicy);

        for (int i = 0; i < 9; i++) {
            assertTrue(engine.process(context, 1).isAllowed(),
                    "Request " + (i + 1) + " should be allowed, global tokens should not have been phantom-consumed");
        }
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void defaultFreePolicyAllowsInitialRequest(String configFile) {
        buildSetup(configFile);
        assertTrue(engine.process(ctx("user1", "tenant-a", "/api/data"), 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void defaultFreePolicyEndpointLevelDeniesAfterCapacity(String configFile) {
        buildSetup(configFile);
        RequestContext ctx = ctx("user1", "tenant-a", "/api/data");
        int cap = minFreeCapacity();

        for (int i = 0; i < cap; i++) {
            assertTrue(engine.process(ctx, 1).isAllowed(), "Request " + (i + 1) + " should be allowed");
        }
        assertFalse(engine.process(ctx, 1).isAllowed(), "Request " + (cap + 1) + " should be denied by ENDPOINT level");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void defaultFreePolicyUsersHaveIndependentAsyncRedisQuota(String configFile) {
        buildSetup(configFile);
        // USER level uses ASYNC_REDIS – each user has its own sliding-window state.
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS), StateRepositoryType.ASYNC_REDIS));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext user1 = ctx("user1", "tenant-a", "/api/data");
        RequestContext user2 = ctx("user2", "tenant-a", "/api/data");

        assertTrue(engine.process(user1, 1).isAllowed());
        assertTrue(engine.process(user1, 1).isAllowed());
        assertFalse(engine.process(user1, 1).isAllowed()); // user1 exhausted

        // user2 has its own independent ASYNC_REDIS bucket
        assertTrue(engine.process(user2, 1).isAllowed());
        assertTrue(engine.process(user2, 1).isAllowed());
        assertFalse(engine.process(user2, 1).isAllowed()); // user2 exhausted
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void redisBackedSingleUserLevelDeniesWhenExhausted(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.REDIS));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1");
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 2).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void redisBackedMultiLevelGlobalDeniesIndependentOfPerUser(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS), StateRepositoryType.REDIS));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, new TokenBucketConfig(10, 5, 1, TimeUnit.SECONDS), StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        assertTrue(engine.process(ctx("user1"), 1).isAllowed());
        assertTrue(engine.process(ctx("user2"), 1).isAllowed()); // different user, shared global
        assertFalse(engine.process(ctx("user3"), 1).isAllowed()); // global exhausted
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void asyncRedisBackedSingleUserLevelAllowsWithinLimit(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.ASYNC_REDIS));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        assertTrue(engine.process(ctx("user1"), 1).isAllowed());
        assertTrue(engine.process(ctx("user1"), 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void asyncRedisBackedSingleUserLevelDeniesWhenExhausted(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.ASYNC_REDIS));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext context = ctx("user1");
        assertTrue(engine.process(context, 1).isAllowed());
        assertTrue(engine.process(context, 2).isAllowed());
        assertFalse(engine.process(context, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void asyncRedisBackedStateFlushedToRedisAfterSync(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.ASYNC_REDIS));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        engine.process(ctx("user1"), 1);
        engine.process(ctx("user1"), 1);

        // Force immediate flush to Redis
        asyncRedisRepo.stop();

        // Verify the state was actually written to Redis by reading it via a plain RedisStateRepository
        RateLimitKeyGenerator keyGen = new RateLimitKeyGenerator();
        RateLimitKey stateKey = keyGen.generate(RateLimitScope.USER, ctx("user1"));
        StateRepository redisOnly = new RedisStateRepository(pool, new JacksonSerializer());

        assertNotNull(redisOnly.getHierarchicalState(stateKey),
                "State should have been flushed from AsyncRedis local cache into Redis");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void factoryDefaultPolicyUsedWhenNoRepoOverride(String configFile) {
        buildSetup(configFile);
        RequestContext ctx = RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .tenantId("t1")
                .userId("u1")
                .apiPath("/api/test")
                .build();

        RateLimitResult result = engine.process(ctx, 1);
        assertTrue(result.isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void repoPolicyTakesPriorityOverFactoryDefault(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig strictCfg = new TokenBucketConfig(1, 1, 10, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy repoPolicy = new HierarchicalRateLimitPolicy();
        repoPolicy.addLevel(new RateLimitLevel(RateLimitScope.USER, strictCfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, repoPolicy);

        RequestContext ctx = RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .userId("u1")
                .build();

        assertTrue(engine.process(ctx, 1).isAllowed());
        assertFalse(engine.process(ctx, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void scopeConfigOverrideAppliedForSpecificUser(String configFile) {
        buildSetup(configFile);
        TokenBucketConfig overrideCfg = new TokenBucketConfig(2, 1, 10, TimeUnit.SECONDS);
        configManager.overrideScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "vip-user", overrideCfg);

        RequestContext vipCtx = RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .tenantId("t1")
                .userId("vip-user")
                .apiPath("/api/test")
                .build();

        assertTrue(engine.process(vipCtx, 1).isAllowed());
        assertTrue(engine.process(vipCtx, 1).isAllowed());
        assertFalse(engine.process(vipCtx, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void differentPlansGetDifferentLimitsFromFactory(String configFile) {
        buildSetup(configFile);
        RequestContext freeCtx = RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .tenantId("t1")
                .userId("u1")
                .apiPath("/api/test")
                .build();

        RequestContext enterpriseCtx = RequestContext.builder()
                .plan(SubscriptionPlan.ENTERPRISE)
                .tenantId("t2")
                .userId("u2")
                .apiPath("/api/test")
                .build();

        int cap = minFreeCapacity();

        for (int i = 0; i < cap; i++) {
            engine.process(freeCtx, 1);
        }
        assertFalse(engine.process(freeCtx, 1).isAllowed());

        for (int i = 0; i < cap; i++) {
            RateLimitResult result = engine.process(enterpriseCtx, 1);
            assertTrue(result.isAllowed());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("multiPlanConfigFiles")
    void pipelineDeniesIfAnyLevelDenies(String configFile) {
        buildSetup(configFile);
        RequestContext ctx = RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .tenantId("t1")
                .userId("u1")
                .apiPath("/api/test")
                .build();

        HierarchicalRateLimitPolicy policy = planFactory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        assertNotNull(policy);
        assertTrue(policy.getLevels().size() > 1);

        int cap = minFreeCapacity();
        for (int i = 0; i < cap; i++) {
            assertTrue(engine.process(ctx, 1).isAllowed(), "Request " + (i + 1) + " should be allowed");
        }
        assertFalse(engine.process(ctx, 1).isAllowed());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void resolutionOrderFactoryThenRepoOverride(String configFile) {
        buildSetup(configFile);
        HierarchicalRateLimitPolicy factoryPolicy = configManager.resolvePolicy(
                RequestContext.builder().plan(SubscriptionPlan.FREE).userId("u1").tenantId("t1").apiPath("/x").build());
        assertNotNull(factoryPolicy);

        TokenBucketConfig overrideCfg = new TokenBucketConfig(1, 1, 10, TimeUnit.SECONDS);
        HierarchicalRateLimitPolicy repoPolicy = new HierarchicalRateLimitPolicy();
        repoPolicy.addLevel(new RateLimitLevel(RateLimitScope.USER, overrideCfg, StateRepositoryType.IN_MEMORY));
        configManager.overridePlanPolicy(SubscriptionPlan.FREE, repoPolicy);

        HierarchicalRateLimitPolicy resolved = configManager.resolvePolicy(
                RequestContext.builder().plan(SubscriptionPlan.FREE).userId("u1").build());
        assertEquals(1, resolved.getLevels().size());
        TokenBucketConfig resolvedCfg = (TokenBucketConfig) resolved.getLevel(RateLimitScope.USER).orElseThrow().getConfig();
        assertEquals(overrideCfg.getCapacity(), resolvedCfg.getCapacity(), 0.0001);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("configFiles")
    void resolveStateRepositoryUsesLevelDirectRepoWhenSet(String configFile) {
        buildSetup(configFile);
        StateRepository directRepo = new InMemoryStateRepository();
        StateRepositoryFactory.getInstance().register(StateRepositoryType.IN_MEMORY, directRepo);

        TokenBucketConfig cfg = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        RateLimitLevel level = new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.IN_MEMORY);

        StateRepository resolved = configManager.resolveStateRepository(level);
        assertSame(directRepo, resolved);
    }

    private int minFreeCapacity() {
        HierarchicalRateLimitPolicy policy = planFactory.getDefaultPolicies().get(SubscriptionPlan.FREE);
        return policy.getLevels().stream()
                .mapToInt(level -> (int) capacityOf(level.getConfig()))
                .min()
                .orElseThrow(() -> new IllegalStateException("No levels in FREE policy"));
    }

    private long capacityOf(RateLimitConfig config) {
        if (config instanceof TokenBucketConfig c) {
            return (long) c.getCapacity();
        }
        if (config instanceof FixedWindowCounterConfig c) {
            return c.getCapacity();
        }
        if (config instanceof SlidingWindowCounterConfig c) {
            return c.getCapacity();
        }
        if (config instanceof SlidingWindowConfig c) {
            return c.getMaxCost();
        }
        throw new IllegalArgumentException("Unknown RateLimitConfig type: " + config.getClass().getSimpleName());
    }

    private RequestContext ctx(String userId) {
        return RequestContext.builder().plan(SubscriptionPlan.FREE).userId(userId).build();
    }

    private RequestContext ctx(String userId, String apiPath) {
        return RequestContext.builder().plan(SubscriptionPlan.FREE).userId(userId).apiPath(apiPath).build();
    }

    private RequestContext ctx(String userId, String tenantId, String apiPath) {
        return RequestContext.builder()
                .plan(SubscriptionPlan.FREE)
                .userId(userId)
                .tenantId(tenantId)
                .apiPath(apiPath)
                .build();
    }
}
