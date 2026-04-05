package org.nullpointer.ratelimiter.core.hierarchical;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.storage.config.RedisConfigRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import org.nullpointer.ratelimiter.utils.PlanPolicyLoader;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.utils.RateLimitKeyGenerator;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;
import org.nullpointer.ratelimiter.factory.StateRepositoryFactory;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalConfigurationManagerTest {
    private static RedisServer mockServer;
    private static JedisPool pool;
    private static PlanPolicyLoader planFactory;

    private ConfigRepository configStore;
    private StateRepository stateStore;
    private HierarchicalConfigurationManager manager;

    @BeforeAll
    static void startServer() throws IOException {
        mockServer = RedisServer.newRedisServer().start();
        pool = new JedisPool("localhost", mockServer.getBindPort());
        planFactory = PlanPolicyLoader.getInstance();
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

        configStore = new RedisConfigRepository(pool, new JacksonSerializer());
        stateStore = new InMemoryStateRepository();

        StateRepositoryFactory repoFactory = StateRepositoryFactory.getInstance();
        repoFactory.clearRegistry();
        manager = new HierarchicalConfigurationManager(configStore, stateStore, planFactory, repoFactory);
    }

    @Test
    void setAndGetHierarchicalState() {
        RateLimitKeyGenerator keyGenerator = new RateLimitKeyGenerator();
        RateLimitKey key = keyGenerator.generate(RateLimitScope.USER,
                RequestContext.builder().plan(SubscriptionPlan.FREE).userId("user1").build());
        RateLimitState state = new TokenBucketState(10, System.nanoTime());

        manager.setHierarchicalState(key, state);

        RateLimitState retrieved = manager.getHierarchicalState(key);
        assertNotNull(retrieved);
        assertSame(state, retrieved);
    }

    @Test
    void getHierarchicalStateReturnsNullWhenNotSet() {
        RateLimitKeyGenerator keyGenerator = new RateLimitKeyGenerator();
        RateLimitKey key = keyGenerator.generate(RateLimitScope.USER,
                RequestContext.builder().plan(SubscriptionPlan.FREE).userId("no-state").build());
        assertNull(manager.getHierarchicalState(key));
    }

    @Test
    void registerAndResolvePolicy() {
        TokenBucketConfig globalCfg = new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS);
        TokenBucketConfig userCfg   = new TokenBucketConfig(10,  1,  1, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, globalCfg, StateRepositoryType.IN_MEMORY));
        policy.addLevel(new RateLimitLevel(RateLimitScope.USER,   userCfg,   StateRepositoryType.IN_MEMORY));

        manager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext ctx = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("u1").build();
        HierarchicalRateLimitPolicy retrieved = manager.resolvePolicy(ctx);
        assertNotNull(retrieved);
        assertEquals(2, retrieved.getLevels().size());
    }

    @Test
    void policyIsPersistedInConfigStore() {
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(RateLimitScope.GLOBAL, cfg, StateRepositoryType.IN_MEMORY));
        manager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        // A second manager sharing the same config store must see the same policy.
        PlanPolicyLoader planFactory = PlanPolicyLoader.getInstance();
        StateRepositoryFactory repoFactory = StateRepositoryFactory.getInstance();

        HierarchicalConfigurationManager manager2 =
                new HierarchicalConfigurationManager(configStore, stateStore, planFactory, repoFactory);
        RequestContext ctx = RequestContext.builder().plan(SubscriptionPlan.FREE).build();
        HierarchicalRateLimitPolicy retrieved = manager2.resolvePolicy(ctx);
        assertNotNull(retrieved);
        assertEquals(1, retrieved.getLevels().size());
    }

    @Test
    void differentPlansResolveToDifferentPolicies() {
        TokenBucketConfig freeCfg = new TokenBucketConfig(10,  1, 1, TimeUnit.SECONDS);
        TokenBucketConfig premiumCfg = new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy freePolicy = new HierarchicalRateLimitPolicy();
        freePolicy.addLevel(new RateLimitLevel(RateLimitScope.USER, freeCfg, StateRepositoryType.IN_MEMORY));

        HierarchicalRateLimitPolicy premiumPolicy = new HierarchicalRateLimitPolicy();
        premiumPolicy.addLevel(new RateLimitLevel(RateLimitScope.USER, premiumCfg, StateRepositoryType.IN_MEMORY));

        manager.overridePlanPolicy(SubscriptionPlan.FREE, freePolicy);
        manager.overridePlanPolicy(SubscriptionPlan.PREMIUM, premiumPolicy);

        RequestContext freeCtx = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("u1").build();
        RequestContext premiumCtx = RequestContext.builder().plan(SubscriptionPlan.PREMIUM).userId("u2").build();

        // Each plan resolves to its own policy.
        // Configs are round-tripped through Redis serialization, so compare by field value.
        TokenBucketConfig resolvedFree = (TokenBucketConfig) manager.resolvePolicy(freeCtx).getLevel(RateLimitScope.USER).get().getConfig();
        assertEquals(freeCfg.getCapacity(), resolvedFree.getCapacity(), 0.0001);
        assertEquals(freeCfg.getRefillTokens(), resolvedFree.getRefillTokens(), 0.0001);

        TokenBucketConfig resolvedPremium = (TokenBucketConfig) manager.resolvePolicy(premiumCtx).getLevel(RateLimitScope.USER).get().getConfig();
        assertEquals(premiumCfg.getCapacity(), resolvedPremium.getCapacity(), 0.0001);
        assertEquals(premiumCfg.getRefillTokens(), resolvedPremium.getRefillTokens(), 0.0001);
    }

    @Test
    void resolveConfigReturnsPlanLevelDefault() {
        TokenBucketConfig cfg = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        RateLimitLevel level = new RateLimitLevel(RateLimitScope.USER, cfg, StateRepositoryType.IN_MEMORY);
        policy.addLevel(level);
        manager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        RequestContext ctx = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("user1").build();
        assertSame(cfg, manager.resolveConfig(ctx, level));
    }

    @Test
    void resolveConfigPrefersEntityOverride() {
        TokenBucketConfig defaultCfg  = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig overrideCfg = new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        RateLimitLevel level = new RateLimitLevel(RateLimitScope.USER, defaultCfg, StateRepositoryType.IN_MEMORY);
        policy.addLevel(level);
        manager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        // Register an entity-level override for "user1" within the FREE plan
        manager.overrideScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "user1", overrideCfg);

        RequestContext ctx = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("user1").build();
        // The scoped config is returned from Redis (round-trip serialization), so compare by field value.
        TokenBucketConfig resolved = (TokenBucketConfig) manager.resolveConfig(ctx, level);
        assertEquals(overrideCfg.getCapacity(), resolved.getCapacity(), 0.0001);
        assertEquals(overrideCfg.getRefillTokens(), resolved.getRefillTokens(), 0.0001);
    }

    @Test
    void resolveConfigFallsBackToPlanDefaultWhenNoEntityOverride() {
        TokenBucketConfig defaultCfg  = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig overrideCfg = new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS);

        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        RateLimitLevel level = new RateLimitLevel(RateLimitScope.USER, defaultCfg, StateRepositoryType.IN_MEMORY);
        policy.addLevel(level);
        manager.overridePlanPolicy(SubscriptionPlan.FREE, policy);

        // Override only set for "premium-user", not "regular-user"
        manager.overrideScopedConfig(SubscriptionPlan.PREMIUM, RateLimitScope.USER, "premium-user", overrideCfg);

        RequestContext ctx = RequestContext.builder().plan(SubscriptionPlan.FREE).userId("regular-user").build();
        assertSame(defaultCfg, manager.resolveConfig(ctx, level));
    }
}
