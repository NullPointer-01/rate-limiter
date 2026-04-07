package org.nullpointer.ratelimiter.storage;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.*;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.factory.StateRepositoryFactory;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.config.RedisConfigRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RedisConfigRepositoryTest {

    private static RedisServer mockServer;
    private static JedisPool pool;

    private ConfigRepository repository;

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
        try (Jedis jedis = pool.getResource()) {
            jedis.flushAll();
        }
        StateRepositoryFactory registry = new StateRepositoryFactory();

        JacksonSerializer serializer = new JacksonSerializer();
        repository = new RedisConfigRepository(pool, serializer);
        registry.register(StateRepositoryType.REDIS, new RedisStateRepository(pool, serializer));
    }

    @Test
    void defaultConfigStoresAndRetrievesTokenBucketConfig() {
        TokenBucketConfig config = new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS);

        repository.setDefaultConfig(config);

        RateLimitConfig retrieved = repository.getDefaultConfig();
        assertNotNull(retrieved);
        assertInstanceOf(TokenBucketConfig.class, retrieved);
        TokenBucketConfig result = (TokenBucketConfig) retrieved;
        assertEquals(config.getCapacity(), result.getCapacity(), 0.0001);
        assertEquals(config.getRefillTokens(), result.getRefillTokens(), 0.0001);
        assertEquals(config.getRefillIntervalMillis(), result.getRefillIntervalMillis(), 0.0001);
    }

    @Test
    void specificConfigRoundTripsTokenBucketConfig() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tbUser").build();
        TokenBucketConfig config = new TokenBucketConfig(50, 5, 2, TimeUnit.MINUTES);

        repository.setConfig(key, config);

        TokenBucketConfig result = (TokenBucketConfig) repository.getConfig(key);
        assertNotNull(result);
        assertEquals(config.getCapacity(), result.getCapacity(), 0.0001);
        assertEquals(config.getRefillIntervalMillis(), result.getRefillIntervalMillis(), 0.0001);
    }

    @Test
    void specificConfigRoundTripsFixedWindowCounterConfig() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fwUser").build();
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(200, 1, TimeUnit.MINUTES);

        repository.setConfig(key, config);

        FixedWindowCounterConfig result = (FixedWindowCounterConfig) repository.getConfig(key);
        assertNotNull(result);
        assertEquals(config.getCapacity(), result.getCapacity());
        assertEquals(config.getWindowSizeMillis(), result.getWindowSizeMillis());
    }

    @Test
    void specificConfigRoundTripsSlidingWindowConfig() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swUser").build();
        SlidingWindowConfig config = new SlidingWindowConfig(300, 30, TimeUnit.SECONDS);

        repository.setConfig(key, config);

        SlidingWindowConfig result = (SlidingWindowConfig) repository.getConfig(key);
        assertNotNull(result);
        assertEquals(config.getMaxCost(), result.getMaxCost());
        assertEquals(config.getWindowSizeMillis(), result.getWindowSizeMillis());
    }

    @Test
    void specificConfigRoundTripsSlidingWindowCounterConfig() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swcUser").build();
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(400, 1, TimeUnit.HOURS);

        repository.setConfig(key, config);

        SlidingWindowCounterConfig result = (SlidingWindowCounterConfig) repository.getConfig(key);
        assertNotNull(result);
        assertEquals(config.getCapacity(), result.getCapacity());
        assertEquals(config.getWindowSizeMillis(), result.getWindowSizeMillis());
    }

    @Test
    void specificConfigReturnsNullForUnknownKey() {
        RateLimitKey key = RateLimitKey.builder().setUserId("nobody").build();
        assertNull(repository.getConfig(key));
    }

    @Test
    void getOrDefaultConfigReturnsSpecificConfigWhenPresent() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig specificConfig = new TokenBucketConfig(25, 5, 1, TimeUnit.SECONDS);
        RateLimitKey key = RateLimitKey.builder().setUserId("specificUser").build();

        repository.setDefaultConfig(defaultConfig);
        repository.setConfig(key, specificConfig);

        assertEquals(25.0, ((TokenBucketConfig) repository.getOrDefaultConfig(key)).getCapacity(), 0.0001);
    }

    @Test
    void getOrDefaultConfigFallsBackToDefaultWhenNoSpecificConfig() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        RateLimitKey key = RateLimitKey.builder().setUserId("fallbackUserXyz").build();

        repository.setDefaultConfig(defaultConfig);

        assertEquals(10.0, ((TokenBucketConfig) repository.getOrDefaultConfig(key)).getCapacity(), 0.0001);
    }

    @Test
    void hierarchyPolicyStoresAndRetrievesWithLevels() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addLevel(new RateLimitLevel(
            RateLimitScope.GLOBAL,
            new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS),
            StateRepositoryType.IN_MEMORY
        ));
        policy.addLevel(new RateLimitLevel(
            RateLimitScope.USER,
            new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS),
            StateRepositoryType.IN_MEMORY
        ));

        repository.setPlanPolicy(SubscriptionPlan.FREE, policy);
        HierarchicalRateLimitPolicy result = repository.getPlanPolicy(SubscriptionPlan.FREE);

        assertNotNull(result);
        assertEquals(2, result.getLevels().size());
        assertEquals(RateLimitScope.GLOBAL, result.getLevels().get(0).getScope());
        assertEquals(RateLimitScope.USER, result.getLevels().get(1).getScope());
    }

    @Test
    void hierarchyPolicyPreservesSortOrder() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        // Added in reverse order — addPolicy sorts them

        policy.addLevel(new RateLimitLevel(
            RateLimitScope.ENDPOINT,
            new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS),
            StateRepositoryType.IN_MEMORY
        ));
        policy.addLevel(new RateLimitLevel(
            RateLimitScope.GLOBAL,
            new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS),
            StateRepositoryType.IN_MEMORY
        ));

        repository.setPlanPolicy(SubscriptionPlan.FREE, policy);
        HierarchicalRateLimitPolicy result = repository.getPlanPolicy(SubscriptionPlan.FREE);

        assertNotNull(result);
        assertEquals(RateLimitScope.GLOBAL, result.getLevels().get(0).getScope());
        assertEquals(RateLimitScope.ENDPOINT, result.getLevels().get(1).getScope());
    }

    @Test
    void scopedConfigStoresOverrideSeparately() {
        TokenBucketConfig defaultUserConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig overrideUserConfig = new TokenBucketConfig(25, 5, 1, TimeUnit.SECONDS);

        repository.setPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "DEFAULT", defaultUserConfig);
        repository.setPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "user-1", overrideUserConfig);

        assertEquals(10.0, ((TokenBucketConfig) repository.getPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "DEFAULT").orElse(null)).getCapacity(), 0.0001);
        assertEquals(25.0, ((TokenBucketConfig) repository.getPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.USER, "user-1").orElse(null)).getCapacity(), 0.0001);
    }

    @Test
    void scopedConfigDifferentScopesStoredIndependently() {
        repository.setPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.TENANT, "tenant-1", new TokenBucketConfig(500, 50, 1, TimeUnit.SECONDS));
        repository.setPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.IP, "ip-1", new TokenBucketConfig(20, 2, 1, TimeUnit.SECONDS));

        assertEquals(500.0, ((TokenBucketConfig) repository.getPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.TENANT, "tenant-1").orElse(null)).getCapacity(), 0.0001);
        assertEquals(20.0, ((TokenBucketConfig) repository.getPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.IP, "ip-1").orElse(null)).getCapacity(), 0.0001);
    }

    @Test
    void scopedConfigReturnsNullForUnknownIdentifier() {
        assertTrue(repository.getPlanScopedConfig(SubscriptionPlan.FREE, RateLimitScope.REGION, "unknownRegion").isEmpty());
    }
}
