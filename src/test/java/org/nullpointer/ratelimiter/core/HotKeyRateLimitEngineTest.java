package org.nullpointer.ratelimiter.core;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.factory.CircuitBreakerFactory;
import org.nullpointer.ratelimiter.hotkey.HotKeyConfig;
import org.nullpointer.ratelimiter.hotkey.KeyTemperature;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.storage.config.RedisConfigRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HotKeyRateLimitEngineTest {

    private static final int CAPACITY = 100;

    // 3 requests / 5 s = HOT, 2 requests / 5s = WARM
    private static final HotKeyConfig HOT_KEY_CONFIG = new HotKeyConfig.Builder()
            .warmThreshold(2)
            .hotThreshold(3)
            .detectionWindowMillis(30000)
            .syncIntervalMillis(1000)
            .hotLocalQuotaFraction(1.0)
            .build();

    private ConfigurationManager manager;
    private RateLimitEngine engine;
    private RateLimitKey key;

    private static RedisServer mockServer;
    private static JedisPool pool;

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
        manager = new ConfigurationManager(
                new RedisConfigRepository(pool, serializer),
                new RedisStateRepository(pool, serializer)
        );        engine = new RateLimitEngine(manager, CircuitBreakerFactory.defaultCircuitBreakerConfig(), HOT_KEY_CONFIG);

        key = RateLimitKey.builder().setUserId("test-user").build();
        manager.setConfig(key, new FixedWindowCounterConfig(CAPACITY, 60, TimeUnit.SECONDS));
    }

    @Test
    void disabledHotKeyConfigLeavesEngineBehaviourUnchanged() {
        RateLimitEngine plain = new RateLimitEngine(manager);
        RateLimitKey k = RateLimitKey.builder().setUserId("plain-user").build();
        manager.setConfig(k, new FixedWindowCounterConfig(5, 60, TimeUnit.SECONDS));

        int allowed = 0;
        for (int i = 0; i < 10; i++) {
            if (plain.process(k, 1).isAllowed()) allowed++;
        }
        assertEquals(5, allowed);
    }


    @Test
    void keyStartsCold() {
        assertNull(engine.getHotKeyEngine().getAllHotKeys()
                .stream().filter(k -> k.equals(key.toKey())).findFirst().orElse(null));
    }

    @Test
    void keyBecomesHotAfterThreshold() {
        for (int i = 0; i < HOT_KEY_CONFIG.getHotThreshold(); i++) {
            engine.process(key, 1);
        }
        assertEquals(KeyTemperature.HOT,
                engine.getHotKeyEngine().getTemperature(key.toKey()));
    }

    @Test
    void hotKeyIsEnforcedByLocalTier() {
        // Make key hot
        for (int i = 0; i < HOT_KEY_CONFIG.getHotThreshold(); i++) {
            engine.process(key, 1);
        }

        // Hot Key tier should reject once limit is exhausted.
        int further = 0;
        for (int i = 0; i < CAPACITY; i++) {
            if (engine.process(key, 1).isAllowed()) further++;
        }

        // The loop can admit at most CAPACITY-1
        assertTrue(further <= CAPACITY - 1,
                "Local tier loop must not exceed localLimit minus preheat slot");

        for (int i = 0; i < CAPACITY * 2; i++) {
            if (engine.process(key, 1).isAllowed()) further++;
        }

        assertTrue(further <= CAPACITY - 1,
                "Local tier loop must not exceed localLimit minus preheat slot");
    }

    @Test
    void hotKeyResultHasCorrectMetadata() {
        // The last iteration consumes Hot Key limit
        for (int i = 0; i < HOT_KEY_CONFIG.getHotThreshold(); i++) {
            engine.process(key, 1);
        }

        RateLimitResult result = engine.process(key, 1);
        assertEquals(CAPACITY - 2, result.getRemaining(), "remaining should equal localLimit minus costs consumed");
    }

    @Test
    void coldKeyStillEnforcedByAlgorithmNotLocalTier() {
        RateLimitKey coldKey = RateLimitKey.builder().setUserId("cold-user").build();
        manager.setConfig(coldKey, new FixedWindowCounterConfig(5, 60, TimeUnit.SECONDS));

        int allowed = 0;
        for (int i = 0; i < 10; i++) {
            if (engine.process(coldKey, 1).isAllowed()) allowed++;
        }

        // With capacity=5 and hotThreshold=3
        // First two 1-2 go via the normal path. Next 5 requests go via the hot path. The rest are denied.
        // Slightly over-allowed, trade-off b/w latency and accuracy
        assertEquals(7, allowed);
    }

    @Test
    void syncFreezesPreventsLocalOverAdmissionAfterGlobalLimitReached() throws Exception {
        // Use a very small capacity so the sync quickly detects exhaustion
        RateLimitKey smallKey = RateLimitKey.builder().setUserId("small-capacity").build();
        int smallCapacity = 10;
        manager.setConfig(smallKey, new FixedWindowCounterConfig(smallCapacity, 60, TimeUnit.SECONDS));

        // Make key hot
        for (int i = 0; i < HOT_KEY_CONFIG.getHotThreshold(); i++) {
            engine.process(smallKey, 1);
        }

        // Admit more requests via local tier. Exceeds smallCapacity locally since local hasn't synced yet)
        for (int i = 0; i < smallCapacity-2; i++) {
            engine.process(smallKey, 1);
        }

        // Sync all keys
        engine.getHotKeyEngine().syncAll();

        // After sync + freeze, all further requests must be denied
        int admittedAfterFreeze = 0;
        for (int i = 0; i < 20; i++) {
            if (engine.process(smallKey, 1).isAllowed()) admittedAfterFreeze++;
        }
        assertEquals(0, admittedAfterFreeze,
                "All requests after freeze must be denied");
    }

    @Test
    void hotPathConcurrentAdmissionsNeverExceedLocalLimit() throws Exception {
        // Make key hot
        for (int i = 0; i < HOT_KEY_CONFIG.getHotThreshold(); i++) {
            engine.process(key, 1);
        }

        int threads = 20;
        int requestsPerThread = 20; // 400 total, CAPACITY = 100
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicInteger admitted = new AtomicInteger();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                for (int r = 0; r < requestsPerThread; r++) {
                    if (engine.process(key, 1).isAllowed()) admitted.incrementAndGet();
                }
                return null;
            });
        }

        List<Future<Void>> futures = exec.invokeAll(tasks);
        for (Future<Void> f : futures) f.get();
        exec.shutdown();

        // The last preheat call already consumed 1 unit in the local tier.
        // The concurrent phase therefore admits at most CAPACITY-1 more.
        assertTrue(admitted.get() <= CAPACITY - 1,
                "Concurrent admitted " + admitted.get() + " exceeds localLimit-1 " + (CAPACITY - 1));
    }

    @Test
    void hotKeyDoesNotAffectOtherKeys() {
        RateLimitKey other = RateLimitKey.builder().setUserId("other-user").build();
        manager.setConfig(other, new FixedWindowCounterConfig(10, 60, TimeUnit.SECONDS));

        // Make key hot
        for (int i = 0; i < HOT_KEY_CONFIG.getHotThreshold(); i++) {
            engine.process(key, 1);
        }
        assertEquals(KeyTemperature.HOT, engine.getHotKeyEngine().getTemperature(key.toKey()));

        int allowed = 0;
        for (int i = 0; i < 15; i++) {
            if (engine.process(other, 1).isAllowed()) allowed++;
        }

        assertEquals(12, allowed, "Other key must be limited to its own localLimit");
    }

    @Test
    void hotKeyConfigValidatesWarmMustBeLessThanHot() {
        assertThrows(IllegalArgumentException.class, () ->
                new HotKeyConfig.Builder()
                        .warmThreshold(100)
                        .hotThreshold(50)
                        .build());
    }

    @Test
    void hotKeyConfigValidatesQuotaFractionRange() {
        assertThrows(IllegalArgumentException.class, () ->
                new HotKeyConfig.Builder()
                        .warmThreshold(10)
                        .hotThreshold(50)
                        .hotLocalQuotaFraction(0.0)
                        .build());
        assertThrows(IllegalArgumentException.class, () ->
                new HotKeyConfig.Builder()
                        .warmThreshold(10)
                        .hotThreshold(50)
                        .hotLocalQuotaFraction(1.1)
                        .build());
    }
}
