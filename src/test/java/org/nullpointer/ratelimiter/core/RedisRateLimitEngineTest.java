package org.nullpointer.ratelimiter.core;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.config.RedisConfigRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RedisRateLimitEngineTest {

    private static RedisServer mockServer;
    private static JedisPool pool;

    private RateLimitEngine engine;
    private ConfigurationManager manager;

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
        );
        engine = new RateLimitEngine(manager);
    }

    @Test
    void tokenBucketAllowsRequestWithinLimit() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-allow").build();
        manager.setConfig(key, new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS));

        RateLimitResult result = engine.process(key, 1);

        assertTrue(result.isAllowed());
    }

    @Test
    void tokenBucketDeniesWhenExhausted() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-deny").build();
        manager.setConfig(key, new TokenBucketConfig(3, 1, 1, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 2).isAllowed());
        assertFalse(engine.process(key, 2).isAllowed());
    }

    @Test
    void tokenBucketRemainingDecrementsWithEachRequest() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-remaining").build();
        manager.setConfig(key, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));

        RateLimitResult r1 = engine.process(key, 1);
        RateLimitResult r2 = engine.process(key, 2);
        RateLimitResult r3 = engine.process(key, 3);

        assertTrue(r1.getRemaining() > r2.getRemaining());
        assertTrue(r2.getRemaining() > r3.getRemaining());
        assertEquals(4, r3.getRemaining());
    }

    @Test
    void tokenBucketRetryAfterMillisIsPositiveWhenDenied() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-retry").build();
        manager.setConfig(key, new TokenBucketConfig(2, 1, 60, TimeUnit.SECONDS));

        engine.process(key, 2); // exhaust
        RateLimitResult denied = engine.process(key, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0);
    }

    @Test
    void tokenBucketRefillsAfterWait() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-refill").build();
        // Refill 5 tokens every 100ms
        manager.setConfig(key, new TokenBucketConfig(5, 5, 100, TimeUnit.MILLISECONDS));

        long baseMillis = 1_000L;
        RequestTime t1 = new RequestTime(baseMillis, baseMillis * 1_000_000L);

        engine.process(key, 5, t1); // exhaust
        RateLimitResult denied = engine.process(key, 1, t1);
        assertFalse(denied.isAllowed()); // still empty

        // Advance time past the refill interval without sleeping
        long waitMillis = Math.max(1L, denied.getRetryAfterMillis() + 1);
        long nextMillis = baseMillis + waitMillis;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000L);

        assertTrue(engine.process(key, 1, t2).isAllowed());
    }

    @Test
    void fixedWindowCounterAllowsWithinLimit() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-allow").build();
        manager.setConfig(key, new FixedWindowCounterConfig(5, 1, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 3).isAllowed());
        assertTrue(engine.process(key, 2).isAllowed());
    }

    @Test
    void fixedWindowCounterDeniesWhenExhausted() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-deny").build();
        manager.setConfig(key, new FixedWindowCounterConfig(3, 1, TimeUnit.SECONDS));

        engine.process(key, 3);
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void fixedWindowCounterResetsAfterWindowExpires() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-reset").build();
        manager.setConfig(key, new FixedWindowCounterConfig(3, 100, TimeUnit.MILLISECONDS));

        long baseMillis = 1_000L;
        RequestTime t1 = new RequestTime(baseMillis, baseMillis * 1_000_000L);

        engine.process(key, 3, t1); // exhaust the window
        RateLimitResult denied = engine.process(key, 1, t1);
        assertFalse(denied.isAllowed());

        // Advance time past the window expiry without sleeping
        long waitMillis = Math.max(1L, denied.getRetryAfterMillis() + 1);
        long nextMillis = baseMillis + waitMillis;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000L);

        assertTrue(engine.process(key, 1, t2).isAllowed());
    }

    @Test
    void fixedWindowCounterRetryAfterMillisIsPositiveWhenDenied() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-retry").build();
        manager.setConfig(key, new FixedWindowCounterConfig(2, 10, TimeUnit.SECONDS));

        engine.process(key, 2);
        RateLimitResult denied = engine.process(key, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0);
    }

    @Test
    void slidingWindowAllowsWithinLimit() {
        RateLimitKey key = RateLimitKey.builder().setUserId("sw-allow").build();
        manager.setConfig(key, new SlidingWindowConfig(10, 1, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 5).isAllowed());
        assertTrue(engine.process(key, 5).isAllowed());
    }

    @Test
    void slidingWindowDeniesWhenExhausted() {
        RateLimitKey key = RateLimitKey.builder().setUserId("sw-deny").build();
        manager.setConfig(key, new SlidingWindowConfig(5, 1, TimeUnit.SECONDS));

        engine.process(key, 5);
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void slidingWindowRetryAfterMillisIsPositiveWhenDenied() {
        RateLimitKey key = RateLimitKey.builder().setUserId("sw-retry").build();
        manager.setConfig(key, new SlidingWindowConfig(3, 10, TimeUnit.SECONDS));

        engine.process(key, 3);
        RateLimitResult denied = engine.process(key, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0);
    }

    @Test
    void slidingWindowCounterAllowsWithinLimit() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-allow").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(10, 1, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 5).isAllowed());
        assertTrue(engine.process(key, 5).isAllowed());
    }

    @Test
    void slidingWindowCounterDeniesWhenExhausted() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-deny").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(5, 1, TimeUnit.SECONDS));

        engine.process(key, 5);
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void independentKeysHaveIndependentQuotas() {
        RateLimitKey key1 = RateLimitKey.builder().setUserId("isolated-user-1").build();
        RateLimitKey key2 = RateLimitKey.builder().setUserId("isolated-user-2").build();
        TokenBucketConfig config = new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS);
        manager.setConfig(key1, config);
        manager.setConfig(key2, config);

        engine.process(key1, 2); // exhaust key1

        // key2 is unaffected
        assertTrue(engine.process(key2, 1).isAllowed());
    }

    @Test
    void differentAlgorithmConfigsForDifferentKeys() {
        RateLimitKey key1 = RateLimitKey.builder().setUserId("algo-tb").build();
        RateLimitKey key2 = RateLimitKey.builder().setUserId("algo-fw").build();
        manager.setConfig(key1, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));
        manager.setConfig(key2, new FixedWindowCounterConfig(10, 1, TimeUnit.SECONDS));

        assertTrue(engine.process(key1, 5).isAllowed());
        assertTrue(engine.process(key2, 5).isAllowed());
    }

    @Test
    void costGreaterThanOneConsumesCorrectly() {
        RateLimitKey key = RateLimitKey.builder().setUserId("cost-key").build();
        manager.setConfig(key, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));

        RateLimitResult r1 = engine.process(key, 6);
        RateLimitResult r2 = engine.process(key, 5); // only 4 remaining

        assertTrue(r1.isAllowed());
        assertFalse(r2.isAllowed());
        assertEquals(4, r2.getRemaining());
    }
}
