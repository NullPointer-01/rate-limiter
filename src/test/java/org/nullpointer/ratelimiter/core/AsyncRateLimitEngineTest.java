package org.nullpointer.ratelimiter.core;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.*;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.*;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.AsyncRedisStateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AsyncRateLimitEngineTest {

    private static RedisServer mockServer;
    private static JedisPool pool;

    private AsyncRedisStateRepository stateRepo;
    private ConfigurationManager manager;
    private RateLimitEngine engine;

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
        stateRepo = new AsyncRedisStateRepository(60_000, pool, new JacksonSerializer());
        manager = new ConfigurationManager(new InMemoryConfigRepository(), stateRepo);
        engine = new RateLimitEngine(manager);
    }

    @Test
    void slidingWindowCounterEnforcesLimitLocallyBeforeFlush() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-local").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(5, 10, TimeUnit.SECONDS));

        for (int i = 0; i < 5; i++) assertTrue(engine.process(key, 1).isAllowed());
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void slidingWindowCounterAllowsExactCapacity() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-exact").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(10, 10, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 6).isAllowed());
        assertTrue(engine.process(key, 4).isAllowed());
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void slidingWindowCounterRemainingDecrementsCorrectly() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-remaining").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(10, 10, TimeUnit.SECONDS));

        RateLimitResult r1 = engine.process(key, 1);
        RateLimitResult r2 = engine.process(key, 3);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertEquals(6, r2.getRemaining());
    }

    @Test
    void slidingWindowCounterRetryAfterMillisIsPositiveWhenDenied() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-retry").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(3, 10, TimeUnit.SECONDS));

        engine.process(key, 3);
        RateLimitResult denied = engine.process(key, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0);
    }

    @Test
    void slidingWindowCounterAllowsAfterWindowAdvances() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-reset").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(3, 100, TimeUnit.MILLISECONDS));

        long baseMillis = 1_000L;
        RequestTime t1 = new RequestTime(baseMillis, baseMillis * 1_000_000L);

        engine.process(key, 3, t1);
        RateLimitResult denied = engine.process(key, 1, t1);
        assertFalse(denied.isAllowed());

        long nextMillis = baseMillis + denied.getRetryAfterMillis() + 1;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000L);

        assertTrue(engine.process(key, 1, t2).isAllowed());
    }

    @Test
    void slidingWindowCounterConcurrentRequestsNeverExceedCapacity() throws Exception {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-conc").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(100, 10, TimeUnit.SECONDS));

        assertEquals(100, runConcurrent(key, 20, 50));
    }

    @Test
    void tokenBucketEnforcesLimitLocallyBeforeFlush() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-local").build();
        manager.setConfig(key, new TokenBucketConfig(5, 1, 10, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 4).isAllowed());
        assertFalse(engine.process(key, 2).isAllowed());
    }

    @Test
    void tokenBucketAllowsExactCapacity() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-exact").build();
        manager.setConfig(key, new TokenBucketConfig(10, 0, 10, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 10).isAllowed());
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void tokenBucketRetryAfterMillisIsPositiveWhenDenied() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-retry").build();
        manager.setConfig(key, new TokenBucketConfig(2, 1, 60, TimeUnit.SECONDS));

        engine.process(key, 2);
        RateLimitResult denied = engine.process(key, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() > 0);
    }

    @Test
    void tokenBucketRefillsAfterWindowAdvances() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-refill").build();
        manager.setConfig(key, new TokenBucketConfig(5, 5, 100, TimeUnit.MILLISECONDS));

        long baseMillis = 1_000L;
        RequestTime t1 = new RequestTime(baseMillis, baseMillis * 1_000_000L);

        engine.process(key, 5, t1);
        RateLimitResult denied = engine.process(key, 1, t1);
        assertFalse(denied.isAllowed());

        long nextMillis = baseMillis + denied.getRetryAfterMillis() + 1;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000L);

        assertTrue(engine.process(key, 1, t2).isAllowed());
    }

    @Test
    void tokenBucketConcurrentRequestsNeverExceedCapacity() throws Exception {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-conc").build();
        manager.setConfig(key, new TokenBucketConfig(100, 1, 10, TimeUnit.SECONDS));

        assertEquals(100, runConcurrent(key, 20, 50));
    }

    @Test
    void fixedWindowCounterEnforcesLimitLocallyBeforeFlush() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-local").build();
        manager.setConfig(key, new FixedWindowCounterConfig(5, 10, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 5).isAllowed());
        assertFalse(engine.process(key, 1).isAllowed());
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
    void fixedWindowCounterResetsAfterWindowAdvances() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-reset").build();
        manager.setConfig(key, new FixedWindowCounterConfig(3, 100, TimeUnit.MILLISECONDS));

        long baseMillis = 1_000L;
        RequestTime t1 = new RequestTime(baseMillis, baseMillis * 1_000_000L);

        engine.process(key, 3, t1);
        RateLimitResult denied = engine.process(key, 1, t1);
        assertFalse(denied.isAllowed());

        long nextMillis = baseMillis + denied.getRetryAfterMillis() + 1;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000L);

        assertTrue(engine.process(key, 1, t2).isAllowed());
    }

    @Test
    void fixedWindowCounterConcurrentRequestsNeverExceedCapacity() throws Exception {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-conc").build();
        manager.setConfig(key, new FixedWindowCounterConfig(100, 10, TimeUnit.SECONDS));

        assertEquals(100, runConcurrent(key, 20, 50));
    }

    @Test
    void slidingWindowEnforcesLimitLocallyBeforeFlush() {
        RateLimitKey key = RateLimitKey.builder().setUserId("sw-local").build();
        manager.setConfig(key, new SlidingWindowConfig(5, 10, TimeUnit.SECONDS));

        assertTrue(engine.process(key, 5).isAllowed());
        assertFalse(engine.process(key, 1).isAllowed());
    }

    @Test
    void slidingWindowConcurrentRequestsNeverExceedCapacity() throws Exception {
        RateLimitKey key = RateLimitKey.builder().setUserId("sw-conc").build();
        manager.setConfig(key, new SlidingWindowConfig(100, 10, TimeUnit.SECONDS));

        assertEquals(100, runConcurrent(key, 20, 50));
    }

    @Test
    void slidingWindowCounterStateCorrectAfterFlushAndNodeRestart() {
        RateLimitKey key = RateLimitKey.builder().setUserId("swc-survive-flush").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(5, 10, TimeUnit.SECONDS));

        engine.process(key, 3);
        stateRepo.stop(); // flush to Redis

        AsyncRedisStateRepository repo2 = new AsyncRedisStateRepository(60_000, pool, new JacksonSerializer());
        ConfigurationManager manager2 = new ConfigurationManager(new InMemoryConfigRepository(), repo2);

        manager2.setConfig(key, new SlidingWindowCounterConfig(5, 10, TimeUnit.SECONDS));
        RateLimitEngine engine2 = new RateLimitEngine(manager2);

        assertTrue(engine2.process(key, 2).isAllowed());
        assertFalse(engine2.process(key, 1).isAllowed());
    }

    @Test
    void tokenBucketStateCorrectAfterFlushAndNodeRestart() {
        RateLimitKey key = RateLimitKey.builder().setUserId("tb-survive-flush").build();
        manager.setConfig(key, new TokenBucketConfig(10, 0, 10, TimeUnit.SECONDS));

        engine.process(key, 6);
        stateRepo.stop(); // flush to Redis

        AsyncRedisStateRepository repo2 = new AsyncRedisStateRepository(60_000, pool, new JacksonSerializer());
        ConfigurationManager manager2 = new ConfigurationManager(new InMemoryConfigRepository(), repo2);
        manager2.setConfig(key, new TokenBucketConfig(10, 0, 10, TimeUnit.SECONDS));
        RateLimitEngine engine2 = new RateLimitEngine(manager2);

        assertTrue(engine2.process(key, 4).isAllowed());
        assertFalse(engine2.process(key, 1).isAllowed());
    }

    @Test
    void fixedWindowCounterStateCorrectAfterFlushAndNodeRestart() {
        RateLimitKey key = RateLimitKey.builder().setUserId("fw-survive-flush").build();
        manager.setConfig(key, new FixedWindowCounterConfig(10, 10, TimeUnit.SECONDS));

        engine.process(key, 7);
        stateRepo.stop(); // flush to Redis

        AsyncRedisStateRepository repo2 = new AsyncRedisStateRepository(60_000, pool, new JacksonSerializer());
        ConfigurationManager manager2 = new ConfigurationManager(new InMemoryConfigRepository(), repo2);
        manager2.setConfig(key, new FixedWindowCounterConfig(10, 10, TimeUnit.SECONDS));
        RateLimitEngine engine2 = new RateLimitEngine(manager2);

        assertTrue(engine2.process(key, 3).isAllowed());
        assertFalse(engine2.process(key, 1).isAllowed());
    }

    @Test
    void newNodeSeesRedisStateOnLocalCacheMiss() {
        RateLimitKey key = RateLimitKey.builder().setUserId("node-restart").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(5, 10, TimeUnit.SECONDS));

        engine.process(key, 4);
        stateRepo.stop();

        AsyncRedisStateRepository repo2 = new AsyncRedisStateRepository(60_000, pool, new JacksonSerializer());
        ConfigurationManager manager2 = new ConfigurationManager(new InMemoryConfigRepository(), repo2);
        manager2.setConfig(key, new SlidingWindowCounterConfig(5, 10, TimeUnit.SECONDS));
        RateLimitEngine engine2 = new RateLimitEngine(manager2);

        assertTrue(engine2.process(key, 1).isAllowed());
        assertFalse(engine2.process(key, 1).isAllowed());
    }

    @Test
    void twoNodesEnforceLimitsLocally() {
        // Make capacity - Total capacity / Number of nodes
        RateLimitKey key = RateLimitKey.builder().setUserId("two-nodes-overage").build();
        SlidingWindowCounterConfig config = new SlidingWindowCounterConfig(5, 10, TimeUnit.SECONDS);

        AsyncRedisStateRepository repo2 = new AsyncRedisStateRepository(60_000, pool, new JacksonSerializer());
        ConfigurationManager manager2 = new ConfigurationManager(new InMemoryConfigRepository(), repo2);
        manager2.setConfig(key, config);
        RateLimitEngine engine2 = new RateLimitEngine(manager2);
        manager.setConfig(key, config);

        int allowedNode1 = 0;
        for (int i = 0; i < 5; i++) if (engine.process(key, 1).isAllowed()) allowedNode1++;

        int allowedNode2 = 0;
        for (int i = 0; i < 5; i++) if (engine2.process(key, 1).isAllowed()) allowedNode2++;

        assertEquals(5, allowedNode1);
        assertEquals(5, allowedNode2);
    }

    @Test
    void independentKeysDoNotInterfereWithEachOther() {
        RateLimitKey key1 = RateLimitKey.builder().setUserId("ind-a").build();
        RateLimitKey key2 = RateLimitKey.builder().setUserId("ind-b").build();
        manager.setConfig(key1, new SlidingWindowCounterConfig(3, 10, TimeUnit.SECONDS));
        manager.setConfig(key2, new SlidingWindowCounterConfig(3, 10, TimeUnit.SECONDS));

        engine.process(key1, 3); // exhaust key1

        // key2 should be unaffected
        assertTrue(engine.process(key2, 3).isAllowed());
        assertFalse(engine.process(key1, 1).isAllowed());
    }

    private int runConcurrent(RateLimitKey key, int threads, int requestsPerThread) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger allowed = new AtomicInteger();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                for (int j = 0; j < requestsPerThread; j++) {
                    if (engine.process(key, 1).isAllowed()) allowed.incrementAndGet();
                }
                return null;
            });
        }

        executor.invokeAll(tasks);
        executor.shutdown();
        return allowed.get();
    }
}
