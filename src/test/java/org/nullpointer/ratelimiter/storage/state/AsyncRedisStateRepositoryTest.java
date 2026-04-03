package org.nullpointer.ratelimiter.storage.state;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.*;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.*;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class AsyncRedisStateRepositoryTest {

    private static RedisServer mockServer;
    private static JedisPool pool;

    private AsyncRedisStateRepository repository;
    private JacksonSerializer serializer;

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
        serializer = new JacksonSerializer();
        // Long interval to ensure scheduler never syncs to redis automatically during tests
        repository = new AsyncRedisStateRepository(60_000, pool, serializer);
    }

    private String rawGet(String key) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.get(key);
        }
    }

    @Test
    void setStateWritesToLocalCacheOnlyBeforeFlush() {
        RateLimitKey key = RateLimitKey.builder().setUserId("local-only").build();
        repository.setState(key, new SlidingWindowCounterState(1_000L));

        assertNull(rawGet("rl:state:" + key.toKey()));
        assertNotNull(repository.getState(key));
    }

    @Test
    void getStateFallsBackToRedisOnLocalCacheMiss() {
        RateLimitKey key = RateLimitKey.builder().setUserId("redis-fallback").build();
        TokenBucketState state = new TokenBucketState(7.0, 500_000L);

        try (Jedis jedis = pool.getResource()) {
            jedis.set("rl:state:" + key.toKey(), serializer.serialize(state));
        }

        // Fresh repo — empty local cache
        AsyncRedisStateRepository freshRepo = new AsyncRedisStateRepository(60_000, pool, serializer);
        RateLimitState result = freshRepo.getState(key);

        assertNotNull(result);
        assertInstanceOf(TokenBucketState.class, result);
        assertEquals(7.0, ((TokenBucketState) result).getAvailableTokens(), 0.0001);
    }

    @Test
    void getHierarchicalStateFallsBackToRedisOnLocalCacheMiss() {
        RateLimitKey key = RateLimitKey.builder().setUserId("hstate-fallback").build();
        SlidingWindowCounterState state = new SlidingWindowCounterState(9_000L);
        state.addCostToWindow(7L, 3L);

        try (Jedis jedis = pool.getResource()) {
            jedis.set("rl:hstate:" + key.toKey(), serializer.serialize(state));
        }

        AsyncRedisStateRepository freshRepo = new AsyncRedisStateRepository(60_000, pool, serializer);
        RateLimitState result = freshRepo.getHierarchicalState(key);

        assertNotNull(result);
        assertInstanceOf(SlidingWindowCounterState.class, result);
        assertEquals(7L, ((SlidingWindowCounterState) result).getWindowCost(3L));
    }

    @Test
    void getStateReturnsNullWhenAbsentInBothLocalCacheAndRedis() {
        RateLimitKey key = RateLimitKey.builder().setUserId("absent-key").build();
        assertNull(repository.getState(key));
    }

    @Test
    void getHierarchicalStateReturnsNullWhenAbsentInBothLocalCacheAndRedis() {
        RateLimitKey key = RateLimitKey.builder().setUserId("absent-hkey").build();
        assertNull(repository.getHierarchicalState(key));
    }


    @Test
    void syncToRedisFlushesAllDirtyKeysAcrossAllStateTypes() {
        RateLimitKey keyA = RateLimitKey.builder().setUserId("flush-tb").build();
        RateLimitKey keyB = RateLimitKey.builder().setUserId("flush-swc").build();
        RateLimitKey keyC = RateLimitKey.builder().setUserId("flush-fw").build();

        repository.setState(keyA, new TokenBucketState(10.0, 100L));
        repository.setState(keyB, new SlidingWindowCounterState(200L));
        repository.setState(keyC, new FixedWindowCounterState());

        repository.syncToRedis();

        assertNotNull(rawGet("rl:state:" + keyA.toKey()));
        assertNotNull(rawGet("rl:state:" + keyB.toKey()));
        assertNotNull(rawGet("rl:state:" + keyC.toKey()));
    }

    @Test
    void syncToRedisOnlyFlushesNewDirtyKeysSincePreviousFlush() {
        RateLimitKey keyA = RateLimitKey.builder().setUserId("dirty-a").build();
        RateLimitKey keyB = RateLimitKey.builder().setUserId("dirty-b").build();

        repository.setState(keyA, new TokenBucketState(5.0, 100L));
        repository.syncToRedis(); // first flush — keyA written

        // Overwrite keyA in Redis with new value; if keyA is re-flushed, the new value disappears
        try (Jedis jedis = pool.getResource()) {
            jedis.set("rl:state:" + keyA.toKey(), "new value");
        }

        repository.setState(keyB, new TokenBucketState(9.0, 200L));
        repository.syncToRedis(); // second flush — only keyB should flush

        assertEquals("new value", rawGet("rl:state:" + keyA.toKey())); // KeyA Not re-flushed
        assertNotNull(rawGet("rl:state:" + keyB.toKey())); // KeyB flushed
    }

    @Test
    void syncToRedisUsesLatestValueWhenStateUpdatedMultipleTimes() {
        RateLimitKey key = RateLimitKey.builder().setUserId("latest-val").build();

        repository.setState(key, new TokenBucketState(1.0, 100L));
        repository.setState(key, new TokenBucketState(5.0, 200L));
        repository.setState(key, new TokenBucketState(9.0, 300L));
        repository.syncToRedis();

        AsyncRedisStateRepository freshRepo = new AsyncRedisStateRepository(60_000, pool, serializer);
        RateLimitState result = freshRepo.getState(key);

        assertInstanceOf(TokenBucketState.class, result);
        assertEquals(9.0, ((TokenBucketState) result).getAvailableTokens(), 0.0001);
    }

    @Test
    void syncToRedisIsNoOpWhenNoDirtyKeys() {
        assertDoesNotThrow(() -> repository.syncToRedis());
    }

    @Test
    void syncToRedisRequeuesKeysOnRedisFailureAndDoesNotThrow() throws IOException {
        RedisServer failServer = RedisServer.newRedisServer().start();
        JedisPool failPool = new JedisPool("localhost", failServer.getBindPort());
        AsyncRedisStateRepository failRepo = new AsyncRedisStateRepository(60_000, failPool, serializer);

        RateLimitKey key = RateLimitKey.builder().setUserId("requeue").build();
        failRepo.setState(key, new TokenBucketState(3.0, 100L));

        failPool.close();
        failServer.stop();

        assertDoesNotThrow(failRepo::syncToRedis); // Must not throw; key is silently re-queued
    }

    @Test
    void stopFlushesPendingDirtyKeysBeforeShutdown() {
        RateLimitKey key = RateLimitKey.builder().setUserId("stop-flush").build();
        repository.setState(key, new SlidingWindowCounterState(1_000L));

        repository.stop();

        assertNotNull(rawGet("rl:state:" + key.toKey()));
    }

    @Test
    void setHierarchicalStateUsesDistinctRedisPrefixFromRegularState() {
        RateLimitKey key = RateLimitKey.builder().setUserId("prefix-check").build();

        repository.setState(key, new TokenBucketState(10.0, 100L));
        repository.setHierarchicalState(key, new TokenBucketState(50.0, 200L));
        repository.syncToRedis();

        String regularRaw = rawGet("rl:state:" + key.toKey());
        String hierarchicalRaw = rawGet("rl:hstate:" + key.toKey());

        assertNotNull(regularRaw);
        assertNotNull(hierarchicalRaw);
        assertNotEquals(regularRaw, hierarchicalRaw);
    }

    @Test
    void hierarchicalStateAndRegularStateAreIndependentForSameKey() {
        RateLimitKey key = RateLimitKey.builder().setUserId("isolation").build();
        TokenBucketState regular = new TokenBucketState(10.0, 100L);
        TokenBucketState hierarchical = new TokenBucketState(50.0, 200L);

        repository.setState(key, regular);
        repository.setHierarchicalState(key, hierarchical);

        assertEquals(10.0, ((TokenBucketState) repository.getState(key)).getAvailableTokens(), 0.0001);
        assertEquals(50.0, ((TokenBucketState) repository.getHierarchicalState(key)).getAvailableTokens(), 0.0001);
    }

    @Test
    void syncToRedisFlushesHierarchicalStateKeys() {
        RateLimitKey key = RateLimitKey.builder().setUserId("hstate-flush").build();
        repository.setHierarchicalState(key, new SlidingWindowCounterState(500L));

        repository.syncToRedis();

        assertNotNull(rawGet("rl:hstate:" + key.toKey()));
        assertNull(rawGet("rl:state:" + key.toKey())); // regular prefix untouched
    }

    @Test
    void concurrentSetsAllEventuallyFlushedToRedis() throws InterruptedException {
        int keyCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<RateLimitKey> keys = new ArrayList<>();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < keyCount; i++) {
            RateLimitKey key = RateLimitKey.builder().setUserId("conc-" + i).build();
            keys.add(key);
            tasks.add(() -> {
                repository.setState(key, new TokenBucketState(5.0, System.nanoTime()));
                return null;
            });
        }

        executor.invokeAll(tasks);
        executor.shutdown();
        repository.syncToRedis();

        for (RateLimitKey key : keys) {
            assertNotNull(rawGet("rl:state:" + key.toKey()), "Missing: " + key.toKey());
        }
    }

    @Test
    void concurrentFlushAndSetDoNotLoseDirtyKeys() throws InterruptedException {
        // Writer thread continuously marks keys dirty while flush fires concurrently
        int keyCount = 50;
        List<RateLimitKey> keys = new ArrayList<>();
        for (int i = 0; i < keyCount; i++) {
            keys.add(RateLimitKey.builder().setUserId("race-" + i).build());
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Writer: marks all keys dirty
        executor.submit(() -> {
            for (RateLimitKey key : keys) {
                repository.setState(key, new SlidingWindowCounterState(System.nanoTime()));
            }
        });

        executor.submit(() -> repository.syncToRedis());

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // Final flush to ensure any keys that landed in new dirty set are also written
        repository.syncToRedis();

        for (RateLimitKey key : keys) {
            assertNotNull(rawGet("rl:state:" + key.toKey()), "Missing after concurrent flush: " + key.toKey());
        }
    }
}
