package org.nullpointer.ratelimiter.storage;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.*;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class RedisStateRepositoryTest {

    private static RedisServer mockServer;
    private static JedisPool pool;

    private StateRepository repository;

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
        repository = new RedisStateRepository(pool, new JacksonSerializer());
    }

    @Test
    void storesAndRetrievesTokenBucketState() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();
        TokenBucketState original = new TokenBucketState(9.5, 1_000_000L);

        repository.setState(key, original);

        RateLimitState retrieved = repository.getState(key);
        assertNotNull(retrieved);
        assertInstanceOf(TokenBucketState.class, retrieved);
        TokenBucketState result = (TokenBucketState) retrieved;
        assertEquals(original.getAvailableTokens(), result.getAvailableTokens(), 0.0001);
        assertEquals(original.getLastRefillNanos(), result.getLastRefillNanos());
    }

    @Test
    void storesAndRetrievesFixedWindowCounterState() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user-2").build();
        FixedWindowCounterState original = new FixedWindowCounterState();
        original.addCostToWindow(3L, 101L);
        original.addCostToWindow(2L, 102L);

        repository.setState(key, original);

        RateLimitState retrieved = repository.getState(key);
        assertNotNull(retrieved);
        assertInstanceOf(FixedWindowCounterState.class, retrieved);
        FixedWindowCounterState result = (FixedWindowCounterState) retrieved;
        assertEquals(3L, result.getWindowCost(101L));
        assertEquals(2L, result.getWindowCost(102L));
    }

    @Test
    void storesAndRetrievesSlidingWindowState() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user-3").build();
        SlidingWindowState original = new SlidingWindowState();
        original.appendRequest(5L, 1_000_000L);
        original.appendRequest(3L, 2_000_000L);

        repository.setState(key, original);

        RateLimitState retrieved = repository.getState(key);
        assertNotNull(retrieved);
        assertInstanceOf(SlidingWindowState.class, retrieved);
        SlidingWindowState result = (SlidingWindowState) retrieved;
        // Large window: all requests still count
        assertEquals(8L, result.getCurrentWindowCost(Long.MAX_VALUE, 3_000_000L));
    }

    @Test
    void storesAndRetrievesSlidingWindowCounterState() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user-4").build();
        SlidingWindowCounterState original = new SlidingWindowCounterState(500_000L);
        original.addCostToWindow(10L, 1L);
        original.addCostToWindow(20L, 2L);

        repository.setState(key, original);

        RateLimitState retrieved = repository.getState(key);
        assertNotNull(retrieved);
        assertInstanceOf(SlidingWindowCounterState.class, retrieved);
        SlidingWindowCounterState result = (SlidingWindowCounterState) retrieved;
        assertEquals(500_000L, result.getOriginNanos());
        assertEquals(10L, result.getWindowCost(1L));
        assertEquals(20L, result.getWindowCost(2L));
    }

    @Test
    void hierarchicalStateIsStoredIndependentlyFromRegularState() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user-5").build();
        TokenBucketState regular = new TokenBucketState(10.0, 100L);
        TokenBucketState hierarchical = new TokenBucketState(50.0, 200L);

        repository.setState(key, regular);
        repository.setHierarchicalState(key, hierarchical);

        assertEquals(10.0, ((TokenBucketState) repository.getState(key)).getAvailableTokens(), 0.0001);
        assertEquals(50.0, ((TokenBucketState) repository.getHierarchicalState(key)).getAvailableTokens(), 0.0001);
    }

    @Test
    void returnsNullForMissingRegularStateKey() {
        RateLimitKey key = RateLimitKey.builder().setUserId("nonexistentRegular").build();
        assertNull(repository.getState(key));
    }

    @Test
    void returnsNullForMissingHierarchicalStateKey() {
        RateLimitKey key = RateLimitKey.builder().setUserId("nonexistentHierarchical").build();
        assertNull(repository.getHierarchicalState(key));
    }

    @Test
    void compositeKeyRoundTripsCorrectly() {
        RateLimitKey key = RateLimitKey.builder()
                .setUserId("alice")
                .setIpAddress("10.0.0.1")
                .setApi("/v1/search")
                .build();
        TokenBucketState state = new TokenBucketState(7.0, 999L);

        repository.setState(key, state);

        TokenBucketState result = (TokenBucketState) repository.getState(key);
        assertNotNull(result);
        assertEquals(7.0, result.getAvailableTokens(), 0.0001);
    }

    @Test
    void overwritingStateReturnsLatestValue() {
        RateLimitKey key = RateLimitKey.builder().setUserId("userOverwrite").build();

        repository.setState(key, new TokenBucketState(10.0, 100L));
        repository.setState(key, new TokenBucketState(5.0, 200L));

        assertEquals(5.0, ((TokenBucketState) repository.getState(key)).getAvailableTokens(), 0.0001);
    }
}
