package org.nullpointer.ratelimiter.algorithms;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TokenBucketAlgorithmTest {

    @Test
    void allowsWithinCapacityAndThenDenies() {
        TokenBucketConfig config = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        RateLimitState state = config.initialRateLimitState(now.nanoTime());
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, now, 3);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, now, 3);

        assertTrue(r1.isAllowed());
        assertEquals(2, r1.getRemaining());
        assertFalse(r2.isAllowed());
        assertTrue(r2.getRetryAfterMillis() >= 0);
    }

    @Test
    void refillsOverTimeAllowsAgain() {
        TokenBucketConfig config = new TokenBucketConfig(1, 1, 1, TimeUnit.SECONDS);
        RequestTime t1 = new RequestTime(1000, 1000_000_000);
        RateLimitState state = config.initialRateLimitState(t1.nanoTime());

        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult first = algorithm.tryConsume(key, config, state, t1, 1);
        RateLimitResult second = algorithm.tryConsume(key, config, state, t1, 1);

        long waitMillis = Math.max(1L, second.getRetryAfterMillis() + 1);
        long nextMillis = 1000 + waitMillis;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000); // 1ms = 1_000_000ns

        RateLimitResult third = algorithm.tryConsume(key, config, state, t2, 1);

        assertTrue(first.isAllowed());
        assertFalse(second.isAllowed());
        assertTrue(third.isAllowed());
    }

    @Test
    void costGreaterThanCapacityDenies() {
        TokenBucketConfig config = new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        RateLimitState state = config.initialRateLimitState(now.nanoTime());
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult result = algorithm.tryConsume(key, config, state, now, 5);

        assertFalse(result.isAllowed());
        assertEquals(2, result.getRemaining());
    }

    @Test
    void multipleCost() {
        TokenBucketConfig config = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        RateLimitState state = config.initialRateLimitState(now.nanoTime());
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost").build();

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, now, 2);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, now, 3);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertFalse(r3.isAllowed());
    }

    @Test
    void differentKeysAreIndependent() {
        TokenBucketConfig config = new TokenBucketConfig(1, 1, 1, TimeUnit.SECONDS);
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey keyA = RateLimitKey.builder().setUserId("user-a").build();
        RateLimitKey keyB = RateLimitKey.builder().setUserId("user-b").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult r1 = algorithm.tryConsume(keyA, config, config.initialRateLimitState(now.nanoTime()), now, 1);
        RateLimitResult r2 = algorithm.tryConsume(keyB, config, config.initialRateLimitState(now.nanoTime()), now, 1);
        RateLimitResult r3 = algorithm.tryConsume(keyA, config, config.initialRateLimitState(now.nanoTime()), now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertTrue(r3.isAllowed());
    }

    @Test
    void testConcurrency() throws InterruptedException {
        int threadCount = 20;
        int requestsPerThread = 50;
        int capacity = 100; // Total allowed requests

        TokenBucketConfig config = new TokenBucketConfig(capacity, 1, 10, TimeUnit.SECONDS); // 100 tokens, refill 1/sec (slow refill for test)
        RequestTime now = new RequestTime(1000, 1000_000_000);
        RateLimitState state = config.initialRateLimitState(now.nanoTime());
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();

        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.atomic.AtomicInteger allowedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < requestsPerThread; j++) {
                    if (algorithm.tryConsume(key, config, state, now, 1).isAllowed()) {
                        allowedCount.incrementAndGet();
                    }
                }
            });
        }

        executor.shutdown();
        boolean finished = executor.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(finished);

        // We expect allowedCount to be exactly capacity because refill rate is negligible
        assertEquals(capacity, allowedCount.get());
    }

    @Test
    void invalidCost() {
        TokenBucketConfig config = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        RateLimitState state = config.initialRateLimitState(now.nanoTime());
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost-test").build();

        assertThrows(InvalidRateLimitCostException.class, () ->
                algorithm.tryConsume(key, config, state, now, 0)
        );
        assertThrows(InvalidRateLimitCostException.class, () ->
                algorithm.tryConsume(key, config, state, now, -1)
        );
    }

    @Test
    void canConsumeDoesNotMutateState() {
        TokenBucketConfig config = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RequestTime now = new RequestTime(1000, 1000_000_000);
        RateLimitState state = config.initialRateLimitState(now.nanoTime());
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-peek").build();

        RateLimitResult peek1 = algorithm.checkLimit(key, config, state, now, 3);
        assertTrue(peek1.isAllowed());
        assertEquals(2, peek1.getRemaining());

        // State unchanged — canConsume again should return the same result
        RateLimitResult peek2 = algorithm.checkLimit(key, config, state, now, 3);
        assertTrue(peek2.isAllowed());
        assertEquals(2, peek2.getRemaining());

        // Actually consume
        RateLimitResult consume = algorithm.tryConsume(key, config, state, now, 3);
        assertTrue(consume.isAllowed());

        // After real consumption, canConsume for 3 more should fail
        RateLimitResult peek3 = algorithm.checkLimit(key, config, state, now, 3);
        assertFalse(peek3.isAllowed());
    }
}
