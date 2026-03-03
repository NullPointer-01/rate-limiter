package org.nullpointer.ratelimiter.algorithms;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitCostException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.state.FixedWindowCounterState;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class FixedWindowCounterAlgorithmTest {

    @Test
    void enforcesFixedWindowCapacity() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(3, 1, TimeUnit.SECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();
        RequestTime now = new RequestTime(1000L, 1000000000L);

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, now);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, now);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, now);
        RateLimitResult r4 = algorithm.tryConsume(key, config, state, now);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertTrue(r3.isAllowed());
        assertFalse(r4.isAllowed());
        assertEquals(0, r3.getRemaining());
        assertTrue(r4.getRetryAfterMillis() >= 0);
    }

    @Test
    void newWindowAllowsAfterWait() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(1, 5, TimeUnit.MILLISECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        long startMillis = 10000;
        RequestTime t1 = new RequestTime(startMillis, startMillis * 1_000_000);

        RateLimitResult first = algorithm.tryConsume(key, config, state, t1, 1);
        RateLimitResult second = algorithm.tryConsume(key, config, state, t1, 1);

        assertTrue(first.isAllowed());
        assertFalse(second.isAllowed());

        long waitMillis = Math.max(1L, second.getRetryAfterMillis() + 1);
        long nextMillis = startMillis + waitMillis;
        RequestTime t2 = new RequestTime(nextMillis, nextMillis * 1_000_000);

        RateLimitResult third = algorithm.tryConsume(key, config, state, t2, 1);

        assertTrue(third.isAllowed());
    }

    @Test
    void remainingIsNeverNegativeOnDeny() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(2, 1, TimeUnit.SECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        algorithm.tryConsume(key, config, state, now, 2);
        RateLimitResult denied = algorithm.tryConsume(key, config, state, now, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRemaining() >= 0);
    }

    @Test
    void multipleCost() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(5, 1, TimeUnit.SECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, now, 2);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, now, 3);
        RateLimitResult r3 = algorithm.tryConsume(key, config, state, now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertFalse(r3.isAllowed());
    }

    @Test
    void differentKeysAreIndependent() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(1, 1, TimeUnit.SECONDS);
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        RateLimitResult r1 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-a").build(), config, new FixedWindowCounterState(), now, 1);
        RateLimitResult r2 = algorithm.tryConsume(RateLimitKey.builder().setUserId("user-b").build(), config, new FixedWindowCounterState(), now, 1);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
    }

    @Test
    void testConcurrency() throws InterruptedException {
        int threadCount = 20;
        int requestsPerThread = 50;
        int capacity = 1000; // Total allowed

        FixedWindowCounterConfig config = new FixedWindowCounterConfig(capacity, 10, TimeUnit.SECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

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

        assertEquals(1000, allowedCount.get());
    }

    @Test
    void invalidCost() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(10, 1, TimeUnit.SECONDS);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-cost-test").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

        assertThrows(InvalidRateLimitCostException.class, () ->
            algorithm.tryConsume(key, config, state, now, 0)
        );
        assertThrows(InvalidRateLimitCostException.class, () ->
            algorithm.tryConsume(key, config, state, now, -1)
        );
    }

    @Test
    void canConsumeDoesNotMutateState() {
        FixedWindowCounterConfig config = new FixedWindowCounterConfig(5, 1, TimeUnit.MINUTES);
        FixedWindowCounterState state = new FixedWindowCounterState();
        FixedWindowCounterAlgorithm algorithm = new FixedWindowCounterAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-peek").build();
        RequestTime now = new RequestTime(1000, 1000_000_000);

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
