package org.nullpointer.ratelimiter.storage.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryAtomicStateRepositoryTest {

    private InMemoryAtomicStateRepository repo;

    @BeforeEach
    void setUp() {
        repo = new InMemoryAtomicStateRepository();
    }

    private static RateLimitKey key(String id) {
        return RateLimitKey.fromKey(id);
    }

    private static RequestTime at(long millis) {
        return new RequestTime(millis, millis * 1_000_000L);
    }

    private int countAllowed(int threads, Callable<RateLimitResult> task) throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> task.call().isAllowed());
        }
        List<Future<Boolean>> futures = exec.invokeAll(tasks);
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);
        int allowed = 0;
        for (Future<Boolean> f : futures) {
            if (f.get()) allowed++;
        }
        return allowed;
    }

    @Test
    void tokenBucketAllowsWithinCapacityThenDenies() {
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("user1");
        RequestTime t = at(1000);

        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 3).isAllowed());
        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 2).isAllowed());
        assertFalse(repo.atomicConsumeAndUpdate(k, cfg, t, 1).isAllowed());
    }

    @Test
    void tokenBucketRemainingDecrementsWithEachConsume() {
        TokenBucketConfig cfg = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("user1");
        RequestTime t = at(1000);

        RateLimitResult r1 = repo.atomicConsumeAndUpdate(k, cfg, t, 1);
        RateLimitResult r2 = repo.atomicConsumeAndUpdate(k, cfg, t, 1);
        RateLimitResult r3 = repo.atomicConsumeAndUpdate(k, cfg, t, 3);

        assertTrue(r1.getRemaining() > r2.getRemaining());
        assertTrue(r2.getRemaining() > r3.getRemaining());
    }

    @Test
    void tokenBucketRefillAllowsAfterDelay() {
        TokenBucketConfig cfg = new TokenBucketConfig(1, 1, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("user-refill");

        RequestTime t1 = at(1000);
        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t1, 1).isAllowed());
        assertFalse(repo.atomicConsumeAndUpdate(k, cfg, t1, 1).isAllowed());

        // Advance > 1000 ms so the token refills
        RequestTime t2 = at(2001);
        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t2, 1).isAllowed());
    }

    @Test
    void fixedWindowAllowsWithinCapacityThenDenies() {
        FixedWindowCounterConfig cfg = new FixedWindowCounterConfig(3, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("user1");
        RequestTime t = at(1000);

        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 2).isAllowed());
        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 1).isAllowed());
        assertFalse(repo.atomicConsumeAndUpdate(k, cfg, t, 1).isAllowed());
    }

    @Test
    void fixedWindowResetsAfterWindowExpiry() {
        FixedWindowCounterConfig cfg = new FixedWindowCounterConfig(3, 100, TimeUnit.MILLISECONDS);
        RateLimitKey k = key("user-reset");

        RequestTime t1 = at(1000);
        repo.atomicConsumeAndUpdate(k, cfg, t1, 3); // exhaust
        assertFalse(repo.atomicConsumeAndUpdate(k, cfg, t1, 1).isAllowed());

        RequestTime t2 = at(1101); // new window
        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t2, 1).isAllowed());
    }

    @Test
    void slidingWindowCounterAllowsWithinCapacityThenDenies() {
        SlidingWindowCounterConfig cfg = new SlidingWindowCounterConfig(5, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("swcBasic");
        RequestTime t = at(1000);

        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 3).isAllowed());
        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 2).isAllowed());
        assertFalse(repo.atomicConsumeAndUpdate(k, cfg, t, 1).isAllowed());
    }

    @Test
    void slidingWindowAllowsWithinCapacityThenDenies() {
        SlidingWindowConfig cfg = new SlidingWindowConfig(4, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("swBasic");
        RequestTime t = at(1000);

        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 2).isAllowed());
        assertTrue(repo.atomicConsumeAndUpdate(k, cfg, t, 2).isAllowed());
        assertFalse(repo.atomicConsumeAndUpdate(k, cfg, t, 1).isAllowed());
    }

    @Test
    void rejectedRequestDoesNotConsumeQuota() {
        TokenBucketConfig cfg = new TokenBucketConfig(3, 0, 1, TimeUnit.HOURS);
        RateLimitKey k = key("tbNoMutationOnDeny");
        RequestTime t = at(1000);

        repo.atomicConsumeAndUpdate(k, cfg, t, 3); // exhaust

        RateLimitResult d1 = repo.atomicConsumeAndUpdate(k, cfg, t, 1);
        RateLimitResult d2 = repo.atomicConsumeAndUpdate(k, cfg, t, 1);

        assertFalse(d1.isAllowed());
        assertFalse(d2.isAllowed());
        // remaining must stay 0, not go negative
        assertEquals(0, d1.getRemaining());
        assertEquals(0, d2.getRemaining());
    }

    @Test
    void differentKeysDoNotShareState() {
        TokenBucketConfig cfg = new TokenBucketConfig(3, 0, 1, TimeUnit.HOURS);
        RequestTime t = at(1000);

        repo.atomicConsumeAndUpdate(key("user1"), cfg, t, 3); // exhaust key-A
        assertFalse(repo.atomicConsumeAndUpdate(key("user1"), cfg, t, 1).isAllowed());

        // user2 still has its full quota
        assertTrue(repo.atomicConsumeAndUpdate(key("user2"), cfg, t, 3).isAllowed());
    }

    @Test
    void tokenBucketExactCapacityAllowedConcurrent() throws Exception {
        int capacity = 20;
        TokenBucketConfig cfg = new TokenBucketConfig(capacity, 60, 1, TimeUnit.MINUTES);
        RateLimitKey k = key("tbConcurrent");
        RequestTime t = at(1000);

        int allowed = countAllowed(capacity * 2, () -> repo.atomicConsumeAndUpdate(k, cfg, t, 1));

        assertEquals(capacity, allowed,
                "Token bucket: exactly " + capacity + " should be allowed, got " + allowed);
    }

    @Test
    void tokenBucketNeverExceedsCapacityUnderHighContention() throws Exception {
        int capacity = 10;
        TokenBucketConfig cfg = new TokenBucketConfig(capacity, 60, 1, TimeUnit.MINUTES);
        RateLimitKey k = key("tbHighContention");
        RequestTime t = at(1000);

        int allowed = countAllowed(200, () -> repo.atomicConsumeAndUpdate(k, cfg, t, 1));

        assertEquals(capacity, allowed,
                "Should never exceed capacity=" + capacity + ", allowed=" + allowed);
    }

    @Test
    void fixedWindowExactCapacityAllowedConcurrent() throws Exception {
        int capacity = 20;
        FixedWindowCounterConfig cfg = new FixedWindowCounterConfig(capacity, 60, TimeUnit.SECONDS);
        RateLimitKey k = key("fwConcurrent");
        RequestTime t = at(1000);

        int allowed = countAllowed(capacity * 2, () -> repo.atomicConsumeAndUpdate(k, cfg, t, 1));

        assertEquals(capacity, allowed,
                "Fixed window: exactly " + capacity + " should be allowed, got " + allowed);
    }

    @Test
    void slidingWindowCounterExactCapacityAllowedConcurrent() throws Exception {
        int capacity = 20;
        SlidingWindowCounterConfig cfg = new SlidingWindowCounterConfig(capacity, 60, TimeUnit.SECONDS);
        RateLimitKey k = key("swcConcurrent");
        RequestTime t = at(1000);

        int allowed = countAllowed(capacity * 2, () -> repo.atomicConsumeAndUpdate(k, cfg, t, 1));

        assertEquals(capacity, allowed,
                "Sliding window counter: exactly " + capacity + " should be allowed, got " + allowed);
    }

    @Test
    void slidingWindowExactCapacityAllowedConcurrent() throws Exception {
        int capacity = 20;
        SlidingWindowConfig cfg = new SlidingWindowConfig(capacity, 60, TimeUnit.SECONDS);
        RateLimitKey k = key("swConcurrent");
        RequestTime t = at(1000);

        int allowed = countAllowed(capacity * 2, () -> repo.atomicConsumeAndUpdate(k, cfg, t, 1));

        assertEquals(capacity, allowed,
                "Sliding window: exactly " + capacity + " should be allowed, got " + allowed);
    }

    @Test
    void multipleKeysHaveIndependentQuotasConcurrent() throws Exception {
        int capacityPerKey = 5;
        int keyCount = 8;
        TokenBucketConfig cfg = new TokenBucketConfig(capacityPerKey, 0, 1, TimeUnit.HOURS);
        RequestTime t = at(1000);

        AtomicInteger totalAllowed = new AtomicInteger();
        ExecutorService exec = Executors.newFixedThreadPool(keyCount * capacityPerKey);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int kIdx = 0; kIdx < keyCount; kIdx++) {
            RateLimitKey rk = key("multiKey" + kIdx);
            for (int r = 0; r < capacityPerKey; r++) {
                tasks.add(() -> {
                    if (repo.atomicConsumeAndUpdate(rk, cfg, t, 1).isAllowed()) {
                        totalAllowed.incrementAndGet();
                    }
                    return null;
                });
            }
        }

        List<Future<Void>> futures = exec.invokeAll(tasks);
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);
        for (Future<Void> f : futures) f.get();

        assertEquals(keyCount * capacityPerKey, totalAllowed.get(),
                "All " + (keyCount * capacityPerKey) + " requests across independent keys should be allowed");
    }

    @Test
    void deniedResultAlwaysHasNonNegativeRetryAfterMillisConcurrent() throws Exception {
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("tbRetryAfter");
        RequestTime t = at(1000);

        ExecutorService exec = Executors.newFixedThreadPool(50);
        List<Future<RateLimitResult>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(exec.submit(() -> repo.atomicConsumeAndUpdate(k, cfg, t, 1)));
        }
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);

        for (Future<RateLimitResult> f : futures) {
            RateLimitResult r = f.get();
            if (!r.isAllowed()) {
                assertTrue(r.getRetryAfterMillis() >= 0,
                        "retryAfterMillis must be >= 0 on denial, got " + r.getRetryAfterMillis());
            }
        }
    }

    @Test
    void allowedResultHasPositiveResetAtMillisTokenBucket() {
        TokenBucketConfig cfg = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RequestTime t = at(1000);
        RateLimitResult r = repo.atomicConsumeAndUpdate(key("tbResetAt"), cfg, t, 1);
        assertTrue(r.isAllowed());
        assertTrue(r.getResetAtMillis() > 0, "resetAtMillis should be > 0 on allowed, got " + r.getResetAtMillis());
        assertEquals(0, r.getRetryAfterMillis(), "retryAfterMillis must be 0 on allowed result");
    }

    @Test
    void allowedResultHasPositiveResetAtMillisFixedWindow() {
        FixedWindowCounterConfig cfg = new FixedWindowCounterConfig(5, 1, TimeUnit.SECONDS);
        RequestTime t = at(1000);
        RateLimitResult r = repo.atomicConsumeAndUpdate(key("fwResetAt"), cfg, t, 1);
        assertTrue(r.isAllowed());
        assertTrue(r.getResetAtMillis() > 0, "resetAtMillis should be > 0 on allowed, got " + r.getResetAtMillis());
        assertEquals(0, r.getRetryAfterMillis(), "retryAfterMillis must be 0 on allowed result");
    }

    @Test
    void allowedResultHasPositiveResetAtMillisSlidingWindowCounter() {
        SlidingWindowCounterConfig cfg = new SlidingWindowCounterConfig(5, 1, TimeUnit.SECONDS);
        RequestTime t = at(1000);
        RateLimitResult r = repo.atomicConsumeAndUpdate(key("swcResetAt"), cfg, t, 1);
        assertTrue(r.isAllowed());
        assertTrue(r.getResetAtMillis() > 0, "resetAtMillis should be > 0 on allowed, got " + r.getResetAtMillis());
        assertEquals(0, r.getRetryAfterMillis(), "retryAfterMillis must be 0 on allowed result");
    }

    @Test
    void allowedResultHasPositiveResetAtMillisSlidingWindow() {
        SlidingWindowConfig cfg = new SlidingWindowConfig(5, 1, TimeUnit.SECONDS);
        RequestTime t = at(1000);
        RateLimitResult r = repo.atomicConsumeAndUpdate(key("swResetAt"), cfg, t, 1);
        assertTrue(r.isAllowed());
        assertTrue(r.getResetAtMillis() > 0, "resetAtMillis should be > 0 on allowed, got " + r.getResetAtMillis());
        assertEquals(0, r.getRetryAfterMillis(), "retryAfterMillis must be 0 on allowed result");
    }

    @Test
    void deniedResultHasNonNegativeRetryAfterMillisFixedWindow() {
        FixedWindowCounterConfig cfg = new FixedWindowCounterConfig(2, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("fwRetryAfter");
        RequestTime t = at(1000);

        repo.atomicConsumeAndUpdate(k, cfg, t, 2); // exhaust
        RateLimitResult denied = repo.atomicConsumeAndUpdate(k, cfg, t, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() >= 0,
                "retryAfterMillis must be >= 0 on denial, got " + denied.getRetryAfterMillis());
    }

    @Test
    void deniedResultHasNonNegativeRetryAfterMillisSlidingWindowCounter() {
        SlidingWindowCounterConfig cfg = new SlidingWindowCounterConfig(2, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("swcRetryAfter");
        RequestTime t = at(1000);

        repo.atomicConsumeAndUpdate(k, cfg, t, 2); // exhaust
        RateLimitResult denied = repo.atomicConsumeAndUpdate(k, cfg, t, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() >= 0,
                "retryAfterMillis must be >= 0 on denial, got " + denied.getRetryAfterMillis());
    }

    @Test
    void deniedResultHasNonNegativeRetryAfterMillisSlidingWindow() {
        SlidingWindowConfig cfg = new SlidingWindowConfig(2, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("swRetryAfter");
        RequestTime t = at(1000);

        repo.atomicConsumeAndUpdate(k, cfg, t, 2); // exhaust
        RateLimitResult denied = repo.atomicConsumeAndUpdate(k, cfg, t, 1);

        assertFalse(denied.isAllowed());
        assertTrue(denied.getRetryAfterMillis() >= 0,
                "retryAfterMillis must be >= 0 on denial, got " + denied.getRetryAfterMillis());
    }

    @Test
    void tokenBucketBulkCostNeverExceedsCapacityConcurrent() throws Exception {
        int capacity = 30;
        int cost = 3; // each request consumes 3 tokens → at most 10 allowed
        TokenBucketConfig cfg = new TokenBucketConfig(capacity, 0, 1, TimeUnit.HOURS);
        RateLimitKey k = key("tbBulkCost");
        RequestTime t = at(1000);

        int allowed = countAllowed(50, () -> repo.atomicConsumeAndUpdate(k, cfg, t, cost));

        assertTrue(allowed <= capacity / cost,
                "Bulk-cost: at most " + (capacity / cost) + " should be allowed, got " + allowed);
    }

    @Test
    void fixedWindowBulkCostNeverExceedsCapacityConcurrent() throws Exception {
        int capacity = 30;
        int cost = 3;
        FixedWindowCounterConfig cfg = new FixedWindowCounterConfig(capacity, 60, TimeUnit.SECONDS);
        RateLimitKey k = key("fwBulkCost");
        RequestTime t = at(1000);

        int allowed = countAllowed(50, () -> repo.atomicConsumeAndUpdate(k, cfg, t, cost));

        assertTrue(allowed <= capacity / cost,
                "Fixed window bulk-cost: at most " + (capacity / cost) + " should be allowed, got " + allowed);
    }

    @Test
    void slidingWindowCounterBulkCostNeverExceedsCapacityConcurrent() throws Exception {
        int capacity = 30;
        int cost = 3;
        SlidingWindowCounterConfig cfg = new SlidingWindowCounterConfig(capacity, 60, TimeUnit.SECONDS);
        RateLimitKey k = key("swcBulkCost");
        RequestTime t = at(1000);

        int allowed = countAllowed(50, () -> repo.atomicConsumeAndUpdate(k, cfg, t, cost));

        assertTrue(allowed <= capacity / cost,
                "Sliding window counter bulk-cost: at most " + (capacity / cost) + " should be allowed, got " + allowed);
    }

    @Test
    void concurrentAllowedResultsHaveValidMetadata() throws Exception {
        int capacity = 20;
        TokenBucketConfig cfg = new TokenBucketConfig(capacity, 1, 1, TimeUnit.SECONDS);
        RateLimitKey k = key("tbMetaConcurrent");
        RequestTime t = at(1000);

        ExecutorService exec = Executors.newFixedThreadPool(capacity * 2);
        List<Future<RateLimitResult>> futures = new ArrayList<>();
        for (int i = 0; i < capacity * 2; i++) {
            futures.add(exec.submit(() -> repo.atomicConsumeAndUpdate(k, cfg, t, 1)));
        }
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);

        for (Future<RateLimitResult> f : futures) {
            RateLimitResult r = f.get();
            if (r.isAllowed()) {
                assertTrue(r.getResetAtMillis() > 0,
                        "resetAtMillis must be > 0 on allowed result, got " + r.getResetAtMillis());
                assertEquals(0, r.getRetryAfterMillis(),
                        "retryAfterMillis must be 0 on allowed result, got " + r.getRetryAfterMillis());
            } else {
                assertTrue(r.getRetryAfterMillis() >= 0,
                        "retryAfterMillis must be >= 0 on denied result, got " + r.getRetryAfterMillis());
            }
        }
    }

    @Test
    void remainingNeverNegativeConcurrentTokenBucket() throws Exception {
        int capacity = 10;
        TokenBucketConfig cfg = new TokenBucketConfig(capacity, 0, 1, TimeUnit.HOURS);
        RateLimitKey k = key("tbRemainingConcurrent");
        RequestTime t = at(1000);

        ExecutorService exec = Executors.newFixedThreadPool(100);
        List<Future<RateLimitResult>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(exec.submit(() -> repo.atomicConsumeAndUpdate(k, cfg, t, 1)));
        }
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);

        for (Future<RateLimitResult> f : futures) {
            RateLimitResult r = f.get();
            assertTrue(r.getRemaining() >= 0,
                    "remaining must never be negative, got " + r.getRemaining());
        }
    }

    @Test
    void remainingNeverNegativeConcurrentFixedWindow() throws Exception {
        int capacity = 10;
        FixedWindowCounterConfig cfg = new FixedWindowCounterConfig(capacity, 60, TimeUnit.SECONDS);
        RateLimitKey k = key("fwRemainingConcurrent");
        RequestTime t = at(1000);

        ExecutorService exec = Executors.newFixedThreadPool(100);
        List<Future<RateLimitResult>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(exec.submit(() -> repo.atomicConsumeAndUpdate(k, cfg, t, 1)));
        }
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);

        for (Future<RateLimitResult> f : futures) {
            RateLimitResult r = f.get();
            assertTrue(r.getRemaining() >= 0,
                    "remaining must never be negative, got " + r.getRemaining());
        }
    }
}
