package org.nullpointer.ratelimiter.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RateLimitEngineTest {

    private ConfigurationManager manager;
    private RateLimitEngine engine;

    @BeforeEach
    void setUp() {
        manager = new ConfigurationManager(
                new InMemoryConfigRepository(),
                new InMemoryStateRepository()
        );
        engine = new RateLimitEngine(manager);
    }

    @Test
    void engineUsesConfigAndStateToEnforceLimits() {
        TokenBucketConfig config = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        manager.setConfig(key, config);

        RateLimitResult r1 = engine.process(key, 3);
        RateLimitResult r2 = engine.process(key, 3);

        assertTrue(r1.isAllowed());
        assertFalse(r2.isAllowed());
    }

    @Test
    void tokenBucketConcurrentRequestsNeverExceedCapacity() throws Exception {
        int capacity = 100;
        RateLimitKey key = RateLimitKey.builder().setUserId("conc-tb").build();
        manager.setConfig(key, new TokenBucketConfig(capacity, 1, 10, TimeUnit.SECONDS));

        assertEquals(capacity, runConcurrent(key, 20, 50));
    }

    @Test
    void fixedWindowConcurrentRequestsNeverExceedCapacity() throws Exception {
        int capacity = 100;
        RateLimitKey key = RateLimitKey.builder().setUserId("conc-fw").build();
        manager.setConfig(key, new FixedWindowCounterConfig(capacity, 10, TimeUnit.SECONDS));

        assertEquals(capacity, runConcurrent(key, 20, 50));
    }

    @Test
    void slidingWindowConcurrentRequestsNeverExceedCapacity() throws Exception {
        int capacity = 100;
        RateLimitKey key = RateLimitKey.builder().setUserId("conc-sw").build();
        manager.setConfig(key, new SlidingWindowConfig(capacity, 10, TimeUnit.SECONDS));

        assertEquals(capacity, runConcurrent(key, 20, 50));
    }

    @Test
    void slidingWindowCounterConcurrentRequestsNeverExceedCapacity() throws Exception {
        int capacity = 100;
        RateLimitKey key = RateLimitKey.builder().setUserId("conc-swc").build();
        manager.setConfig(key, new SlidingWindowCounterConfig(capacity, 10, TimeUnit.SECONDS));

        assertEquals(capacity, runConcurrent(key, 20, 50));
    }

    @Test
    void concurrentRequestsForDifferentKeysAreIndependent() throws Exception {
        int capacity = 50;
        RateLimitKey key1 = RateLimitKey.builder().setUserId("conc-independent-a").build();
        RateLimitKey key2 = RateLimitKey.builder().setUserId("conc-independent-b").build();
        manager.setConfig(key1, new TokenBucketConfig(capacity, 0, 1, TimeUnit.HOURS));
        manager.setConfig(key2, new TokenBucketConfig(capacity, 0, 1, TimeUnit.HOURS));

        int threads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger allowed1 = new AtomicInteger();
        AtomicInteger allowed2 = new AtomicInteger();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            tasks.add(() -> {
                RateLimitKey k = (idx % 2 == 0) ? key1 : key2;
                AtomicInteger counter = (idx % 2 == 0) ? allowed1 : allowed2;
                for (int j = 0; j < 10; j++) {
                    if (engine.process(k, 1).isAllowed()) counter.incrementAndGet();
                }
                return null;
            });
        }

        List<Future<Void>> futures = executor.invokeAll(tasks);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        for (Future<Void> f : futures) f.get();

        assertTrue(allowed1.get() <= capacity,
                "key1 allowed " + allowed1.get() + " but capacity is " + capacity);
        assertTrue(allowed2.get() <= capacity,
                "key2 allowed " + allowed2.get() + " but capacity is " + capacity);

        // Exhausting key1 must not affect key2
        assertNotEquals(0, allowed2.get(), "key2 should not be blocked by key1's quota");
    }

    private int runConcurrent(RateLimitKey key, int threads, int requestsPerThread) throws InterruptedException, ExecutionException {
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

        List<Future<Void>> futures = executor.invokeAll(tasks);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        for (Future<Void> f : futures) f.get();
        return allowed.get();
    }
}
