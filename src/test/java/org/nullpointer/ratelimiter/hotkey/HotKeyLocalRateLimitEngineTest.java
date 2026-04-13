package org.nullpointer.ratelimiter.hotkey;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.utils.SystemTimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class HotKeyLocalRateLimitEngineTest {

    private static final long WINDOW_MILLIS = 60_000L;

    private static final HotKeyConfig CONFIG = new HotKeyConfig.Builder()
            .warmThreshold(2)
            .hotThreshold(3)
            .detectionWindowMillis(5000)
            .syncIntervalMillis(100000) // Long interval to prevent syncing
            .hotLocalQuotaFraction(1.0)
            .build();

    // Hot key detection configuration
    private static final HotKeyConfig DETECTOR_CONFIG = new HotKeyConfig.Builder()
            .warmThreshold(10)
            .hotThreshold(50)
            .detectionWindowMillis(5_000)
            .syncIntervalMillis(3_600_000)   // 1 hour — effectively disabled for unit tests
            .build();

    private HotKeyLocalRateLimitEngine engine;
    private HotKeyLocalRateLimitEngine detector;

    @BeforeEach
    void setUp() {
        ConfigurationManager manager = new ConfigurationManager(
                new InMemoryConfigRepository(),
                new InMemoryStateRepository()
        );
        engine = new HotKeyLocalRateLimitEngine(
                CONFIG, manager, new SystemTimeSource(), newStripes());
        detector = new HotKeyLocalRateLimitEngine(
                DETECTOR_CONFIG,
                new ConfigurationManager(
                        new InMemoryConfigRepository(),
                        new InMemoryStateRepository()),
                new SystemTimeSource(),
                newStripes());
    }

    private static Object[] newStripes() {
        Object[] stripes = new Object[1024];
        for (int i = 0; i < stripes.length; i++) stripes[i] = new Object();
        return stripes;
    }

    private static FixedWindowCounterConfig config(long capacity) {
        return new FixedWindowCounterConfig(capacity, WINDOW_MILLIS, TimeUnit.MILLISECONDS);
    }

    private static RequestTime time(long nowMillis) {
        return new RequestTime(nowMillis, System.nanoTime());
    }

    private static RateLimitKey key(String userId) {
        return RateLimitKey.builder().setUserId(userId).build();
    }

    @Test
    void admitsUpToLocalLimit() {
        long capacity = 5;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-a");
        FixedWindowCounterConfig cfg = config(capacity);

        for (int i = 0; i < capacity; i++) {
            assertTrue(engine.process(k, cfg, 1, time(now)).isAllowed(),
                    "Request " + (i + 1) + " should be admitted");
        }
    }

    @Test
    void deniesBeyondLocalLimit() {
        long capacity = 3;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-b");
        FixedWindowCounterConfig cfg = config(capacity);

        for (int i = 0; i < capacity; i++) {
            engine.process(k, cfg, 1, time(now));
        }
        assertFalse(engine.process(k, cfg, 1, time(now)).isAllowed());
    }

    @Test
    void admitWithCostGreaterThanOne() {
        long capacity = 10;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-cost");
        FixedWindowCounterConfig cfg = config(capacity);

        assertTrue(engine.process(k, cfg, 4, time(now)).isAllowed());
        assertTrue(engine.process(k, cfg, 4, time(now)).isAllowed());

        // 4+4+4 = 12, capacity is 10
        assertFalse(engine.process(k, cfg, 4, time(now)).isAllowed());
    }

    @Test
    void costIsReflectedInRemainingCount() {
        long capacity = 10;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-cost-remaining");
        FixedWindowCounterConfig cfg = config(capacity);

        RateLimitResult result = engine.process(k, cfg, 3, time(now));
        assertTrue(result.isAllowed());

        assertEquals(7, result.getRemaining());
    }

    @Test
    void windowResetAllowsNewRequests() {
        long capacity = 2;
        long t0 = 1_000L;
        RateLimitKey k = key("key-c");
        FixedWindowCounterConfig cfg = config(capacity);

        engine.process(k, cfg, 1, time(t0));
        engine.process(k, cfg, 1, time(t0));
        assertFalse(engine.process(k, cfg, 1, time(t0)).isAllowed());  // full

        long t1 = t0 + WINDOW_MILLIS + 1; // Advance time
        assertTrue(engine.process(k, cfg, 1, time(t1)).isAllowed());
    }

    @Test
    void windowResetUnfreezes() {
        long capacity = 5;
        long t0 = 1_000L;
        RateLimitKey k = key("key-d");
        FixedWindowCounterConfig cfg = config(capacity);

        engine.process(k, cfg, 1, time(t0));
        engine.freeze(k.toKey());
        assertTrue(engine.isFrozen(k.toKey()));

        long t1 = t0 + WINDOW_MILLIS + 1;
        assertTrue(engine.process(k, cfg, 1, time(t1)).isAllowed());
        assertFalse(engine.isFrozen(k.toKey()));
    }

    @Test
    void frozenKeyDeniesAllRequests() {
        long capacity = 100;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-e");
        FixedWindowCounterConfig cfg = config(capacity);

        engine.process(k, cfg, 1, time(now));  // consume one
        engine.freeze(k.toKey());

        for (int i = 0; i < 10; i++) {
            assertFalse(engine.process(k, cfg, 1, time(now)).isAllowed(),
                    "Frozen key must deny request " + (i + 1));
        }
    }

    @Test
    void reflectsAdmittedCostUnitsSinceLastReset() {
        long capacity = 20;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-f-cost");
        FixedWindowCounterConfig cfg = config(capacity);

        engine.process(k, cfg, 3, time(now));
        engine.process(k, cfg, 2, time(now));

        assertEquals(5, engine.getAndResetCost(k.toKey()));
    }

    @Test
    void costReturnsZeroAfterReset() {
        long capacity = 10;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-g");
        FixedWindowCounterConfig cfg = config(capacity);

        engine.process(k, cfg, 1, time(now));
        engine.getAndResetCost(k.toKey());  // first drain
        assertEquals(0, engine.getAndResetCost(k.toKey()));
    }

    @Test
    void costAccumulatesBetweenResets() {
        long capacity = 20;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-h");
        FixedWindowCounterConfig cfg = config(capacity);

        for (int i = 0; i < 5; i++) engine.process(k, cfg, 1, time(now));
        assertEquals(5, engine.getAndResetCost(k.toKey()));

        for (int i = 0; i < 3; i++) engine.process(k, cfg, 1, time(now));
        assertEquals(3, engine.getAndResetCost(k.toKey()));
    }

    @Test
    void deltaOnUnknownKeyReturnsZero() {
        assertEquals(0, engine.getAndResetCost("user=nonexistent-key"));
    }

    @Test
    void resultMetadataOnAdmit() {
        long capacity = 10;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-result");
        FixedWindowCounterConfig cfg = config(capacity);

        RateLimitResult result = engine.process(k, cfg, 1, time(now));

        assertTrue(result.isAllowed());
        assertEquals(capacity - 1, result.getRemaining());
        assertEquals(0, result.getRetryAfterMillis());
        assertEquals(capacity, result.getLimit());
    }

    @Test
    void resultMetadataOnDenial() {
        long capacity = 10;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("key-denied");
        FixedWindowCounterConfig cfg = config(capacity);

        engine.process(k, cfg, 10, time(now)); // Exhaust quote
        RateLimitResult result = engine.process(k, cfg, 1, time(now));

        assertFalse(result.isAllowed());
        assertTrue(result.getRetryAfterMillis() > 0);
        assertEquals(0, result.getRemaining());
    }

    @Test
    void concurrentAdmissionsNeverExceedLocalLimit() throws Exception {
        long capacity = 100;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("conc-key");
        FixedWindowCounterConfig cfg = config(capacity);
        int threads = 20;

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicInteger admitted = new AtomicInteger();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                for (int r = 0; r < 10; r++) {  // 20 threads × 10 = 200 requests
                    if (engine.process(k, cfg, 1, time(now)).isAllowed()) {
                        admitted.incrementAndGet();
                    }
                }
                return null;
            });
        }

        List<Future<Void>> futures = exec.invokeAll(tasks);
        for (Future<Void> f : futures) f.get();
        exec.shutdown();

        assertEquals(capacity, admitted.get(),
                "Exactly localLimit=" + capacity + " requests should be admitted concurrently");
    }

    @Test
    void concurrentAdmissionsWithCostNeverExceedLocalLimit() throws Exception {
        long capacity = 100;
        long now = System.currentTimeMillis();
        RateLimitKey k = key("conc-key-cost");
        FixedWindowCounterConfig cfg = config(capacity);
        int cost = 3;
        int threads = 10;
        int requestsPerThread = 10;

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        AtomicInteger totalAdmittedCost = new AtomicInteger();
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                for (int r = 0; r < requestsPerThread; r++) {
                    if (engine.process(k, cfg, cost, time(now)).isAllowed()) {
                        totalAdmittedCost.addAndGet(cost);
                    }
                }
                return null;
            });
        }

        List<Future<Void>> futures = exec.invokeAll(tasks);
        for (Future<Void> f : futures) f.get();
        exec.shutdown();

        assertTrue(totalAdmittedCost.get() <= capacity,
                "Total admitted cost " + totalAdmittedCost.get() + " must not exceed capacity " + capacity);
    }

    @Test
    void unknownKeyReturnsCold() {
        assertEquals(KeyTemperature.COLD, detector.getTemperature("unknown-key"));
    }

    @Test
    void keyStaysColdBelowWarmThreshold() {
        long now = System.currentTimeMillis();
        KeyTemperature last = null;
        for (int i = 0; i < DETECTOR_CONFIG.getWarmThreshold() - 1; i++) {
            last = detector.recordAndClassify("key-cold", now);
        }
        assertEquals(KeyTemperature.COLD, last);
    }

    @Test
    void keyBecomesWarmAtWarmThreshold() {
        long now = System.currentTimeMillis();
        KeyTemperature last = null;
        for (int i = 0; i < DETECTOR_CONFIG.getWarmThreshold(); i++) {
            last = detector.recordAndClassify("key-warm", now);
        }
        assertEquals(KeyTemperature.WARM, last);
    }

    @Test
    void keyBecomesHotAtHotThreshold() {
        long now = System.currentTimeMillis();
        KeyTemperature last = null;
        for (int i = 0; i < DETECTOR_CONFIG.getHotThreshold(); i++) {
            last = detector.recordAndClassify("key-hot", now);
        }
        assertEquals(KeyTemperature.HOT, last);
    }

    @Test
    void hotKeyStaysHotForSubsequentRequestsInSameWindow() {
        long now = System.currentTimeMillis();
        for (int i = 0; i < DETECTOR_CONFIG.getHotThreshold(); i++) {
            detector.recordAndClassify("sticky-hot", now);
        }
        // One more request in same window — key must stay hot
        assertEquals(KeyTemperature.HOT, detector.recordAndClassify("sticky-hot", now));
    }

    @Test
    void warmKeyDoesNotDemoteToColMidWindow() {
        long now = System.currentTimeMillis();
        for (int i = 0; i < DETECTOR_CONFIG.getWarmThreshold(); i++) {
            detector.recordAndClassify("stays-warm", now);
        }
        // Key must stay warm
        assertEquals(KeyTemperature.WARM, detector.getTemperature("stays-warm"));
    }


    @Test
    void coldKeyStaysColdAfterWindowResetWithLowCount() {
        long t0 = 1_000L;
        for (int i = 0; i < 5; i++) {
            detector.recordAndClassify("low-traffic", t0);
        }
        // Advance past the detection window
        long t1 = t0 + DETECTOR_CONFIG.getDetectionWindowMillis() + 1;
        KeyTemperature after = detector.recordAndClassify("low-traffic", t1);
        assertEquals(KeyTemperature.COLD, after);
    }

    @Test
    void hotKeyMovesToWarmAfterWindowResetIfTrafficDropsAboveWarmThreshold() {
        long t0 = 1_000L;
        // Make it hot
        for (int i = 0; i < DETECTOR_CONFIG.getHotThreshold(); i++) {
            detector.recordAndClassify("drop-to-warm", t0);
        }

        // Advance time
        long t1 = t0 + DETECTOR_CONFIG.getDetectionWindowMillis() + 1;
        for (int i = 0; i < DETECTOR_CONFIG.getWarmThreshold(); i++) {
            detector.recordAndClassify("drop-to-warm", t1);
        }
        KeyTemperature temp = detector.getTemperature("drop-to-warm");
        assertEquals(KeyTemperature.WARM, temp);
    }

    @Test
    void hotKeyDemotesToColdAfterWindowResetWithVeryLowCount() {
        long t0 = 1_000L;
        // Make it hot
        for (int i = 0; i < DETECTOR_CONFIG.getHotThreshold(); i++) {
            detector.recordAndClassify("hot-then-cold", t0);
        }
        assertEquals(KeyTemperature.HOT, detector.getTemperature("hot-then-cold"));

        // Advance time
        long t1 = t0 + DETECTOR_CONFIG.getDetectionWindowMillis() + 1;
        KeyTemperature after = detector.recordAndClassify("hot-then-cold", t1);
        assertEquals(KeyTemperature.COLD, after);
    }


    @Test
    void getAllHotKeysReturnsOnlyHotKeys() {
        long now = System.currentTimeMillis();

        // hot-1 and hot-2 become HOT
        for (int i = 0; i < DETECTOR_CONFIG.getHotThreshold(); i++) {
            detector.recordAndClassify("hot-1", now);
            detector.recordAndClassify("hot-2", now);
        }
        // warm-1 is only WARM
        for (int i = 0; i < DETECTOR_CONFIG.getWarmThreshold(); i++) {
            detector.recordAndClassify("warm-1", now);
        }

        Set<String> hotKeys = detector.getAllHotKeys();
        assertTrue(hotKeys.contains("hot-1"));
        assertTrue(hotKeys.contains("hot-2"));
        assertFalse(hotKeys.contains("warm-1"));
    }

    @Test
    void getAllHotKeysIsEmptyWhenNoHotKeys() {
        long now = System.currentTimeMillis();
        detector.recordAndClassify("cold-key", now);
        assertTrue(detector.getAllHotKeys().isEmpty());
    }

    @Test
    void concurrentClassificationsDoNotCorruptTemperature() throws Exception {
        int threads = 5;
        int requestsPerThread = DETECTOR_CONFIG.getHotThreshold() / threads; // Must be divisible
        long now = System.currentTimeMillis();

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                // Key is not hot
                for (int r = 0; r < requestsPerThread-1; r++) {
                    assertNotEquals(KeyTemperature.HOT, detector.recordAndClassify("concurrent-key", now));
                }
                return null;
            });
        }

        List<Future<Void>> futures = exec.invokeAll(tasks);
        for (Future<Void> f : futures) {
            f.get();
        }

        // Key is still not hot
        for (int t = 0; t < threads-1; t++) {
            detector.recordAndClassify("concurrent-key", now);
        }

        exec.shutdown();

        // Last request to make key hot
        detector.recordAndClassify("concurrent-key", now);
        assertEquals(KeyTemperature.HOT, detector.getTemperature("concurrent-key"));
    }
}
