package org.nullpointer.ratelimiter.hotkey;

import org.nullpointer.ratelimiter.cache.Cache;
import org.nullpointer.ratelimiter.cache.SimpleCache;
import org.nullpointer.ratelimiter.cache.evictionpolicy.LRUEvictionPolicy;
import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.TimeSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A lock-free in-memory Rate Limit Engine using Fixed Window Counter algorithm.
 */
public class HotKeyLocalRateLimitEngine {

    private static final Logger logger = Logger.getLogger(HotKeyLocalRateLimitEngine.class.getName());
    private static final int DEFAULT_CACHE_CAPACITY = 10_000;
    private static final int MAX_RETRIES = 32;

    private final Cache<String, AtomicReference<KeyBucket>> buckets;
    private final Cache<String, AtomicReference<HotKeyState>> entries;

    // Secondary index — keys whose temperature is HOT. Kept in sync lazily.
    private final Set<String> hotKeysIndex = ConcurrentHashMap.newKeySet();

    private final ConfigurationManager configurationManager;
    private final HotKeyConfig config;
    private final TimeSource timeSource;

    // Shared with RateLimitEngine — same stripe array.
    private final Object[] engineLocks;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "hot-key-synchronizer");
                t.setDaemon(true);
                return t;
            });

    private volatile ScheduledFuture<?> future;

    public HotKeyLocalRateLimitEngine(HotKeyConfig config, ConfigurationManager configurationManager, TimeSource timeSource, Object[] engineLocks) {
        this.config = config;
        this.configurationManager = configurationManager;
        this.timeSource = timeSource;
        this.engineLocks = engineLocks;

        this.entries = new SimpleCache<>(DEFAULT_CACHE_CAPACITY, new LRUEvictionPolicy<>());
        this.buckets = new SimpleCache<>(DEFAULT_CACHE_CAPACITY, new LRUEvictionPolicy<>());

        start();
    }

    public KeyTemperature recordAndClassify(String rawKey, long nowMillis) {
        AtomicReference<HotKeyState> ref = entries.computeIfAbsent(rawKey,
                k -> new AtomicReference<>(HotKeyState.initial(nowMillis)));

        // Non-blocking CAS
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            HotKeyState current = ref.get();
            HotKeyState next = nextHotKeyState(current, nowMillis);

            if (ref.compareAndSet(current, next)) {
                if (next.temperature == KeyTemperature.HOT) {
                    hotKeysIndex.add(rawKey);
                } else if (current.temperature == KeyTemperature.HOT) {
                    hotKeysIndex.remove(rawKey);
                }
                return next.temperature;
            }
        }

        // Fallback to locking
        synchronized (ref) {
            HotKeyState current = ref.get();
            HotKeyState next = nextHotKeyState(current, nowMillis);
            ref.set(next);

            if (next.temperature == KeyTemperature.HOT) {
                hotKeysIndex.add(rawKey);
            } else if (current.temperature == KeyTemperature.HOT) {
                hotKeysIndex.remove(rawKey);
            }
            return next.temperature;
        }
    }

    public RateLimitResult process(RateLimitKey key, RateLimitConfig rateLimitConfig, int cost, RequestTime time) {
        String rawKey = key.toKey();
        long nowMillis = time.currentTimeMillis();
        long windowMillis = rateLimitConfig.getWindowSizeMillis();

        long globalCapacity = rateLimitConfig.getCapacity();
        long localLimit = Math.max(1, (long) (globalCapacity * config.getHotLocalQuotaFraction()));

        AtomicReference<KeyBucket> ref = buckets.computeIfAbsent(rawKey,
                k -> new AtomicReference<>(KeyBucket.initial(nowMillis)));

        // Non-blocking CAS
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            KeyBucket current = ref.get();
            ProcessResult pr = computeProcessResult(current, nowMillis, windowMillis, globalCapacity, localLimit, cost);

            if (pr.frozen) return pr.result;
            if (ref.compareAndSet(current, pr.nextBucket)) return pr.result;
        }

        // Fallback to locking
        synchronized (ref) {
            KeyBucket current = ref.get();
            ProcessResult pr = computeProcessResult(current, nowMillis, windowMillis, globalCapacity, localLimit, cost);

            if (!pr.frozen) ref.set(pr.nextBucket);
            return pr.result;
        }
    }

    public KeyTemperature getTemperature(String rawKey) {
        AtomicReference<HotKeyState> ref = entries.get(rawKey);
        if (ref == null) return KeyTemperature.COLD;
        return ref.get().temperature;
    }

    // Returns an unmodifiable snapshot of all hot keys
    public Set<String> getAllHotKeys() {
        return Collections.unmodifiableSet(new HashSet<>(hotKeysIndex));
    }

    /**
     * Returns the number of request cost-units consumed locally since the last sync, then resets the cost.
     */
    public long getAndResetCost(String key) {
        AtomicReference<KeyBucket> ref = buckets.get(key);
        if (ref == null) return 0;

        // Non-blocking CAS
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            KeyBucket current = ref.get();
            long delta = current.windowCount - current.syncedCount;
            if (delta <= 0) return 0;

            KeyBucket next = new KeyBucket(current.windowCount, current.windowCount, current.windowStartMillis, current.frozen);
            if (ref.compareAndSet(current, next)) return delta;
        }

        // Fallback to locking
        synchronized (ref) {
            KeyBucket current = ref.get();
            long delta = current.windowCount - current.syncedCount;
            if (delta <= 0) return 0;

            ref.set(new KeyBucket(current.windowCount, current.windowCount, current.windowStartMillis, current.frozen));
            return delta;
        }
    }

    /**
     * Freezes so that processing returns denied for all subsequent requests until the local window resets.
     */
    public void freeze(String rawKey) {
        AtomicReference<KeyBucket> ref = buckets.get(rawKey);
        if (ref == null) return;

        // Non-blocking CAS
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            KeyBucket current = ref.get();
            if (current.frozen) return;

            KeyBucket next = new KeyBucket(current.windowCount, current.syncedCount, current.windowStartMillis, true);
            if (ref.compareAndSet(current, next)) return;
        }

        // Fallback to locking
        synchronized (ref) {
            KeyBucket current = ref.get();
            if (!current.frozen) {
                ref.set(new KeyBucket(current.windowCount, current.syncedCount, current.windowStartMillis, true));
            }
        }
    }

    public boolean isFrozen(String rawKey) {
        AtomicReference<KeyBucket> ref = buckets.get(rawKey);
        return ref != null && ref.get().frozen;
    }

    private void start() {
        future = scheduler.scheduleWithFixedDelay(
                this::syncAll,
                config.getSyncIntervalMillis(),
                config.getSyncIntervalMillis(),
                TimeUnit.MILLISECONDS
        );
        logger.info("HotKeyLocalRateLimitEngine sync started (interval="
                + config.getSyncIntervalMillis() + "ms)");
    }

    public void stop() throws InterruptedException {
        if (future != null) {
            future.cancel(false);
        }
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);
        entries.close();
        buckets.close();
        logger.info("HotKeyLocalRateLimitEngine sync stopped");
    }

    // Exposed for testing
    public void syncAll() {
        for (String key : getAllHotKeys()) {
            try {
                // If the entry was evicted from the cache, clean up the index
                if (entries.get(key) == null) {
                    hotKeysIndex.remove(key);
                    continue;
                }
                syncKey(key);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error syncing hot key: " + key, e);
            }
        }
    }

    /**
     * Syncs a single key — drains local accumulated cost by replaying the request.
     * Freezes if the global limit is exhausted.
     */
    private void syncKey(String rawKey) {
        long delta = getAndResetCost(rawKey);
        if (delta == 0) {
            return; // No change since last sync
        }

        RateLimitKey key = RateLimitKey.fromKey(rawKey);
        RequestTime time = timeSource.capture();

        // Acquire the same stripe lock the RateLimitEngine uses for this key
        // Without the lock, a normal request and a sync can interleave
        Object lock = engineLocks[Math.abs(rawKey.hashCode()) % 1024];

        synchronized (lock) {
            try {
                RateLimitConfig rateLimitConfig = configurationManager.getConfig(key);
                RateLimitState state = configurationManager.getState(key);
                if (state == null) {
                    state = rateLimitConfig.initialRateLimitState(time.nanoTime());
                }

                // Replay the batched cost as a single consume.
                RateLimitResult result = rateLimitConfig.getAlgorithm().tryConsume(key, rateLimitConfig, state, time, delta);
                configurationManager.setState(key, state);

                if (!result.isAllowed()) {
                    freeze(rawKey);
                    logger.fine("Key '" + rawKey + "' frozen after sync: global limit reached "
                            + "(delta=" + delta + ")");
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Sync failed for key - " + rawKey + " cost - " + delta, e);
            }
        }
    }

    private HotKeyState nextHotKeyState(HotKeyState current, long nowMillis) {
        long windowStart = current.windowStartMillis;
        long count = current.count;
        KeyTemperature temp = current.temperature;

        // Reset if window is expired
        if (nowMillis - windowStart >= config.getDetectionWindowMillis()) {
            count = 0;
            windowStart = nowMillis;
            temp = KeyTemperature.COLD;
        }

        count++;

        if (count >= config.getHotThreshold()) {
            temp = KeyTemperature.HOT;
        } else if (count >= config.getWarmThreshold() && temp != KeyTemperature.HOT) {
            temp = KeyTemperature.WARM;
        }

        return new HotKeyState(count, windowStart, temp);
    }

    private ProcessResult computeProcessResult(KeyBucket current, long nowMillis, long windowMillis, long globalCapacity, long localLimit, int cost) {
        long wc = current.windowCount, sc = current.syncedCount, ws = current.windowStartMillis;
        boolean frozen = current.frozen;

        if (nowMillis - ws >= windowMillis) {
            wc = 0;
            sc = 0;
            ws = nowMillis;
            frozen = false;
        }
        long resetAtMillis = ws + windowMillis;

        if (frozen) {
            RateLimitResult result = RateLimitResult.builder()
                    .allowed(false).limit(globalCapacity).remaining(0)
                    .resetAtMillis(resetAtMillis)
                    .retryAfterMillis(Math.max(0, resetAtMillis - nowMillis)).build();
            return new ProcessResult(result, null, true);
        }

        boolean allowed = wc + cost <= localLimit;
        long newWc = allowed ? wc + cost : wc;
        KeyBucket nextBucket = new KeyBucket(newWc, sc, ws, false);

        RateLimitResult result = RateLimitResult.builder()
                .allowed(allowed).limit(globalCapacity)
                .remaining(Math.max(0, globalCapacity - newWc))
                .resetAtMillis(resetAtMillis)
                .retryAfterMillis(allowed ? 0 : Math.max(0, resetAtMillis - nowMillis)).build();
        return new ProcessResult(result, nextBucket, false);
    }

    // Immutable temperature-tracking state
    private record HotKeyState(long count, long windowStartMillis, KeyTemperature temperature) {
        static HotKeyState initial(long nowMillis) {
            return new HotKeyState(0, nowMillis, KeyTemperature.COLD);
        }
    }

    // Immutable per-key bucket for state sync
    private record KeyBucket(long windowCount, long syncedCount, long windowStartMillis, boolean frozen) {
        static KeyBucket initial(long nowMillis) {
            return new KeyBucket(0, 0, nowMillis, false);
        }
    }

    private record ProcessResult(RateLimitResult result, KeyBucket nextBucket, boolean frozen) { }
}
