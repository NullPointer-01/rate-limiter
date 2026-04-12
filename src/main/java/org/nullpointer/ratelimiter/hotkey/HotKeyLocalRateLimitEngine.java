package org.nullpointer.ratelimiter.hotkey;

import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.TimeSource;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An in-memory Rate Limit Engine using Fixed Window Counter algorithm
 */
public class HotKeyLocalRateLimitEngine {

    private static final Logger logger = Logger.getLogger(HotKeyLocalRateLimitEngine.class.getName());
    private final ConcurrentHashMap<String, KeyBucket> buckets = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, HotKeyState> entries = new ConcurrentHashMap<>();

    private final ConfigurationManager configurationManager;
    private final HotKeyConfig config;
    private final TimeSource timeSource;

    // Shared with RateLimitEngine — same lock objects per key.
    private final Map<String, Object> engineLocks;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "hot-key-synchronizer");
                t.setDaemon(true);
                return t;
            });

    private volatile ScheduledFuture<?> future;

    public HotKeyLocalRateLimitEngine(HotKeyConfig config, ConfigurationManager configurationManager, TimeSource timeSource, Map<String, Object> engineLocks) {
        this.config = config;
        this.configurationManager = configurationManager;
        this.timeSource = timeSource;
        this.engineLocks = engineLocks;
        start();
    }

    public KeyTemperature recordAndClassify(String rawKey, long nowMillis) {
        HotKeyState entry = entries.computeIfAbsent(rawKey, k -> new HotKeyState(nowMillis));

        synchronized (entry) {
            if (nowMillis - entry.windowStartMillis >= config.getDetectionWindowMillis()) {
                entry.count = 0;
                entry.windowStartMillis = nowMillis;
                entry.temperature = KeyTemperature.COLD;
            }

            entry.count++;

            if (entry.count >= config.getHotThreshold()) {
                entry.temperature = KeyTemperature.HOT;
            } else if (entry.count >= config.getWarmThreshold() && entry.temperature != KeyTemperature.HOT) {
                entry.temperature = KeyTemperature.WARM;
            }

            return entry.temperature;
        }
    }

    public RateLimitResult process(RateLimitKey key, RateLimitConfig rateLimitConfig, int cost, RequestTime time) {
        String rawKey = key.toKey();
        long nowMillis = time.currentTimeMillis();
        long windowMillis = rateLimitConfig.getWindowSizeMillis();
        long globalCapacity = rateLimitConfig.getCapacity();
        long localLimit = Math.max(1,
                (long) (globalCapacity * config.getHotLocalQuotaFraction()));

        KeyBucket bucket = buckets.computeIfAbsent(rawKey, k -> new KeyBucket(nowMillis));

        synchronized (bucket) {
            if (nowMillis - bucket.windowStartMillis >= windowMillis) {
                bucket.windowCount = 0;
                bucket.syncedCount = 0;
                bucket.windowStartMillis = nowMillis;
                bucket.frozen = false;
            }

            long resetAtMillis = bucket.windowStartMillis + windowMillis;

            if (bucket.frozen) {
                return RateLimitResult.builder()
                        .allowed(false)
                        .limit(globalCapacity)
                        .remaining(0)
                        .resetAtMillis(resetAtMillis)
                        .retryAfterMillis(Math.max(0, resetAtMillis - nowMillis))
                        .build();
            }

            boolean allowed = bucket.windowCount + cost <= localLimit;
            if (allowed) {
                bucket.windowCount += cost;
            }

            return RateLimitResult.builder()
                    .allowed(allowed)
                    .limit(globalCapacity)
                    .remaining(Math.max(0, globalCapacity - bucket.windowCount))
                    .resetAtMillis(resetAtMillis)
                    .retryAfterMillis(allowed ? 0 : Math.max(0, resetAtMillis - nowMillis))
                    .build();
        }
    }

    public KeyTemperature getTemperature(String rawKey) {
        HotKeyState entry = entries.get(rawKey);
        if (entry == null) return KeyTemperature.COLD;

        synchronized (entry) {
            return entry.temperature;
        }
    }

    // Returns an unmodifiable snapshot of all hot keys
    public Set<String> getAllHotKeys() {
        Set<String> hotKeys = ConcurrentHashMap.newKeySet();
        entries.forEach((key, entry) -> {
            synchronized (entry) {
                if (entry.temperature == KeyTemperature.HOT) {
                    hotKeys.add(key);
                }
            }
        });
        return Collections.unmodifiableSet(hotKeys);
    }

    /**
     * Returns the number of request cost-units consumed locally since the last sync, then resets the cost.
     */
    public long getAndResetCost(String key) {
        KeyBucket bucket = buckets.get(key);
        if (bucket == null) return 0;

        synchronized (bucket) {
            long delta = bucket.windowCount - bucket.syncedCount;
            bucket.syncedCount = bucket.windowCount;
            return Math.max(0, delta);
        }
    }

    /**
     * Freezes so that processing returns denied for all subsequent requests until the local window resets.
     */
    public void freeze(String rawKey) {
        KeyBucket bucket = buckets.get(rawKey);
        if (bucket != null) {
            bucket.frozen = true;
        }
    }

    public boolean isFrozen(String rawKey) {
        KeyBucket bucket = buckets.get(rawKey);
        return bucket != null && bucket.frozen;
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
        logger.info("HotKeyLocalRateLimitEngine sync stopped");
    }

    // Exposed for testing
    public void syncAll() {
        Set<String> hotKeys = getAllHotKeys();
        for (String key : hotKeys) {
            try {
                syncKey(key);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error syncing hot key: " + key, e);
            }
        }
    }

    /**
     * Syncs a single key - drains local accumulated cost by replaying the request
     * Freezes if the global limit is exhausted.
     */
    private void syncKey(String rawKey) {
        long delta = getAndResetCost(rawKey);
        if (delta == 0) {
            return; // No change since last sync
        }

        RateLimitKey key = RateLimitKey.fromKey(rawKey);
        RequestTime time = timeSource.capture();

        // Acquire the same lock the RateLimitEngine uses for this key
        Object lock = engineLocks.computeIfAbsent(rawKey, k -> new Object());

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

    private static final class HotKeyState {
        long count;
        long windowStartMillis;
        KeyTemperature temperature;

        HotKeyState(long nowMillis) {
            this.count = 0;
            this.windowStartMillis = nowMillis;
            this.temperature = KeyTemperature.COLD;
        }
    }

    /**
     * Per-key bucket having State Sync information
     */
    private static final class KeyBucket {
        long windowCount = 0;
        long syncedCount = 0;
        long windowStartMillis;
        boolean frozen = false;

        KeyBucket(long nowMillis) {
            this.windowStartMillis = nowMillis;
        }
    }
}
