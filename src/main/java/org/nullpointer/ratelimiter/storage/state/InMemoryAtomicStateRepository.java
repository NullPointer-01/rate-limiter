package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryAtomicStateRepository implements AtomicStateRepository {

    private static final int MAX_RETRIES = 32;
    private static final long DEFAULT_IDLE_EXPIRY_SECONDS = 3600L;
    private static final long DEFAULT_EVICTION_INTERVAL_SECONDS = 300L;

    private final ConcurrentHashMap<String, AtomicReference<TimestampedState>> stateMap = new ConcurrentHashMap<>();
    private final long idleExpiryMs;
    private final ScheduledExecutorService evictionExecutor;

    public InMemoryAtomicStateRepository() {
        this(DEFAULT_IDLE_EXPIRY_SECONDS, DEFAULT_EVICTION_INTERVAL_SECONDS);
    }

    public InMemoryAtomicStateRepository(long idleExpirySeconds, long evictionIntervalSeconds) {
        this.idleExpiryMs = idleExpirySeconds * 1000L;
        this.evictionExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "atomic-state-repo-evictor");
            t.setDaemon(true);
            return t;
        });
        this.evictionExecutor.scheduleWithFixedDelay(
                this::evictStaleEntries,
                evictionIntervalSeconds,
                evictionIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    @Override
    public RateLimitResult atomicConsumeAndUpdate(RateLimitKey key, RateLimitConfig config, RequestTime time, int cost) {
        String k = key.toKey();
        AtomicReference<TimestampedState> ref =
                stateMap.computeIfAbsent(k, k1 -> new AtomicReference<>(null));

        long nowMs = time.currentTimeMillis();

        // Non-blocking CAS
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            TimestampedState current = ref.get();
            RateLimitState currentState = (current == null)
                    ? config.initialRateLimitState(time.nanoTime())
                    : current.state.copy();

            RateLimitResult result = config.getAlgorithm().tryConsume(key, config, currentState, time, cost);

            TimestampedState next = new TimestampedState(currentState, nowMs);
            if (ref.compareAndSet(current, next)) {
                return result;
            }
        }

        // Fallback to locking
        synchronized (ref) {
            TimestampedState current = ref.get();
            RateLimitState currentState = (current == null)
                    ? config.initialRateLimitState(time.nanoTime())
                    : current.state.copy();

            RateLimitResult result = config.getAlgorithm().tryConsume(key, config, currentState, time, cost);
            ref.set(new TimestampedState(currentState, nowMs));
            return result;
        }
    }

    /**
     * Two-phase eviction:
     * 1) CAS null into stale refs. Never removes the map entry to preserve CAS safety.
     * 2) Remove map entries whose ref is still null. If not null, it implies that a new request has re-populated it.
     */
    void evictStaleEntries() {
        long cutoff = System.currentTimeMillis() - idleExpiryMs;

        // Set null for stale entries
        stateMap.forEach((key, ref) -> {
            TimestampedState ts = ref.get();
            if (ts != null && ts.lastAccessMs < cutoff) {
                ref.compareAndSet(ts, null);
            }
        });

        // Remove entries that are still null
        stateMap.entrySet().removeIf(e -> e.getValue().get() == null);
    }

    static final class TimestampedState {
        final RateLimitState state;
        final long lastAccessMs;

        TimestampedState(RateLimitState state, long lastAccessMs) {
            this.state = state;
            this.lastAccessMs = lastAccessMs;
        }
    }
}
