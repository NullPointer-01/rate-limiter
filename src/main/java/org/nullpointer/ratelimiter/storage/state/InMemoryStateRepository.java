package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class InMemoryStateRepository implements StateRepository {

    private static final long DEFAULT_IDLE_EXPIRY_SECONDS = 3600L;
    private static final long DEFAULT_EVICTION_INTERVAL_SECONDS = 300L;

    private final ConcurrentHashMap<String, TimestampedState> stateMap;
    private final ConcurrentHashMap<String, TimestampedState> hierarchicalStateMap;
    private final long idleExpiryMs;
    private final ScheduledExecutorService evictionExecutor;

    public InMemoryStateRepository() {
        this(DEFAULT_IDLE_EXPIRY_SECONDS, DEFAULT_EVICTION_INTERVAL_SECONDS);
    }

    public InMemoryStateRepository(long idleExpirySeconds, long evictionIntervalSeconds) {
        this.stateMap = new ConcurrentHashMap<>();
        this.hierarchicalStateMap = new ConcurrentHashMap<>();
        this.idleExpiryMs = idleExpirySeconds * 1000L;

        this.evictionExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "state-repo-evictor");
            t.setDaemon(true);
            return t;
        });
        this.evictionExecutor.scheduleAtFixedRate(
                this::evictStaleEntries,
                evictionIntervalSeconds,
                evictionIntervalSeconds,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void setState(RateLimitKey key, RateLimitState state) {
        String k = key.toKey();
        TimestampedState entry = stateMap.get(k);
        if (entry != null) {
            entry.state = state;
            entry.lastAccessMs = System.currentTimeMillis();
        } else {
            stateMap.put(k, new TimestampedState(state, System.currentTimeMillis()));
        }
    }

    @Override
    public RateLimitState getState(RateLimitKey key) {
        return getFromMap(stateMap, key);
    }

    @Override
    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        String k = key.toKey();
        TimestampedState entry = hierarchicalStateMap.get(k);
        if (entry != null) {
            entry.state = state;
            entry.lastAccessMs = System.currentTimeMillis();
        } else {
            hierarchicalStateMap.put(k, new TimestampedState(state, System.currentTimeMillis()));
        }
    }

    @Override
    public RateLimitState getHierarchicalState(RateLimitKey key) {
        return getFromMap(hierarchicalStateMap, key);
    }

    private RateLimitState getFromMap(ConcurrentHashMap<String, TimestampedState> map, RateLimitKey key) {
        TimestampedState entry = map.get(key.toKey());
        if (entry == null) return null;

        long cutoff = System.currentTimeMillis() - idleExpiryMs;
        if (entry.lastAccessMs < cutoff) return null;

        entry.lastAccessMs = System.currentTimeMillis();
        return entry.state;
    }

    private void evictStaleEntries() {
        long cutoff = System.currentTimeMillis() - idleExpiryMs;
        stateMap.entrySet().removeIf(e -> e.getValue().lastAccessMs < cutoff);
        hierarchicalStateMap.entrySet().removeIf(e -> e.getValue().lastAccessMs < cutoff);
    }

    static final class TimestampedState {
        volatile RateLimitState state;
        volatile long lastAccessMs;

        TimestampedState(RateLimitState state, long lastAccessMs) {
            this.state = state;
            this.lastAccessMs = lastAccessMs;
        }
    }
}
