package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class InMemoryAtomicStateRepository implements AtomicStateRepository {

    private static final int MAX_RETRIES = 32;
    private final Map<String, AtomicReference<RateLimitState>> stateMap = new ConcurrentHashMap<>();

    @Override
    public RateLimitResult atomicConsumeAndUpdate(RateLimitKey key, RateLimitConfig config, RequestTime time, int cost) {
        String k = key.toKey();
        AtomicReference<RateLimitState> ref =
                stateMap.computeIfAbsent(k, k1 -> new AtomicReference<>(null));

        // Non-blocking CAS
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            RateLimitState current = ref.get();
            RateLimitState copy = (current == null)
                    ? config.initialRateLimitState(time.nanoTime())
                    : current.copy();

            RateLimitResult result = config.getAlgorithm().tryConsume(key, config, copy, time, cost);

            // CAS succeeded
            if (ref.compareAndSet(current, copy)) {
                return result;
            }
        }

        // Fallback to locking
        synchronized (ref) {
            RateLimitState current = ref.get();
            RateLimitState copy = (current == null) ? config.initialRateLimitState(time.nanoTime()) : current.copy();

            RateLimitResult result = config.getAlgorithm().tryConsume(key, config, copy, time, cost);
            ref.set(copy);
            return result;
        }
    }
}
