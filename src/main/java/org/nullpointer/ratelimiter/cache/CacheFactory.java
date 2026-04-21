package org.nullpointer.ratelimiter.cache;

import org.nullpointer.ratelimiter.cache.evictionpolicy.LRUEvictionPolicy;

public class CacheFactory<K, V> {
    public Cache<K, V> defaultCache(final int capacity) {
        return new SimpleCache<>(capacity, new LRUEvictionPolicy<>());
    }
}
