package org.nullpointer.ratelimiter.cache.evictionpolicy;

import java.util.Optional;

public interface EvictionPolicy<K> {
    void onKeyAccess(K key);
    void onKeyRemove(K key);
    Optional<K> evictionCandidate();
}