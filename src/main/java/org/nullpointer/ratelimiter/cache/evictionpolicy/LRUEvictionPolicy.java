package org.nullpointer.ratelimiter.cache.evictionpolicy;

import java.util.LinkedHashMap;
import java.util.Optional;

public class LRUEvictionPolicy<K> implements EvictionPolicy<K> {
    private final LinkedHashMap<K, Boolean> map;

    public LRUEvictionPolicy() {
        this.map = new LinkedHashMap<>(16, 0.75f, true);
    }

    @Override
    public void onKeyAccess(K key) {
        map.put(key, Boolean.TRUE);
    }

    @Override
    public void onKeyRemove(K key) {
        map.remove(key);
    }

    @Override
    public Optional<K> evictionCandidate() {
        if (map.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(map.keySet().iterator().next());
    }
}