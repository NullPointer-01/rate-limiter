package org.nullpointer.ratelimiter.cache.evictionpolicy;

import java.util.LinkedHashSet;
import java.util.Optional;

public class FIFOEvictionPolicy<K> implements EvictionPolicy<K> {
    private final LinkedHashSet<K> set;

    public FIFOEvictionPolicy() {
        this.set = new LinkedHashSet<>();
    }

    @Override
    public void onKeyAccess(K key) {
        set.add(key);
    }

    @Override
    public void onKeyRemove(K key) {
        set.remove(key);
    }

    @Override
    public Optional<K> evictionCandidate() {
        if (set.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(set.iterator().next());
    }
}