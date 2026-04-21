package org.nullpointer.ratelimiter.cache.evictionpolicy;

import java.util.*;

public class LFUEvictionPolicy<K> implements EvictionPolicy<K> {
    private final Map<K, Integer> freqMap;
    private final Map<Integer, LinkedHashSet<K>> freqBuckets;
    private int minFreq;

    public LFUEvictionPolicy() {
        this.freqMap = new HashMap<>();
        this.freqBuckets = new HashMap<>();
        this.minFreq = 0;
    }

    @Override
    public void onKeyAccess(K key) {
        int currFreq = freqMap.getOrDefault(key, 0);
        // First insertion of key
        if (currFreq == 0) {
            freqMap.put(key, 1);
            freqBuckets.computeIfAbsent(1, k -> new LinkedHashSet<>()).add(key);
            minFreq = 1;
        }
        else {
            freqMap.put(key, currFreq + 1);
            freqBuckets.get(currFreq).remove(key);

            if (freqBuckets.get(currFreq).isEmpty()) {
                freqBuckets.remove(currFreq);
                if (currFreq == minFreq) minFreq = currFreq + 1;
            }

            freqBuckets.computeIfAbsent(currFreq + 1, k -> new LinkedHashSet<>()).add(key);
        }
    }

    @Override
    public void onKeyRemove(K key) {
        Integer freq = freqMap.remove(key);
        if (freq == null) return; // Key not present in cache

        freqBuckets.get(freq).remove(key);

        if (freqBuckets.get(freq).isEmpty()) {
            freqBuckets.remove(freq);
            // Rare scenario: O(n) scan only when a bucket is fully drained by removal
            // Hence, Hashmap is preferred over TreeMap for maintaining frequency counts
            if (freq == minFreq) {
                minFreq = freqMap.values().stream().mapToInt(i -> i).min().orElse(0);
            }
        }
    }

    @Override
    public Optional<K> evictionCandidate() {
        if (freqMap.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(freqBuckets.get(minFreq).iterator().next());
    }
}
