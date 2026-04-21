package org.nullpointer.ratelimiter.cache;

import java.util.function.Function;

public interface Cache<K, V> extends AutoCloseable {
    void set(K key, V value);

    V computeIfAbsent(K key, Function<K, V> mappingFunction);

    V get(K key);

    void remove(K key);

    @Override
    void close();
}
