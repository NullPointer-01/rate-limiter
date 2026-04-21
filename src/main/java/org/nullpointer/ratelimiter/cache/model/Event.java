package org.nullpointer.ratelimiter.cache.model;

public class Event<K> {
    private final EventType type;
    private final K key;

    public Event(EventType type, K key) {
        this.type = type;
        this.key = key;
    }

    public EventType getType() {
        return type;
    }

    public K getKey() {
        return key;
    }
}
