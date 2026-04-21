package org.nullpointer.ratelimiter.cache.model;

import java.util.concurrent.ConcurrentLinkedQueue;

public class StripedBuffer<K> {
    private static final int READ_BUFFER_CAPACITY = 256;

    private final CircularBuffer<K> readBuffer;
    private final ConcurrentLinkedQueue<Event<K>> writeBuffer; // Unbounded buffer

    public StripedBuffer() {
        this.readBuffer = new CircularBuffer<>(READ_BUFFER_CAPACITY);
        this.writeBuffer = new ConcurrentLinkedQueue<>();
    }

    public void recordAccess(K key) {
        readBuffer.offer(key);
    }

    public K pollAccess() {
        return readBuffer.poll();
    }

    public void recordAdd(K key) {
        writeBuffer.offer(new Event<>(EventType.ADD, key));
    }

    public void recordUpdate(K key) {
        writeBuffer.offer(new Event<>(EventType.UPDATE, key));
    }

    public void recordRemoval(K key) {
        writeBuffer.offer(new Event<>(EventType.REMOVE, key));
    }

    public Event<K> pollWrite() {
        return writeBuffer.poll();
    }
}
