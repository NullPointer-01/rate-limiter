package org.nullpointer.ratelimiter.cache.model;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A fixed-size, lock-free MPSC (multi-producer, single-consumer) circular buffer.
 */
public class CircularBuffer<K> {
    private final AtomicReferenceArray<K> buffer;
    private final int mask;
    private final int capacity;

    // Monotonically increasing write cursor
    private final AtomicLong tail = new AtomicLong(0);

    // Read cursor
    private long head = 0;

    public CircularBuffer(int capacity) {
        // Capacity must be a power of 2 for efficient index masking.
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Capacity must be a positive power of 2, got: " + capacity);
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        this.buffer = new AtomicReferenceArray<>(capacity);
    }

    /**
     * Appends a key to the buffer. If the buffer is full, the oldest unread slot is silently overwritten.
     */
    public void offer(K key) {
        long claimed = tail.getAndIncrement();
        int slot = (int) (claimed & mask);
        buffer.set(slot, key);
    }

    /**
     * Only a single thread may call poll.
     * Polls the next available key from the buffer.
     */
    public K poll() {
        long currentTail = tail.get();

        // Nothing to read
        if (head >= currentTail) {
            return null;
        }

        // Skip overwritten entries
        if (currentTail - head > capacity) {
            head = currentTail - capacity;
        }

        while (head < currentTail) {
            int slot = (int) (head & mask);
            K value = buffer.getAndSet(slot, null);
            head++;

            if (value != null) {
                return value;
            }
            // If value is null, it means a producer claimed the slot but hasn't written yet (race condition)
        }

        return null;
    }

    /**
     * Returns an estimate of the number of unread entries in the buffer.
     */
    public int size() {
        long diff = tail.get() - head;

        if (diff <= 0) return 0;
        if (diff > capacity) return capacity;

        return (int) diff;
    }
}
