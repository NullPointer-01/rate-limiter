package org.nullpointer.ratelimiter.cache;

import org.nullpointer.ratelimiter.cache.evictionpolicy.EvictionPolicy;
import org.nullpointer.ratelimiter.cache.model.Event;
import org.nullpointer.ratelimiter.cache.model.StripedBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class SimpleCache<K, V> implements Cache<K, V> {
    private final ConcurrentHashMap<K, V> map;
    private final int capacity;
    private final EvictionPolicy<K> evictionPolicy;

    private final List<StripedBuffer<K>> buffers;
    private final ReentrantLock maintenanceLock;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> future;

    private static final int READ_DRAIN_LIMIT_PER_STRIPE = 16;
    private static final int BUFFERS_SIZE = 16;

    public SimpleCache(int capacity, EvictionPolicy<K> evictionPolicy) {
        this.capacity = capacity;
        this.evictionPolicy = evictionPolicy;

        this.map = new ConcurrentHashMap<>();
        this.buffers = new ArrayList<>(BUFFERS_SIZE);
        for (int i = 0; i < BUFFERS_SIZE; i++) this.buffers.add(new StripedBuffer<>());

        this.maintenanceLock = new ReentrantLock();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "maintenance-thread");
            t.setDaemon(true);
            return t;
        });
        this.future = scheduler.scheduleWithFixedDelay(this::maintenance, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void set(K key, V value) {
        V old = map.put(key, value);
        StripedBuffer<K> stripe = buffers.get(stripeIndex());
        if (old == null) {
            stripe.recordAdd(key);
        } else {
            stripe.recordUpdate(key);
        }
        tryMaintenance();
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> mappingFunction) {
        AtomicBoolean created = new AtomicBoolean(false);
        V value = map.computeIfAbsent(key, k -> {
            created.set(true);
            return mappingFunction.apply(k);
        });
        StripedBuffer<K> stripe = buffers.get(stripeIndex());
        if (created.get()) {
            stripe.recordAdd(key);
            tryMaintenance();
        } else {
            stripe.recordAccess(key);
        }
        return value;
    }

    @Override
    public V get(K key) {
        V value = map.get(key);
        if (value != null) {
            buffers.get(stripeIndex()).recordAccess(key);
        }
        return value;
    }

    @Override
    public void remove(K key) {
        if (map.remove(key) != null) {
            buffers.get(stripeIndex()).recordRemoval(key);
            tryMaintenance();
        }
    }

    public void close() {
        future.cancel(false);
        scheduler.shutdown();
    }

    private void tryMaintenance() {
        if (maintenanceLock.tryLock()) {
            try {
                drainAndEvict();
            } finally {
                maintenanceLock.unlock();
            }
        }
    }

    private void maintenance() {
        maintenanceLock.lock();
        try {
            drainAndEvict();
        } finally {
            maintenanceLock.unlock();
        }
    }

    private void drainAndEvict() {
        // Step 1: Drain all write events first
        for (int i = 0; i < BUFFERS_SIZE; i++) {
            Event<K> event;
            while ((event = buffers.get(i).pollWrite()) != null) {
                switch (event.getType()) {
                    case ADD, UPDATE -> evictionPolicy.onKeyAccess(event.getKey());
                    case REMOVE      -> evictionPolicy.onKeyRemove(event.getKey());
                }
            }
        }

        // Step 2: Drain read events — bounded per stripe
        for (int i = 0; i < BUFFERS_SIZE; i++) {
            K key;
            int stripeDrained = 0;
            while (stripeDrained++ < READ_DRAIN_LIMIT_PER_STRIPE && (key = buffers.get(i).pollAccess()) != null) {
                if (map.containsKey(key)) {
                    evictionPolicy.onKeyAccess(key);
                }
            }
        }

        // Step 3: Enforce capacity
        while (map.size() > capacity) {
            Optional<K> candidate = evictionPolicy.evictionCandidate();
            if (candidate.isEmpty()) break;

            map.remove(candidate.get());
            evictionPolicy.onKeyRemove(candidate.get());
        }
    }

    private int stripeIndex() {
        return (int) (Thread.currentThread().getId() % BUFFERS_SIZE);
    }
}

