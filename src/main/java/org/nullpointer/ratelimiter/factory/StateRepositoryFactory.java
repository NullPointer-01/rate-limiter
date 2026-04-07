package org.nullpointer.ratelimiter.factory;

import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.storage.state.AsyncRedisStateRepository;
import org.nullpointer.ratelimiter.storage.state.AtomicStateRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryAtomicStateRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.RedisLuaStateRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateRepositoryFactory {
    private final Map<StateRepositoryType, StateRepository> registry = new ConcurrentHashMap<>();
    private final Map<StateRepositoryType, AtomicStateRepository> atomicRegistry = new ConcurrentHashMap<>();

    public void registerDefaults(JedisPool pool) {
        JacksonSerializer serializer = new JacksonSerializer();

        registry.put(StateRepositoryType.IN_MEMORY, new InMemoryStateRepository());
        registry.put(StateRepositoryType.REDIS, new RedisStateRepository(pool, serializer));
        registry.put(StateRepositoryType.ASYNC_REDIS, new AsyncRedisStateRepository(1000, pool, serializer));

        atomicRegistry.put(StateRepositoryType.IN_MEMORY_ATOMIC, new InMemoryAtomicStateRepository());
        atomicRegistry.put(StateRepositoryType.REDIS_ATOMIC, new RedisLuaStateRepository(pool));
    }

    public void register(StateRepositoryType type, StateRepository repository) {
        if (type == null) throw new IllegalArgumentException("StateRepositoryType cannot be null");
        if (repository == null) throw new IllegalArgumentException("StateRepository cannot be null");

        registry.put(type, repository);
    }

    public void registerAtomic(StateRepositoryType type, AtomicStateRepository repository) {
        if (type == null) throw new IllegalArgumentException("StateRepositoryType cannot be null");
        if (repository == null) throw new IllegalArgumentException("AtomicStateRepository cannot be null");

        atomicRegistry.put(type, repository);
    }

    public StateRepository resolve(StateRepositoryType type) {
        if (type == null) throw new IllegalArgumentException("StateRepositoryType cannot be null");

        StateRepository repo = registry.get(type);
        if (repo == null) {
            throw new IllegalStateException("No StateRepository registered for type: " + type);
        }

        return repo;
    }

    public AtomicStateRepository resolveAtomic(StateRepositoryType type) {
        if (type == null) throw new IllegalArgumentException("StateRepositoryType cannot be null");

        AtomicStateRepository repo = atomicRegistry.get(type);
        if (repo == null) {
            throw new IllegalStateException("No AtomicStateRepository registered for type: " + type);
        }

        return repo;
    }
}
