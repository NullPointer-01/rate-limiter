package org.nullpointer.ratelimiter.factory;

import org.nullpointer.ratelimiter.model.state.StateRepositoryType;
import org.nullpointer.ratelimiter.storage.state.AsyncRedisStateRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.RedisStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateRepositoryFactory {
    private static final Map<StateRepositoryType, StateRepository> registry = new ConcurrentHashMap<>();
    private static StateRepositoryFactory instance;

    private StateRepositoryFactory() {}

    public static StateRepositoryFactory getInstance() {
        if (instance == null) {
            synchronized (StateRepositoryFactory.class) {
                if (instance == null) {
                    instance = new StateRepositoryFactory();
                    registerDefaults();
                }
            }
        }
        return instance;
    }

    private static void registerDefaults() {
        registry.put(StateRepositoryType.IN_MEMORY, new InMemoryStateRepository());

        JedisPool pool = RedisConnectionFactory.createPool();
        JacksonSerializer serializer = new JacksonSerializer();

        registry.put(StateRepositoryType.REDIS, new RedisStateRepository(pool, serializer));
        registry.put(StateRepositoryType.ASYNC_REDIS, new AsyncRedisStateRepository(1000, pool, serializer));
    }

    public void register(StateRepositoryType type, StateRepository repository) {
        if (type == null) throw new IllegalArgumentException("StateRepositoryType cannot be null");
        if (repository == null) throw new IllegalArgumentException("StateRepository cannot be null");

        registry.put(type, repository);
    }

    public void clearRegistry() {
        registry.clear();
    }

    public StateRepository resolve(StateRepositoryType type) {
        if (type == null) throw new IllegalArgumentException("StateRepositoryType cannot be null");

        StateRepository repo = registry.get(type);
        if (repo == null) {
            throw new IllegalStateException("No StateRepository registered for type: " + type);
        }

        return repo;
    }

    public boolean isRegistered(StateRepositoryType type) {
        return type != null && registry.containsKey(type);
    }
}
