package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class AsyncRedisStateRepository implements StateRepository {
    private static final Logger logger = Logger.getLogger(AsyncRedisStateRepository.class.getName());

    private static final String STATE_PREFIX = "rl:state:";
    private static final String HIERARCHICAL_STATE_PREFIX = "rl:hstate:";

    private static final long DEFAULT_TTL_SECONDS = 7200L;

    private final JedisPool jedisPool;
    private final JacksonSerializer serializer;
    private final long ttlSeconds;

    // Local InMemoryState
    private final Map<String, RateLimitState> stateMap;
    private final AtomicReference<Set<String>> dirtyKeys;

    private final long syncIntervalInMillis;
    private final ScheduledExecutorService executor;

    public AsyncRedisStateRepository(long syncIntervalInMillis, JedisPool jedisPool, JacksonSerializer serializer) {
        this(syncIntervalInMillis, DEFAULT_TTL_SECONDS, jedisPool, serializer);
    }

    public AsyncRedisStateRepository(long syncIntervalInMillis, long ttlSeconds, JedisPool jedisPool, JacksonSerializer serializer) {
        this.syncIntervalInMillis = syncIntervalInMillis;
        this.ttlSeconds = ttlSeconds;
        this.jedisPool = jedisPool;
        this.serializer = serializer;

        this.stateMap = new ConcurrentHashMap<>();
        this.dirtyKeys = new AtomicReference<>(ConcurrentHashMap.newKeySet());

        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "Thread - Async Redis Service");
            t.setDaemon(true);
            return t;
        };
        this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    public void start() {
        this.executor.scheduleWithFixedDelay(
                this::syncToRedis,
                syncIntervalInMillis, // Immediate flush on start is not required
                syncIntervalInMillis,
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        this.syncToRedis();
        this.executor.shutdown();
    }

    void syncToRedis() {
        Set<String> keysToFlush = dirtyKeys.getAndSet(ConcurrentHashMap.newKeySet());
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();

            for (String redisKey : keysToFlush) {
                RateLimitState state = stateMap.get(redisKey);
                if (state != null) {
                    pipeline.setex(redisKey, ttlSeconds, serializer.serialize(state));
                }

            }
            pipeline.sync();

            // Evict from write-back buffer for keys that were not re-dirtied during the flush
            Set<String> currentDirty = dirtyKeys.get();
            for (String key : keysToFlush) {
                if (!currentDirty.contains(key)) {
                    stateMap.remove(key);
                }
            }
        } catch (Exception ex) {
            logger.warning("Redis flush failed, re-queuing " + keysToFlush.size() + " keys: " + ex.getMessage());
            // Merge back into dirty set for retry at next sync
            dirtyKeys.get().addAll(keysToFlush);
        }
    }

    @Override
    public void setState(RateLimitKey key, RateLimitState state) {
        String redisKey = STATE_PREFIX + key.toKey();
        this.stateMap.put(redisKey, state);
        this.dirtyKeys.get().add(redisKey);
    }

    @Override
    public RateLimitState getState(RateLimitKey key) {
        String redisKey = STATE_PREFIX + key.toKey();
        RateLimitState state = this.stateMap.get(redisKey);

        // Fetch from Local, if not present fallback to Redis
        if (state == null) {
            try (Jedis jedis = jedisPool.getResource()) {
                String json = jedis.get(redisKey);
                if (json == null) {
                    return null;
                }
                return serializer.deserialize(json, RateLimitState.class);
            }
        }

        return state;
    }

    @Override
    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        String redisKey = HIERARCHICAL_STATE_PREFIX + key.toKey();
        this.stateMap.put(redisKey, state);
        this.dirtyKeys.get().add(redisKey);
    }

    @Override
    public RateLimitState getHierarchicalState(RateLimitKey key) {
        String redisKey = HIERARCHICAL_STATE_PREFIX + key.toKey();
        RateLimitState state = this.stateMap.get(redisKey);

        // Fetch from Local, if not present fallback to Redis
        if (state == null) {
            try (Jedis jedis = jedisPool.getResource()) {
                String json = jedis.get(redisKey);
                if (json == null) {
                    return null;
                }
                return serializer.deserialize(json, RateLimitState.class);
            }
        }

        return state;
    }
}
