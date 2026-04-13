package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisStateRepository implements StateRepository {

    private static final String STATE_PREFIX = "rl:state:";
    private static final String HIERARCHICAL_STATE_PREFIX = "rl:hstate:";

    private static final long DEFAULT_TTL_SECONDS = 7200L;

    private final JedisPool jedisPool;
    private final JacksonSerializer serializer;
    private final long ttlSeconds;

    public RedisStateRepository(JedisPool jedisPool, JacksonSerializer serializer) {
        this(jedisPool, serializer, DEFAULT_TTL_SECONDS);
    }

    public RedisStateRepository(JedisPool jedisPool, JacksonSerializer serializer, long ttlSeconds) {
        this.jedisPool = jedisPool;
        this.serializer = serializer;
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public void setState(RateLimitKey key, RateLimitState state) {
        String redisKey = STATE_PREFIX + key.toKey();
        String json = serializer.serialize(state);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(redisKey, ttlSeconds, json);
        }
    }

    @Override
    public RateLimitState getState(RateLimitKey key) {
        String redisKey = STATE_PREFIX + key.toKey();
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(redisKey);
            if (json == null) {
                return null;
            }
            return serializer.deserialize(json, RateLimitState.class);
        }
    }

    @Override
    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        String redisKey = HIERARCHICAL_STATE_PREFIX + key.toKey();
        String json = serializer.serialize(state);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(redisKey, ttlSeconds, json);
        }
    }

    @Override
    public RateLimitState getHierarchicalState(RateLimitKey key) {
        String redisKey = HIERARCHICAL_STATE_PREFIX + key.toKey();
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(redisKey);
            if (json == null) {
                return null;
            }
            return serializer.deserialize(json, RateLimitState.class);
        }
    }
}
