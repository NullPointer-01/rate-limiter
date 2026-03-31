package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisStateRepository implements StateRepository {

    private static final String STATE_PREFIX = "rl:state:";
    private static final String HIERARCHICAL_STATE_PREFIX = "rl:hstate:";

    private final JedisPool jedisPool;
    private final JacksonSerializer serializer;

    public RedisStateRepository(JedisPool jedisPool, JacksonSerializer serializer) {
        this.jedisPool = jedisPool;
        this.serializer = serializer;
    }

    @Override
    public void setState(RateLimitKey key, RateLimitState state) {
        String redisKey = STATE_PREFIX + key.toKey();
        String json = serializer.serialize(state);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(redisKey, json);
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
            jedis.set(redisKey, json);
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
