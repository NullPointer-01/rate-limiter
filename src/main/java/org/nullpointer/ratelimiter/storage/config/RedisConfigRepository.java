package org.nullpointer.ratelimiter.storage.config;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.utils.JacksonSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisConfigRepository implements ConfigRepository {

    private static final String CONFIG_PREFIX = "rl:config:";
    private static final String DEFAULT_CONFIG_KEY = "rl:config:default";
    private static final String SCOPED_CONFIG_PREFIX = "rl:sconfig:";
    private static final String POLICY_KEY = "rl:policy";

    private final JedisPool jedisPool;
    private final JacksonSerializer serializer;

    public RedisConfigRepository(JedisPool jedisPool, JacksonSerializer serializer) {
        this.jedisPool = jedisPool;
        this.serializer = serializer;
    }

    @Override
    public void setDefaultConfig(RateLimitConfig config) {
        String json = serializer.serialize(config);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(DEFAULT_CONFIG_KEY, json);
        }
    }

    @Override
    public RateLimitConfig getDefaultConfig() {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(DEFAULT_CONFIG_KEY);
            if (json == null) {
                return null;
            }
            return serializer.deserialize(json, RateLimitConfig.class);
        }
    }

    @Override
    public void setConfig(RateLimitKey key, RateLimitConfig config) {
        String redisKey = CONFIG_PREFIX + key.toKey();
        String json = serializer.serialize(config);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(redisKey, json);
        }
    }

    @Override
    public RateLimitConfig getConfig(RateLimitKey key) {
        String redisKey = CONFIG_PREFIX + key.toKey();
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(redisKey);
            if (json == null) {
                return null;
            }
            return serializer.deserialize(json, RateLimitConfig.class);
        }
    }

    @Override
    public RateLimitConfig getOrDefaultConfig(RateLimitKey key) {
        RateLimitConfig config = getConfig(key);
        return config != null ? config : getDefaultConfig();
    }

    @Override
    public void setHierarchyPolicy(HierarchicalRateLimitPolicy policy) {
        String json = serializer.serialize(policy);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(POLICY_KEY, json);
        }
    }

    @Override
    public HierarchicalRateLimitPolicy getHierarchyPolicy() {
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(POLICY_KEY);
            if (json == null) {
                return null;
            }
            return serializer.deserialize(json, HierarchicalRateLimitPolicy.class);
        }
    }

    @Override
    public void setScopedConfig(RateLimitScope scope, String identifier, RateLimitConfig config) {
        String redisKey = toScopedConfigKey(scope, identifier);
        String json = serializer.serialize(config);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(redisKey, json);
        }
    }

    @Override
    public RateLimitConfig getScopedConfig(RateLimitScope scope, String identifier) {
        String redisKey = toScopedConfigKey(scope, identifier);
        try (Jedis jedis = jedisPool.getResource()) {
            String json = jedis.get(redisKey);
            if (json == null) {
                return null;
            }
            return serializer.deserialize(json, RateLimitConfig.class);
        }
    }

    private String toScopedConfigKey(RateLimitScope scope, String identifier) {
        return SCOPED_CONFIG_PREFIX + scope.getPrefix() + ":" + identifier;
    }
}
