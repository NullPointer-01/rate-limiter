package org.nullpointer.ratelimiter.factory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.nullpointer.ratelimiter.model.config.RedisConnectionConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.InputStream;

public class RedisConnectionFactory {
    private static final String DEFAULT_CONFIG_PATH = "redis.yml";

    private RedisConnectionFactory() {}

    public static JedisPool createPool() {
        return createPool(DEFAULT_CONFIG_PATH);
    }

    public static JedisPool createPool(String configPath) {
        try (InputStream is = RedisConnectionFactory.class
                .getClassLoader().getResourceAsStream(configPath)) {

            if (is == null) throw new IllegalStateException(
                    "Redis configuration file not found on classpath: " + configPath);

            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            JsonNode root = mapper.readTree(is);
            RedisConnectionConfig cfg = mapper.treeToValue(root.get("redis"), RedisConnectionConfig.class);

            RedisConnectionConfig.PoolConfig pc = cfg.getPool();
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(pc.getMaxTotal());
            poolConfig.setMaxIdle(pc.getMaxIdle());
            poolConfig.setMinIdle(pc.getMinIdle());

            return new JedisPool(poolConfig, cfg.getHost(), cfg.getPort(),
                    cfg.getTimeout());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to load Redis configuration from: " + configPath, e);
        }
    }
}
