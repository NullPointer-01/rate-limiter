package org.nullpointer.ratelimiter.model.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RedisConnectionConfig {
    @JsonProperty("host")
    private String host = "localhost";

    @JsonProperty("port")
    private int port = 6379;

    @JsonProperty("timeout")
    private int timeout = 2000;

    @JsonProperty("database")
    private int database = 0;

    @JsonProperty("pool")
    private PoolConfig pool = new PoolConfig();

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getDatabase() {
        return database;
    }

    public PoolConfig getPool() {
        return pool;
    }

    public static class PoolConfig {
        @JsonProperty("maxTotal")
        private int maxTotal = 64;
        @JsonProperty("maxIdle")
        private int maxIdle = 16;
        @JsonProperty("minIdle")
        private int minIdle = 4;

        public int getMaxTotal() {
            return maxTotal;
        }

        public int getMaxIdle() {
            return maxIdle;
        }

        public int getMinIdle() {
            return minIdle;
        }
    }
}
