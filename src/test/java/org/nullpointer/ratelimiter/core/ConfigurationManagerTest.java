package org.nullpointer.ratelimiter.core;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.InMemoryStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationManagerTest {

    @Test
    void returnsSpecificOrDefaultConfig() {
        InMemoryStore store = new InMemoryStore();
        ConfigurationManager manager = new ConfigurationManager(store);

        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        manager.setDefaultConfig(defaultConfig);

        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        TokenBucketConfig specific = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        manager.setConfig(key, specific);

        assertSame(specific, manager.getConfig(key));
    }

    @Test
    void throwsWhenNoConfigAtAll() {
        InMemoryStore store = new InMemoryStore();
        ConfigurationManager manager = new ConfigurationManager(store);
        RateLimitKey key = RateLimitKey.builder().setUserId("user-missing").build();
        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getConfig(key));
    }
}
