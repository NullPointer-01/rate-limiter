package org.nullpointer.ratelimiter.core;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.config.ConfigStore;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigStore;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateStore;
import org.nullpointer.ratelimiter.storage.state.StateStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigurationManagerTest {

    @Test
    void returnsSpecificOrDefaultConfig() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager manager = new ConfigurationManager(configStore, stateStore);

        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        manager.setDefaultConfig(defaultConfig);

        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        TokenBucketConfig specific = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        manager.setConfig(key, specific);

        assertSame(specific, manager.getConfig(key));
    }

    @Test
    void throwsWhenNoConfigAtAll() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager manager = new ConfigurationManager(configStore, stateStore);
        RateLimitKey key = RateLimitKey.builder().setUserId("user-missing").build();
        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getConfig(key));
    }
}
