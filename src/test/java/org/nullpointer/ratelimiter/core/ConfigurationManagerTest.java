package org.nullpointer.ratelimiter.core;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigurationManagerTest {

    @Test
    void returnsSpecificOrDefaultConfig() {
        ConfigRepository configStore = new InMemoryConfigRepository();
        StateRepository stateStore = new InMemoryStateRepository();
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
        ConfigRepository configStore = new InMemoryConfigRepository();
        StateRepository stateStore = new InMemoryStateRepository();
        ConfigurationManager manager = new ConfigurationManager(configStore, stateStore);
        RateLimitKey key = RateLimitKey.builder().setUserId("user-missing").build();
        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getConfig(key));
    }
}
