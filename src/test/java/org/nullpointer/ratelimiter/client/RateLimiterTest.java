package org.nullpointer.ratelimiter.client;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.config.ConfigStore;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigStore;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateStore;
import org.nullpointer.ratelimiter.storage.state.StateStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimiterTest {

    @Test
    void clientDelegatesToEngine() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager manager = new ConfigurationManager(configStore, stateStore);
        RateLimiter rateLimiter = new RateLimiter(manager);

        TokenBucketConfig config = new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS);
        RateLimitKey key = RateLimitKey.builder().setUserId("client-user").build();
        manager.setConfig(key, config);

        RateLimitResult r1 = rateLimiter.process(key);
        RateLimitResult r2 = rateLimiter.process(key);
        RateLimitResult r3 = rateLimiter.process(key);

        assertTrue(r1.isAllowed());
        assertTrue(r2.isAllowed());
        assertFalse(r3.isAllowed());
    }
}
