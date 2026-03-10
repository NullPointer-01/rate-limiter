package org.nullpointer.ratelimiter.client;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimiterTest {

    @Test
    void clientDelegatesToEngine() {
        ConfigRepository configStore = new InMemoryConfigRepository();
        StateRepository stateStore = new InMemoryStateRepository();
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
