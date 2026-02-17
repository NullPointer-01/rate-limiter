package org.nullpointer.ratelimiter.core;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.InMemoryStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RateLimitEngineTest {

    @Test
    void engineUsesConfigAndStateToEnforceLimits() {
        InMemoryStore store = new InMemoryStore();
        ConfigurationManager manager = new ConfigurationManager(store);
        RateLimitEngine engine = new RateLimitEngine(manager);

        TokenBucketConfig config = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        manager.setConfig(key, config);

        RateLimitResult r1 = engine.process(key, 3);
        RateLimitResult r2 = engine.process(key, 3);

        assertTrue(r1.isAllowed());
        assertFalse(r2.isAllowed());
    }
}
