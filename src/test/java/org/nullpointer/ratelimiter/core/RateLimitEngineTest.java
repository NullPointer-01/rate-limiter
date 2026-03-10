package org.nullpointer.ratelimiter.core;

import org.junit.jupiter.api.Test;
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

class RateLimitEngineTest {

    @Test
    void engineUsesConfigAndStateToEnforceLimits() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager manager = new ConfigurationManager(configStore, stateStore);
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
