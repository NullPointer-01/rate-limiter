package org.nullpointer.ratelimiter.core;

import org.junit.jupiter.api.Test;
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

class RateLimitEngineTest {

    @Test
    void engineUsesConfigAndStateToEnforceLimits() {
        ConfigRepository configStore = new InMemoryConfigRepository();
        StateRepository stateStore = new InMemoryStateRepository();
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
