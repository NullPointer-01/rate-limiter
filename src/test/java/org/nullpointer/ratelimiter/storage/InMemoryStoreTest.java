package org.nullpointer.ratelimiter.storage;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryStoreTest {

    @Test
    void storesAndRetrievesConfigAndState() {
        InMemoryStore store = new InMemoryStore();
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        store.setDefaultConfig(defaultConfig);

        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();
        TokenBucketConfig specificConfig = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        store.setConfig(key, specificConfig);

        store.setState(key, new TokenBucketState(5, System.nanoTime()));

        assertSame(defaultConfig, store.getDefaultConfig());
        assertSame(specificConfig, store.getConfig(key));
        assertNotNull(store.getState(key));
    }
}
