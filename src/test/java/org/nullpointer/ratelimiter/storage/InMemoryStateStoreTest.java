package org.nullpointer.ratelimiter.storage;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateStore;
import org.nullpointer.ratelimiter.storage.state.StateStore;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class InMemoryStateStoreTest {

    @Test
    void storesAndRetrievesRegularState() {
        StateStore store = new InMemoryStateStore();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();

        store.setState(key, new TokenBucketState(5, System.nanoTime()));

        assertNotNull(store.getState(key));
    }

    @Test
    void storesAndRetrievesHierarchicalState() {
        StateStore store = new InMemoryStateStore();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();

        store.setHierarchicalState(key, new TokenBucketState(5, System.nanoTime()));

        assertNotNull(store.getHierarchicalState(key));
    }
}
