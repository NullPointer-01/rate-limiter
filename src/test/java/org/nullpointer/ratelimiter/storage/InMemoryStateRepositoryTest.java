package org.nullpointer.ratelimiter.storage;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class InMemoryStateRepositoryTest {

    @Test
    void storesAndRetrievesRegularState() {
        StateRepository store = new InMemoryStateRepository();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();

        store.setState(key, new TokenBucketState(5, System.nanoTime()));

        assertNotNull(store.getState(key));
    }

    @Test
    void storesAndRetrievesHierarchicalState() {
        StateRepository store = new InMemoryStateRepository();
        RateLimitKey key = RateLimitKey.builder().setUserId("user").build();

        store.setHierarchicalState(key, new TokenBucketState(5, System.nanoTime()));

        assertNotNull(store.getHierarchicalState(key));
    }
}
