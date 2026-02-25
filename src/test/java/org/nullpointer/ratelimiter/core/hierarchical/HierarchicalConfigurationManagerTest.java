package org.nullpointer.ratelimiter.core.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;
import org.nullpointer.ratelimiter.storage.InMemoryStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalConfigurationManagerTest {

    private InMemoryStore store;
    private HierarchicalConfigurationManager manager;

    @BeforeEach
    void setUp() {
        store = new InMemoryStore();
        manager = new HierarchicalConfigurationManager(store);
    }

    @Test
    void setAndGetHierarchicalConfig() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();
        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();
        config.addLevel(
                new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS),
                RateLimitKey.builder().setUserId("user1").build()
        );

        manager.setHierarchicalConfig(key, config);

        HierarchicalRateLimitConfig retrieved = manager.getHierarchicalConfig(key);
        assertNotNull(retrieved);
        assertFalse(retrieved.isEmpty());
        assertEquals(1, retrieved.getLevels().size());
    }

    @Test
    void getHierarchicalConfigThrowsWhenNotFound() {
        RateLimitKey key = RateLimitKey.builder().setUserId("nonexistent").build();

        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getHierarchicalConfig(key));
    }

    @Test
    void getHierarchicalConfigThrowsWhenEmpty() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user-empty").build();
        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();

        manager.setHierarchicalConfig(key, config);

        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getHierarchicalConfig(key));
    }

    @Test
    void setAndGetHierarchicalState() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();
        RateLimitState state = new TokenBucketState(10, System.nanoTime());

        manager.setHierarchicalState(key, state);

        RateLimitState retrieved = manager.getHierarchicalState(key);
        assertNotNull(retrieved);
        assertSame(state, retrieved);
    }

    @Test
    void getHierarchicalStateReturnsNullWhenNotSet() {
        RateLimitKey key = RateLimitKey.builder().setUserId("no-state").build();

        assertNull(manager.getHierarchicalState(key));
    }

    @Test
    void hierarchicalConfigWithMultipleLevels() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();
        HierarchicalRateLimitConfig config = new HierarchicalRateLimitConfig();

        RateLimitKey globalKey = RateLimitKey.builder().setDomain("service").build();
        RateLimitKey userKey = RateLimitKey.builder().setUserId("user1").build();
        RateLimitKey endpointKey = RateLimitKey.builder().setUserId("user1").setApi("/api/data").build();

        config.addLevel(new TokenBucketConfig(1000, 100, 1, TimeUnit.SECONDS), globalKey);
        config.addLevel(new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS), userKey);
        config.addLevel(new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS), endpointKey);

        manager.setHierarchicalConfig(key, config);

        HierarchicalRateLimitConfig retrieved = manager.getHierarchicalConfig(key);
        assertEquals(3, retrieved.getLevels().size());
        assertEquals(1, retrieved.getLevels().get(0).getLevel());
        assertEquals(2, retrieved.getLevels().get(1).getLevel());
        assertEquals(3, retrieved.getLevels().get(2).getLevel());
    }

    @Test
    void overwritingHierarchicalConfigReplacesExisting() {
        RateLimitKey key = RateLimitKey.builder().setUserId("user1").build();

        HierarchicalRateLimitConfig config1 = new HierarchicalRateLimitConfig();
        config1.addLevel(
                new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS),
                RateLimitKey.builder().setUserId("user1").build()
        );
        manager.setHierarchicalConfig(key, config1);

        HierarchicalRateLimitConfig config2 = new HierarchicalRateLimitConfig();
        config2.addLevel(
                new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS),
                RateLimitKey.builder().setUserId("user1").build()
        );
        config2.addLevel(
                new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS),
                RateLimitKey.builder().setDomain("service").build()
        );
        manager.setHierarchicalConfig(key, config2);

        HierarchicalRateLimitConfig retrieved = manager.getHierarchicalConfig(key);
        assertEquals(2, retrieved.getLevels().size());
    }
}
