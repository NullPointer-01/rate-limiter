package org.nullpointer.ratelimiter.core.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;
import org.nullpointer.ratelimiter.storage.config.ConfigStore;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigStore;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateStore;
import org.nullpointer.ratelimiter.storage.state.StateStore;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalConfigurationManagerTest {

    private ConfigStore configStore;
    private StateStore stateStore;
    private HierarchicalConfigurationManager manager;

    @BeforeEach
    void setUp() {
        configStore = new InMemoryConfigStore();
        stateStore = new InMemoryStateStore();
        manager = new HierarchicalConfigurationManager(configStore, stateStore);
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
    void setAndGetHierarchyPolicy() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));

        manager.setHierarchyPolicy(policy);

        HierarchicalRateLimitConfig retrieved = manager.getHierarchyPolicy();
        assertNotNull(retrieved);
        assertEquals(2, retrieved.getLevels().size());
    }

    @Test
    void hierarchyPolicyIsPersistedInStore() {
        HierarchicalRateLimitConfig policy = new HierarchicalRateLimitConfig();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));

        manager.setHierarchyPolicy(policy);

        HierarchicalConfigurationManager anotherManager = new HierarchicalConfigurationManager(configStore, stateStore);
        HierarchicalRateLimitConfig retrieved = anotherManager.getHierarchyPolicy();
        assertNotNull(retrieved);
        assertEquals(1, retrieved.getLevels().size());
    }

    @Test
    void getHierarchyPolicyThrowsWhenNotSet() {
        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getHierarchyPolicy());
    }

    @Test
    void getHierarchyPolicyThrowsWhenEmpty() {
        manager.setHierarchyPolicy(new HierarchicalRateLimitConfig());
        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getHierarchyPolicy());
    }

    @Test
    void resolveConfigReturnsDefaultPolicy() {
        manager.addDefaultPolicy(RateLimitScope.USER, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));

        RequestContext context = RequestContext.builder().userId("user1").build();
        assertNotNull(manager.resolveConfig(RateLimitScope.USER, context));
    }

    @Test
    void scopedPolicyIsPersistedInStore() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        manager.addDefaultPolicy(RateLimitScope.USER, defaultConfig);

        HierarchicalConfigurationManager anotherManager = new HierarchicalConfigurationManager(configStore, stateStore);
        RequestContext context = RequestContext.builder().userId("user1").build();
        assertSame(defaultConfig, anotherManager.resolveConfig(RateLimitScope.USER, context));
    }

    @Test
    void resolveConfigPrefersOverride() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig overrideConfig = new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS);

        manager.addDefaultPolicy(RateLimitScope.USER, defaultConfig);
        manager.addOverridePolicy(RateLimitScope.USER, "user1", overrideConfig);

        RequestContext context = RequestContext.builder().userId("user1").build();
        assertSame(overrideConfig, manager.resolveConfig(RateLimitScope.USER, context));
    }

    @Test
    void resolveConfigFallsBackToDefaultWhenNoOverride() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        manager.addDefaultPolicy(RateLimitScope.USER, defaultConfig);

        RequestContext context = RequestContext.builder().userId("regular-user").build();
        assertSame(defaultConfig, manager.resolveConfig(RateLimitScope.USER, context));
    }

    @Test
    void resolveConfigReturnsNullWhenNoPolicyExists() {
        RequestContext context = RequestContext.builder().userId("user1").build();
        assertNull(manager.resolveConfig(RateLimitScope.USER, context));
    }
}
