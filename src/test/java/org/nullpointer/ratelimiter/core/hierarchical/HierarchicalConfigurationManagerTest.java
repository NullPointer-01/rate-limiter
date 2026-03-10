package org.nullpointer.ratelimiter.core.hierarchical;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.model.state.TokenBucketState;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigRepository;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalConfigurationManagerTest {

    private ConfigRepository configStore;
    private StateRepository stateStore;
    private HierarchicalConfigurationManager manager;

    @BeforeEach
    void setUp() {
        configStore = new InMemoryConfigRepository();
        stateStore = new InMemoryStateRepository();
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
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));
        policy.addPolicy(RateLimitScope.USER, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));

        manager.setHierarchyPolicy(policy);

        HierarchicalRateLimitPolicy retrieved = manager.getHierarchyPolicy();
        assertNotNull(retrieved);
        assertEquals(2, retrieved.getLevels().size());
    }

    @Test
    void hierarchyPolicyIsPersistedInStore() {
        HierarchicalRateLimitPolicy policy = new HierarchicalRateLimitPolicy();
        policy.addPolicy(RateLimitScope.GLOBAL, new TokenBucketConfig(100, 10, 1, TimeUnit.SECONDS));

        manager.setHierarchyPolicy(policy);

        HierarchicalConfigurationManager anotherManager = new HierarchicalConfigurationManager(configStore, stateStore);
        HierarchicalRateLimitPolicy retrieved = anotherManager.getHierarchyPolicy();
        assertNotNull(retrieved);
        assertEquals(1, retrieved.getLevels().size());
    }

    @Test
    void getHierarchyPolicyThrowsWhenNotSet() {
        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getHierarchyPolicy());
    }

    @Test
    void getHierarchyPolicyThrowsWhenEmpty() {
        manager.setHierarchyPolicy(new HierarchicalRateLimitPolicy());
        assertThrows(RateLimitConfigNotFoundException.class, () -> manager.getHierarchyPolicy());
    }

    @Test
    void resolveScopedConfigReturnsDefaultPolicy() {
        manager.setDefaultScopedConfig(RateLimitScope.USER, new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS));

        RequestContext context = RequestContext.builder().userId("user1").build();
        assertNotNull(manager.resolveScopedConfig(RateLimitScope.USER, context));
    }

    @Test
    void scopedConfigIsPersistedInStore() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        manager.setDefaultScopedConfig(RateLimitScope.USER, defaultConfig);

        HierarchicalConfigurationManager anotherManager = new HierarchicalConfigurationManager(configStore, stateStore);
        RequestContext context = RequestContext.builder().userId("user1").build();
        assertSame(defaultConfig, anotherManager.resolveScopedConfig(RateLimitScope.USER, context));
    }

    @Test
    void resolveScopedConfigPrefersOverride() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        TokenBucketConfig overrideConfig = new TokenBucketConfig(50, 5, 1, TimeUnit.SECONDS);

        manager.setDefaultScopedConfig(RateLimitScope.USER, defaultConfig);
        manager.setOverrideScopedConfig(RateLimitScope.USER, "user1", overrideConfig);

        RequestContext context = RequestContext.builder().userId("user1").build();
        assertSame(overrideConfig, manager.resolveScopedConfig(RateLimitScope.USER, context));
    }

    @Test
    void resolveScopedConfigFallsBackToDefaultWhenNoOverride() {
        TokenBucketConfig defaultConfig = new TokenBucketConfig(10, 1, 1, TimeUnit.SECONDS);
        manager.setDefaultScopedConfig(RateLimitScope.USER, defaultConfig);

        RequestContext context = RequestContext.builder().userId("regular-user").build();
        assertSame(defaultConfig, manager.resolveScopedConfig(RateLimitScope.USER, context));
    }

    @Test
    void resolveScopedConfigReturnsNullWhenNoPolicyExists() {
        RequestContext context = RequestContext.builder().userId("user1").build();
        assertNull(manager.resolveScopedConfig(RateLimitScope.USER, context));
    }
}
