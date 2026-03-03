package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.storage.Store;

public class HierarchicalConfigurationManager extends ConfigurationManager {
    private static final String DEFAULT_IDENTIFIER = "DEFAULT";

    public HierarchicalConfigurationManager(Store store) {
        super(store);
    }

    public void setHierarchyPolicy(HierarchicalRateLimitConfig policy) {
        this.store.setHierarchyPolicy(policy);
    }

    public HierarchicalRateLimitConfig getHierarchyPolicy() {
        HierarchicalRateLimitConfig hierarchyPolicy = this.store.getHierarchyPolicy();
        if (hierarchyPolicy == null || hierarchyPolicy.isEmpty()) {
            throw new RateLimitConfigNotFoundException("No hierarchy policy configured");
        }
        return hierarchyPolicy;
    }

    public void addDefaultPolicy(RateLimitScope scope, RateLimitConfig config) {
        this.store.setScopedPolicy(scope, DEFAULT_IDENTIFIER, config);
    }

    public void addOverridePolicy(RateLimitScope scope, String identifier, RateLimitConfig config) {
        if (DEFAULT_IDENTIFIER.equals(identifier)) {
            throw new IllegalArgumentException("Identifier cannot be DEFAULT");
        }
        this.store.setScopedPolicy(scope, identifier, config);
    }

    public RateLimitConfig resolveConfig(RateLimitScope scope, RequestContext context) {
        // Check if any overrides are present
        String identifier = extractIdentifier(scope, context);
        if (identifier != null) {
            RateLimitConfig overrideConfig = this.store.getScopedPolicy(scope, identifier);
            if (overrideConfig != null) {
                return overrideConfig;
            }
        }

        // Fall back to default
        return this.store.getScopedPolicy(scope, DEFAULT_IDENTIFIER);
    }

    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        this.store.setHierarchicalState(key, state);
    }

    public RateLimitState getHierarchicalState(RateLimitKey key) {
        return this.store.getHierarchicalState(key);
    }

    // Should match with RateLimitKeyGenerator.extractIdentifier()
    private String extractIdentifier(RateLimitScope scope, RequestContext context) {
        switch (scope) {
            case GLOBAL:
                return "global";
            case REGION:
                return context.getRegion().orElse(null);
            case TENANT:
                return context.getTenantId().orElse(null);
            case SERVICE:
                return context.getServiceId().orElse(null);
            case USER:
                return context.getUserId().orElse(null);
            case IP:
                return context.getIpAddress().orElse(null);
            case ENDPOINT:
                return (context.getHttpMethod().orElse("ANY") + ":" + context.getApiPath().orElse("/"));
            default:
                return null;
        }
    }
}
